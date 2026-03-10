import { DurableObject } from 'cloudflare:workers'
import { getContainer } from '@cloudflare/containers'

/**
 * VolumeActor DO — one per volume. Provides a stable endpoint that survives
 * container restarts. Forwards requests to the assigned SandboxContainer,
 * auto-assigning via Scheduler and remounting on container death.
 */
export class VolumeActor extends DurableObject<Env> {
  private containerName: string | null = null

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Internal: Scheduler notifies us that our container died.
    if (path === '/_internal/container-stopped' && request.method === 'POST') {
      this.containerName = null
      return new Response('ok')
    }

    // Extract the volume name from the DO name. The server-entry passes it
    // as a header since DO names aren't available on the instance.
    const volume = request.headers.get('X-Volume') ?? ''
    if (!volume) {
      return new Response('missing X-Volume header', { status: 400 })
    }

    // Ensure we have a container with the volume mounted.
    try {
      await this.ensureContainer(volume)
    } catch (e) {
      console.error(`[volume-actor] ensureContainer failed for ${volume}`, e)
      return new Response(`container setup failed: ${e}`, { status: 502 })
    }

    // Inject volume query param so daemon handlers (e.g. /sandbox/shell) know which volume.
    const fwdUrl = new URL(`http://container${path}${url.search}`)
    if (!fwdUrl.searchParams.has('volume')) {
      fwdUrl.searchParams.set('volume', volume)
    }

    const container = getContainer(this.env.SANDBOX, this.containerName!)
    const isWebSocket = request.headers.get('Upgrade')?.toLowerCase() === 'websocket'

    if (isWebSocket) {
      return this.proxyWebSocket(container, fwdUrl.toString())
    }

    // Buffer the body so we can replay it on retry.
    const bodyBytes = request.method !== 'GET' ? await request.arrayBuffer() : null

    const makeReq = () =>
      new Request(fwdUrl.toString(), {
        method: request.method,
        headers: request.headers,
        body: bodyBytes,
      })

    // Forward request to the container.
    try {
      return await container.fetch(makeReq())
    } catch (e) {
      // Container may have died — clear assignment and retry once.
      console.warn(`[volume-actor] fetch failed for ${volume}, retrying`, e)
      this.containerName = null
      try {
        await this.ensureContainer(volume)
        const retryContainer = getContainer(this.env.SANDBOX, this.containerName!)
        return await retryContainer.fetch(makeReq())
      } catch (retryErr) {
        return new Response(`container unavailable: ${retryErr}`, { status: 502 })
      }
    }
  }

  /**
   * Proxy a WebSocket connection to the container by creating a WebSocketPair
   * and bridging messages between the client and the container daemon.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async proxyWebSocket(container: any, containerUrl: string): Promise<Response> {
    // Connect to the container's WebSocket endpoint.
    const containerResp = await container.fetch(
      new Request(containerUrl, {
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
        },
      }),
    )
    const upstream = containerResp.webSocket
    if (!upstream) {
      return new Response('container did not return websocket', { status: 502 })
    }
    upstream.accept()

    // Create a pair for the client.
    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    server.accept()

    // Bridge: server (client-facing) ↔ upstream (container-facing).
    server.addEventListener('message', (ev) => {
      try {
        upstream.send(ev.data)
      } catch {
        server.close()
      }
    })
    upstream.addEventListener('message', (ev: MessageEvent) => {
      try {
        server.send(ev.data)
      } catch {
        upstream.close()
      }
    })
    server.addEventListener('close', (ev: CloseEvent) => {
      upstream.close(ev.code, ev.reason)
    })
    upstream.addEventListener('close', (ev: CloseEvent) => {
      server.close(ev.code, ev.reason)
    })
    server.addEventListener('error', () => upstream.close())
    upstream.addEventListener('error', () => server.close())

    return new Response(null, { status: 101, webSocket: client })
  }

  private async ensureContainer(volume: string): Promise<void> {
    if (this.containerName) return

    // Ask Scheduler for a container assignment.
    const scheduler = this.env.SCHEDULER.get(this.env.SCHEDULER.idFromName('scheduler'))
    const assignRes = await scheduler.fetch(
      new Request('http://scheduler/_internal/assign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume }),
      }),
    )
    const { container: name } = (await assignRes.json()) as { container: string }

    // Ensure the container is running.
    const container = getContainer(this.env.SANDBOX, name)
    const state = await container.getState()
    if (state.status !== 'running') {
      await container.startAndWaitForPorts()
    }

    // Mount the volume in the container.
    const mountRes = await container.fetch(
      new Request('http://container/mount', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume, mountpoint: volume }),
      }),
    )
    if (!mountRes.ok) {
      const body = (await mountRes.json().catch(() => ({}))) as { error?: string }
      // "already mounted" is fine — volume was already there.
      if (!body.error?.includes('already')) {
        throw new Error(body.error ?? `mount failed (${mountRes.status})`)
      }
    }

    this.containerName = name
  }
}
