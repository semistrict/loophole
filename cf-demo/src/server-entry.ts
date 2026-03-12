import { getContainer } from '@cloudflare/containers'
import tanstackHandler from '@tanstack/react-start/server-entry'

export { SandboxContainer } from './container'
export { Scheduler } from './scheduler'
export { VolumeActor } from './volume-actor'

const VOLUME_PREFIX = /^\/v\/([^/]+)\/(.*)/  // /v/{volume}/{rest}
const API_PREFIX = /^\/api\//                 // /api/*
const DEBUG_PREFIX = /^\/debug\//             // /debug/*
const CONTROL_PREFIX = /^\/debug\/control\/([^/]+)(?:\/(.*))?$/ // /debug/control/{id}/{rest}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function proxyControlWebSocket(container: any, containerUrl: string, headers: Headers): Promise<Response> {
  const upstreamResp = await container.fetch(
    new Request(containerUrl, {
      headers,
    }),
  )
  const upstream = upstreamResp.webSocket
  if (!upstream) {
    return new Response('container did not return websocket', { status: 502 })
  }
  upstream.accept()

  const pair = new WebSocketPair()
  const [client, server] = Object.values(pair)
  server.accept()

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

export default {
  async fetch(request: Request, ...args: unknown[]) {
    const url = new URL(request.url)
    const env = args[0] as Env

    // Per-volume routes → VolumeActor DO
    const volMatch = url.pathname.match(VOLUME_PREFIX)
    if (volMatch) {
      const volume = decodeURIComponent(volMatch[1])
      const rest = volMatch[2]
      const actor = env.VOLUMES.get(env.VOLUMES.idFromName(volume))
      const headers = new Headers(request.headers)
      headers.set('X-Volume', volume)
      return actor.fetch(
        new Request(`http://volume/${rest}${url.search}`, {
          method: request.method,
          headers,
          body: request.method !== 'GET' ? request.body : undefined,
        }),
      )
    }

    const controlMatch = url.pathname.match(CONTROL_PREFIX)
    if (controlMatch) {
      const containerName = decodeURIComponent(controlMatch[1])
      const rest = controlMatch[2] || 'status'
      const container = getContainer(env.SANDBOX, containerName)
      const state = await container.getState()
      if (state.status !== 'running') {
        await container.startAndWaitForPorts()
      }

      const headers = new Headers(request.headers)
      headers.set('X-Control-Secret', env.CONTROL_SECRET)
      const isWebSocket = request.headers.get('Upgrade')?.toLowerCase() === 'websocket'
      if (isWebSocket) {
        headers.set('Connection', 'Upgrade')
        headers.set('Upgrade', 'websocket')
        return proxyControlWebSocket(
          container,
          `http://container/control/${rest}${url.search}`,
          headers,
        )
      }

      const bodyBytes =
        request.method !== 'GET' && request.method !== 'HEAD'
          ? await request.arrayBuffer()
          : null

      return container.fetch(
        new Request(`http://container/control/${rest}${url.search}`, {
          method: request.method,
          headers,
          body: bodyBytes,
        }),
      )
    }

    // Global API + debug routes → Scheduler DO
    if (API_PREFIX.test(url.pathname) || DEBUG_PREFIX.test(url.pathname)) {
      const scheduler = env.SCHEDULER.get(env.SCHEDULER.idFromName('scheduler'))
      return scheduler.fetch(request)
    }

    // React app (TanStack Start)
    return (tanstackHandler as any).fetch(request, ...args)
  },
}
