import { DurableObject } from 'cloudflare:workers'
import { getContainer } from '@cloudflare/containers'

/**
 * Scheduler DO — singleton that bin-packs volumes across SandboxContainer DOs.
 *
 * SQLite state:
 *   containers(name TEXT PK, volume_count INTEGER)
 *   assignments(volume TEXT PK, container_name TEXT REFERENCES containers)
 */
export class Scheduler extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS containers (
        name TEXT PRIMARY KEY,
        volume_count INTEGER NOT NULL DEFAULT 0
      )
    `)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS assignments (
        volume TEXT PRIMARY KEY,
        container_name TEXT NOT NULL REFERENCES containers(name)
      )
    `)
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Internal APIs
    if (path === '/_internal/assign' && request.method === 'POST') {
      return this.handleAssign(request)
    }
    if (path === '/_internal/release' && request.method === 'POST') {
      return this.handleRelease(request)
    }
    if (path === '/_internal/container-died' && request.method === 'POST') {
      return this.handleContainerDied(request)
    }

    // Public APIs — forward to any container
    if (path === '/api/volumes') {
      return this.forwardToAnyContainer(request)
    }
    if (path.startsWith('/api/volumes/') && request.method === 'DELETE') {
      const volume = decodeURIComponent(path.slice('/api/volumes/'.length))
      return this.forwardDeleteVolume(volume)
    }

    // Stop all containers (for forcing new image rollout after deploy)
    if (path === '/debug/stop-all' && request.method === 'POST') {
      return this.handleStopAll()
    }

    // Force-kill all containers (SIGKILL — for when stop hangs on frozen FS)
    if (path === '/debug/kill-all' && request.method === 'POST') {
      return this.handleKillAll()
    }

    // Debug routes — forward to any container
    if (path.startsWith('/debug/')) {
      return this.forwardToAnyContainer(request)
    }

    return new Response('not found', { status: 404 })
  }

  private async handleAssign(request: Request): Promise<Response> {
    const { volume } = (await request.json()) as { volume: string }

    // Check existing assignment.
    const existing = [
      ...this.sql.exec<{ container_name: string }>(
        'SELECT container_name FROM assignments WHERE volume = ?',
        volume,
      ),
    ]
    if (existing.length > 0) {
      return Response.json({ container: existing[0].container_name })
    }

    // Bin-pack: find container with lowest volume_count under limit.
    const MAX_VOLUMES_PER_CONTAINER = 10
    const candidates = [
      ...this.sql.exec<{ name: string; volume_count: number }>(
        'SELECT name, volume_count FROM containers WHERE volume_count < ? ORDER BY volume_count DESC LIMIT 1',
        MAX_VOLUMES_PER_CONTAINER,
      ),
    ]

    let containerName: string
    if (candidates.length > 0) {
      containerName = candidates[0].name
    } else {
      // Create new container.
      const count = [
        ...this.sql.exec<{ cnt: number }>('SELECT COUNT(*) AS cnt FROM containers'),
      ][0].cnt
      containerName = `container-${count}`
      this.sql.exec('INSERT INTO containers (name, volume_count) VALUES (?, 0)', containerName)
    }

    this.sql.exec('INSERT INTO assignments (volume, container_name) VALUES (?, ?)', volume, containerName)
    this.sql.exec('UPDATE containers SET volume_count = volume_count + 1 WHERE name = ?', containerName)

    return Response.json({ container: containerName })
  }

  private async handleRelease(request: Request): Promise<Response> {
    const { volume } = (await request.json()) as { volume: string }

    const rows = [
      ...this.sql.exec<{ container_name: string }>(
        'SELECT container_name FROM assignments WHERE volume = ?',
        volume,
      ),
    ]
    if (rows.length > 0) {
      this.sql.exec('DELETE FROM assignments WHERE volume = ?', volume)
      this.sql.exec(
        'UPDATE containers SET volume_count = MAX(volume_count - 1, 0) WHERE name = ?',
        rows[0].container_name,
      )
    }
    return Response.json({ ok: true })
  }

  private async handleContainerDied(request: Request): Promise<Response> {
    const { container } = (await request.json()) as { container: string }

    // Find all volumes assigned to this container.
    const affected = [
      ...this.sql.exec<{ volume: string }>(
        'SELECT volume FROM assignments WHERE container_name = ?',
        container,
      ),
    ]

    // Remove assignments and reset count.
    this.sql.exec('DELETE FROM assignments WHERE container_name = ?', container)
    this.sql.exec('UPDATE containers SET volume_count = 0 WHERE name = ?', container)

    const volumes = affected.map((r) => r.volume)

    // Notify each VolumeActor that its container is gone.
    for (const vol of volumes) {
      try {
        const actor = this.env.VOLUMES.get(this.env.VOLUMES.idFromName(vol))
        await actor.fetch(
          new Request('http://volume/_internal/container-stopped', { method: 'POST' }),
        )
      } catch (e) {
        console.error(`[scheduler] failed to notify VolumeActor for ${vol}`, e)
      }
    }

    return Response.json({ volumes })
  }

  private async forwardToAnyContainer(request: Request): Promise<Response> {
    const containerName = this.pickAnyContainer()
    const container = getContainer(this.env.SANDBOX, containerName)

    // Ensure container is running.
    const state = await container.getState()
    if (state.status !== 'running') {
      await container.startAndWaitForPorts()
    }

    // Buffer the body so it can be forwarded to the container.
    const bodyBytes = request.method !== 'GET' ? await request.arrayBuffer() : null

    // Rewrite URL to container-internal format.
    const url = new URL(request.url)
    let daemonPath = url.pathname
    // /api/volumes → /volumes, /debug/foo → /foo
    if (daemonPath.startsWith('/api/')) {
      daemonPath = daemonPath.slice(4) // /api/volumes → /volumes
    } else if (daemonPath.startsWith('/debug/')) {
      daemonPath = daemonPath.slice(6) // /debug/status → status
    }

    return container.fetch(
      new Request(`http://container/${daemonPath}${url.search}`, {
        method: request.method,
        headers: request.headers,
        body: bodyBytes,
      }),
    )
  }

  private forwardDeleteVolume(volume: string): Promise<Response> {
    // Forward as POST /delete to daemon (which expects POST with JSON body).
    const containerName = this.pickAnyContainer()
    const container = getContainer(this.env.SANDBOX, containerName)
    return container.fetch(
      new Request('http://container/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume }),
      }),
    )
  }

  private async handleStopAll(): Promise<Response> {
    const containers = [
      ...this.sql.exec<{ name: string }>('SELECT name FROM containers'),
    ]
    const stopped: string[] = []
    const errors: string[] = []
    for (const row of containers) {
      try {
        const container = getContainer(this.env.SANDBOX, row.name)
        await container.stop()
        stopped.push(row.name)
      } catch (e) {
        errors.push(`${row.name}: ${e}`)
      }
    }
    // Clear all assignments and counts since containers are gone.
    this.sql.exec('DELETE FROM assignments')
    this.sql.exec('UPDATE containers SET volume_count = 0')
    return Response.json({ stopped, errors })
  }

  private async handleKillAll(): Promise<Response> {
    const containers = [
      ...this.sql.exec<{ name: string }>('SELECT name FROM containers'),
    ]
    const killed: string[] = []
    const errors: string[] = []
    for (const row of containers) {
      try {
        const container = getContainer(this.env.SANDBOX, row.name)
        await container.destroy()
        killed.push(row.name)
      } catch (e) {
        errors.push(`${row.name}: ${e}`)
      }
    }
    this.sql.exec('DELETE FROM assignments')
    this.sql.exec('UPDATE containers SET volume_count = 0')
    return Response.json({ killed, errors })
  }

  private pickAnyContainer(): string {
    // Prefer a container that has volumes (likely already running).
    const rows = [
      ...this.sql.exec<{ name: string }>(
        'SELECT name FROM containers WHERE volume_count > 0 ORDER BY volume_count DESC LIMIT 1',
      ),
    ]
    if (rows.length > 0) return rows[0].name

    // Fall back to any existing container.
    const any = [
      ...this.sql.exec<{ name: string }>('SELECT name FROM containers LIMIT 1'),
    ]
    if (any.length > 0) return any[0].name

    // No containers at all — create one.
    const name = 'container-0'
    this.sql.exec('INSERT INTO containers (name, volume_count) VALUES (?, 0)', name)
    return name
  }
}
