import { DurableObject } from 'cloudflare:workers'
import { getContainer } from '@cloudflare/containers'

const MAX_SANDBOXES_PER_CONTAINER = 10
const CONTAINER_DIED_PATH = '/_internal/container-died'
const RAW_SANDBOX_PREFIX = '/sandbox'
const RAW_SANDBOXES_PREFIX = '/sandboxes'
const SANDBOX_PREFIX = '/api/sandbox'
const SANDBOXES_PREFIX = '/api/sandboxes'
const TOOLBOX_PREFIX = '/toolbox/'
const CONTROL_PREFIX = '/debug/control/'
const DEBUG_CONTAINER_PATH = '/debug/container'

type SandboxRecord = {
  id: string
  name: string
  state?: string
}

type ContainerRow = {
  name: string
  do_id: string
  sandbox_count: number
}

type ContainerDebugResponse = {
  name: string
  doId: string
  sandboxCount: number
  status: string
}

export class Scheduler extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS containers (
        name TEXT PRIMARY KEY,
        do_id TEXT NOT NULL UNIQUE,
        sandbox_count INTEGER NOT NULL DEFAULT 0
      )
    `)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS sandboxes (
        sandbox_id TEXT PRIMARY KEY,
        sandbox_name TEXT NOT NULL,
        container_name TEXT NOT NULL REFERENCES containers(name)
      )
    `)
    this.ensureSchema()
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    if (path === CONTAINER_DIED_PATH && request.method === 'POST') {
      return this.handleContainerDied(request)
    }
    if ((path === RAW_SANDBOX_PREFIX || path === RAW_SANDBOXES_PREFIX || path === SANDBOX_PREFIX || path === SANDBOXES_PREFIX) && request.method === 'GET') {
      return this.handleListSandboxes()
    }
    if ((path === RAW_SANDBOX_PREFIX || path === RAW_SANDBOXES_PREFIX || path === SANDBOX_PREFIX || path === SANDBOXES_PREFIX) && request.method === 'POST') {
      return this.handleCreateSandbox(request)
    }
    if (
      path.startsWith(`${RAW_SANDBOX_PREFIX}/`) ||
      path.startsWith(`${RAW_SANDBOXES_PREFIX}/`) ||
      path.startsWith(`${SANDBOX_PREFIX}/`) ||
      path.startsWith(`${SANDBOXES_PREFIX}/`)
    ) {
      return this.handleSandboxRequest(request)
    }
    if (path.startsWith(TOOLBOX_PREFIX)) {
      return this.handleToolboxRequest(request)
    }
    if (path === '/debug/stop-all' && request.method === 'POST') {
      return this.handleStopAll()
    }
    if (path === '/debug/kill-all' && request.method === 'POST') {
      return this.handleKillAll()
    }
    if (path === DEBUG_CONTAINER_PATH && request.method === 'GET') {
      return this.handleDebugContainer()
    }
    if (path.startsWith(CONTROL_PREFIX)) {
      return this.handleControlRequest(request)
    }
    if (path.startsWith('/api/') || path.startsWith('/v/') || path.startsWith('/debug/')) {
      return this.forwardToAnyContainer(request)
    }
    return new Response('not found', { status: 404 })
  }

  private async handleContainerDied(request: Request): Promise<Response> {
    const { containerId } = (await request.json()) as { containerId: string }
    const rows = [
      ...this.sql.exec<{ name: string }>(
        'SELECT name FROM containers WHERE do_id = ?',
        containerId,
      ),
    ]
    if (rows.length === 0) {
      return Response.json({ ok: true, removed: false })
    }
    const containerName = rows[0].name
    try {
      const state = await this.containerForName(containerName).getState()
      if (state.status === 'running') {
        return Response.json({ ok: true, removed: false, stale: true, container: containerName })
      }
    } catch (error) {
      console.warn('[scheduler] container state check failed during stop handling', { container: containerName, error })
    }
    this.sql.exec('DELETE FROM sandboxes WHERE container_name = ?', containerName)
    this.sql.exec('DELETE FROM containers WHERE name = ?', containerName)
    return Response.json({ ok: true, removed: true, container: containerName })
  }

  private async handleListSandboxes(): Promise<Response> {
    const containers = this.listContainers()
    const sandboxes: SandboxRecord[] = []

    for (const row of containers) {
      try {
        const response = await this.forwardRequestToContainer(
          row.name,
          new Request('http://scheduler/api/sandbox', { method: 'GET' }),
          'http://container/api/sandbox',
        )
        if (!response.ok) {
          continue
        }
        const listed = (await response.json()) as SandboxRecord[]
        sandboxes.push(...listed)
        this.reconcileContainerSandboxes(row.name, listed)
      } catch (error) {
        console.warn('[scheduler] list sandboxes failed', { container: row.name, error })
      }
    }

    return Response.json(sandboxes)
  }

  private async handleCreateSandbox(request: Request): Promise<Response> {
    const containerName = this.pickContainerForNewSandbox()
    const body = request.method !== 'GET' && request.method !== 'HEAD'
      ? await request.arrayBuffer()
      : null
    const response = await this.forwardRequestToContainer(
      containerName,
      new Request('http://scheduler/api/sandbox', {
        method: request.method,
        headers: request.headers,
        body,
      }),
      'http://container/api/sandbox',
    )
    if (!response.ok) {
      return response
    }

    const created = await response.clone().json() as SandboxRecord
    this.ensureContainerRow(containerName)
    this.sql.exec(
      'INSERT OR REPLACE INTO sandboxes (sandbox_id, sandbox_name, container_name) VALUES (?, ?, ?)',
      created.id,
      created.name ?? created.id,
      containerName,
    )
    this.sql.exec(
      'UPDATE containers SET sandbox_count = (SELECT COUNT(*) FROM sandboxes WHERE container_name = ?) WHERE name = ?',
      containerName,
      containerName,
    )
    return response
  }

  private async handleSandboxRequest(request: Request): Promise<Response> {
    const { sandboxId, suffix } = this.parseSandboxPath(request.url)
    if (!sandboxId) {
      return new Response('sandbox id required', { status: 404 })
    }
    const containerName = await this.findSandboxContainer(sandboxId)
    if (!containerName) {
      return new Response('sandbox not found', { status: 404 })
    }

    const target = `http://container/api/sandbox/${encodeURIComponent(sandboxId)}${suffix}`
    const response = await this.forwardRequestToContainer(containerName, request, target)
    if (request.method === 'DELETE' && response.ok) {
      this.sql.exec('DELETE FROM sandboxes WHERE sandbox_id = ?', sandboxId)
      this.sql.exec(
        'UPDATE containers SET sandbox_count = (SELECT COUNT(*) FROM sandboxes WHERE container_name = ?) WHERE name = ?',
        containerName,
        containerName,
      )
    }
    return response
  }

  private async handleToolboxRequest(request: Request): Promise<Response> {
    const sandboxId = this.parseToolboxSandboxID(request.url)
    if (!sandboxId) {
      return new Response('sandbox id required', { status: 404 })
    }
    const containerName = await this.findSandboxContainer(sandboxId)
    if (!containerName) {
      return new Response('sandbox not found', { status: 404 })
    }
    return this.forwardRequestToContainer(containerName, request, `http://container${new URL(request.url).pathname}${new URL(request.url).search}`)
  }

  private async handleControlRequest(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const parts = url.pathname.slice(CONTROL_PREFIX.length).split('/')
    const containerName = decodeURIComponent(parts[0] ?? '')
    if (!containerName) {
      return new Response('container name required', { status: 400 })
    }
    const rest = parts.slice(1).join('/') || 'status'
    return this.forwardRequestToContainer(
      containerName,
      request,
      `http://container/control/${rest}${url.search}`,
    )
  }

  private async handleDebugContainer(): Promise<Response> {
    const row = this.pickAnyContainerRow()
    const container = this.containerForName(row.name)
    const state = await container.getState()
    const payload: ContainerDebugResponse = {
      name: row.name,
      doId: row.do_id,
      sandboxCount: row.sandbox_count,
      status: state.status,
    }
    return Response.json(payload)
  }

  private async forwardToAnyContainer(request: Request): Promise<Response> {
    const containerName = this.pickAnyContainer()
    const url = new URL(request.url)
    let targetPath = url.pathname
    if (targetPath.startsWith('/debug/')) {
      targetPath = `/${targetPath.slice('/debug/'.length)}`
    }
    return this.forwardRequestToContainer(
      containerName,
      request,
      `http://container${targetPath}${url.search}`,
    )
  }

  private async handleStopAll(): Promise<Response> {
    const containers = this.listContainers()
    const stopped: string[] = []
    const errors: string[] = []

    for (const row of containers) {
      try {
        await this.containerForName(row.name).stop()
        stopped.push(row.name)
      } catch (error) {
        errors.push(`${row.name}: ${String(error)}`)
      }
    }

    this.sql.exec('DELETE FROM sandboxes')
    this.sql.exec('DELETE FROM containers')
    return Response.json({ stopped, errors })
  }

  private async handleKillAll(): Promise<Response> {
    const containers = this.listContainers()
    const killed: string[] = []
    const errors: string[] = []

    for (const row of containers) {
      try {
        await this.containerForName(row.name).destroy()
        killed.push(row.name)
      } catch (error) {
        errors.push(`${row.name}: ${String(error)}`)
      }
    }

    this.sql.exec('DELETE FROM sandboxes')
    this.sql.exec('DELETE FROM containers')
    return Response.json({ killed, errors })
  }

  private async forwardRequestToContainer(
    containerName: string,
    request: Request,
    targetURL: string,
  ): Promise<Response> {
    const container = this.containerForName(containerName)
    await this.ensureContainerRunning(container)

    const isWebSocket = request.headers.get('Upgrade')?.toLowerCase() === 'websocket'
    if (isWebSocket) {
      return this.proxyWebSocket(container, targetURL, request)
    }

    const body = request.method !== 'GET' && request.method !== 'HEAD'
      ? await request.arrayBuffer()
      : null

    return container.fetch(new Request(targetURL, {
      method: request.method,
      headers: request.headers,
      body,
    }))
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async proxyWebSocket(container: any, containerURL: string, request: Request): Promise<Response> {
    const headers = new Headers(request.headers)
    headers.set('Upgrade', 'websocket')
    headers.set('Connection', 'Upgrade')
    const upstreamResponse = await container.fetch(new Request(containerURL, {
      headers,
    }))
    const upstream = upstreamResponse.webSocket
    if (!upstream) {
      return new Response('container did not return websocket', { status: 502 })
    }
    upstream.accept()

    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    server.accept()

    server.addEventListener('message', (event) => {
      try {
        upstream.send(event.data)
      } catch {
        server.close()
      }
    })
    upstream.addEventListener('message', (event: MessageEvent) => {
      try {
        server.send(event.data)
      } catch {
        upstream.close()
      }
    })
    server.addEventListener('close', (event: CloseEvent) => upstream.close(event.code, event.reason))
    upstream.addEventListener('close', (event: CloseEvent) => server.close(event.code, event.reason))
    server.addEventListener('error', () => upstream.close())
    upstream.addEventListener('error', () => server.close())

    return new Response(null, { status: 101, webSocket: client })
  }

  private containerForName(name: string) {
    // The helper's generic constraint is too narrow for our generated Env type.
    // Runtime behavior is correct: the namespace is still the SandboxContainer binding.
    return getContainer(this.env.SANDBOX as any, name)
  }

  private async ensureContainerRunning(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    container: any,
  ): Promise<void> {
    const state = await container.getState()
    if (state.status !== 'running') {
      await container.startAndWaitForPorts()
    }
  }

  private pickContainerForNewSandbox(): string {
    const rows = [
      ...this.sql.exec<ContainerRow>(
        'SELECT name, do_id, sandbox_count FROM containers WHERE sandbox_count < ? ORDER BY sandbox_count ASC, name ASC LIMIT 1',
        MAX_SANDBOXES_PER_CONTAINER,
      ),
    ]
    if (rows.length > 0) {
      return rows[0].name
    }
    return this.createContainerRow().name
  }

  private pickAnyContainer(): string {
    return this.pickAnyContainerRow().name
  }

  private pickAnyContainerRow(): ContainerRow {
    const rows = [
      ...this.sql.exec<ContainerRow>(
        'SELECT name, do_id, sandbox_count FROM containers ORDER BY sandbox_count DESC, name ASC LIMIT 1',
      ),
    ]
    if (rows.length > 0) {
      return rows[0]
    }
    return this.createContainerRow()
  }

  private createContainerRow(): ContainerRow {
    const name = `sandbox-${crypto.randomUUID().slice(0, 8)}`
    this.ensureContainerRow(name)
    return {
      name,
      do_id: this.env.SANDBOX.idFromName(name).toString(),
      sandbox_count: 0,
    }
  }

  private ensureSchema(): void {
    this.ensureColumn(
      'containers',
      'do_id',
      'ALTER TABLE containers ADD COLUMN do_id TEXT',
    )
    this.ensureColumn(
      'containers',
      'sandbox_count',
      'ALTER TABLE containers ADD COLUMN sandbox_count INTEGER NOT NULL DEFAULT 0',
    )
    const missingIDs = [
      ...this.sql.exec<{ name: string }>(
        'SELECT name FROM containers WHERE do_id IS NULL OR do_id = ?',
        '',
      ),
    ]
    for (const row of missingIDs) {
      this.sql.exec(
        'UPDATE containers SET do_id = ? WHERE name = ?',
        this.env.SANDBOX.idFromName(row.name).toString(),
        row.name,
      )
    }
  }

  private ensureColumn(tableName: string, columnName: string, alterSQL: string): void {
    const rows = [
      ...this.sql.exec<{ name: string }>(`PRAGMA table_info(${tableName})`),
    ]
    const hasColumn = rows.some((row) => row.name === columnName)
    if (!hasColumn) {
      this.sql.exec(alterSQL)
    }
  }

  private ensureContainerRow(name: string): void {
    const doID = this.env.SANDBOX.idFromName(name).toString()
    this.sql.exec(
      'INSERT OR IGNORE INTO containers (name, do_id, sandbox_count) VALUES (?, ?, 0)',
      name,
      doID,
    )
  }

  private listContainers(): ContainerRow[] {
    return [
      ...this.sql.exec<ContainerRow>(
        'SELECT name, do_id, sandbox_count FROM containers ORDER BY name ASC',
      ),
    ]
  }

  private reconcileContainerSandboxes(containerName: string, sandboxes: SandboxRecord[]): void {
    this.ensureContainerRow(containerName)
    this.sql.exec('DELETE FROM sandboxes WHERE container_name = ?', containerName)
    for (const sandbox of sandboxes) {
      this.sql.exec(
        'INSERT OR REPLACE INTO sandboxes (sandbox_id, sandbox_name, container_name) VALUES (?, ?, ?)',
        sandbox.id,
        sandbox.name ?? sandbox.id,
        containerName,
      )
    }
    this.sql.exec(
      'UPDATE containers SET sandbox_count = ? WHERE name = ?',
      sandboxes.length,
      containerName,
    )
  }

  private async findSandboxContainer(sandboxId: string): Promise<string | null> {
    const existing = [
      ...this.sql.exec<{ container_name: string }>(
        'SELECT container_name FROM sandboxes WHERE sandbox_id = ?',
        sandboxId,
      ),
    ]
    if (existing.length > 0) {
      return existing[0].container_name
    }

    for (const row of this.listContainers()) {
      try {
        const response = await this.forwardRequestToContainer(
          row.name,
          new Request(`http://scheduler/api/sandbox/${encodeURIComponent(sandboxId)}`, {
            method: 'GET',
          }),
          `http://container/api/sandbox/${encodeURIComponent(sandboxId)}`,
        )
        if (!response.ok) {
          continue
        }
        const sandbox = (await response.json()) as SandboxRecord
        this.ensureContainerRow(row.name)
        this.sql.exec(
          'INSERT OR REPLACE INTO sandboxes (sandbox_id, sandbox_name, container_name) VALUES (?, ?, ?)',
          sandbox.id,
          sandbox.name ?? sandbox.id,
          row.name,
        )
        this.sql.exec(
          'UPDATE containers SET sandbox_count = (SELECT COUNT(*) FROM sandboxes WHERE container_name = ?) WHERE name = ?',
          row.name,
          row.name,
        )
        return row.name
      } catch (error) {
        console.warn('[scheduler] sandbox lookup failed', { container: row.name, sandboxId, error })
      }
    }

    return null
  }

  private parseSandboxPath(rawURL: string): { sandboxId: string | null; suffix: string } {
    const url = new URL(rawURL)
    const base = url.pathname.startsWith(SANDBOX_PREFIX)
      ? SANDBOX_PREFIX
      : url.pathname.startsWith(SANDBOXES_PREFIX)
        ? SANDBOXES_PREFIX
        : url.pathname.startsWith(RAW_SANDBOX_PREFIX)
          ? RAW_SANDBOX_PREFIX
          : RAW_SANDBOXES_PREFIX
    const rest = url.pathname.slice(base.length + 1)
    const parts = rest.split('/')
    const sandboxId = parts[0] ? decodeURIComponent(parts[0]) : null
    const suffix = parts.length > 1 ? `/${parts.slice(1).join('/')}${url.search}` : url.search
    return { sandboxId, suffix }
  }

  private parseToolboxSandboxID(rawURL: string): string | null {
    const url = new URL(rawURL)
    const rest = url.pathname.slice(TOOLBOX_PREFIX.length)
    const sandboxId = rest.split('/')[0]
    return sandboxId ? decodeURIComponent(sandboxId) : null
  }
}
