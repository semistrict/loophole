import { authedFetch } from './client-lib'

type SandboxRecord = {
  id: string
  name: string
  state: string
}

type SessionRecord = {
  sessionId: string
}

type CommandResponse = {
  stdout?: string
  stderr?: string
  exit_code?: number
}

const volumeBase = process.env.CF_DEMO_SMOKE_VOLUME?.trim() || 'smoke-local'
const sandboxName = `${volumeBase}-${Date.now()}`

function logStep(message: string) {
  console.log(`[smoke] ${message}`)
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await authedFetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`${method} ${path} -> ${response.status} ${text}`)
  }
  if (text === '') {
    return undefined as T
  }
  return JSON.parse(text) as T
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

async function main() {
  logStep(`creating sandbox ${sandboxName}`)
  const created = await request<SandboxRecord>('POST', '/api/sandbox', { name: sandboxName })
  assert(created.id, 'sandbox create response missing id')
  assert(created.state === 'running', `expected running sandbox, got ${created.state}`)

  try {
    logStep('listing sandboxes')
    const sandboxes = await request<SandboxRecord[]>('GET', '/api/sandbox')
    assert(sandboxes.some((sandbox) => sandbox.id === created.id), 'created sandbox missing from list')

    logStep('fetching sandbox by id')
    const fetched = await request<SandboxRecord>('GET', `/api/sandbox/${encodeURIComponent(created.id)}`)
    assert(fetched.name === sandboxName, `expected sandbox name ${sandboxName}, got ${fetched.name}`)

    logStep('listing seeded guest tools')
    const binEntries = await request<Array<{ name: string }>>(
      'GET',
      `/toolbox/${encodeURIComponent(created.id)}/toolbox/files?path=${encodeURIComponent('/bin')}`,
    )
    const entryNames = new Set(binEntries.map((entry) => entry.name))
    assert(entryNames.has('sh'), 'expected /bin/sh in guest rootfs')

    logStep('running direct argv command')
    const direct = await request<CommandResponse>(
      'POST',
      `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/execute`,
      { argv: ['/bin/echo', 'hello'], cwd: '/' },
    )
    assert(direct.exit_code === 0, `expected direct exec exit 0, got ${direct.exit_code}`)
    assert(direct.stdout === 'hello\n', `unexpected direct stdout: ${JSON.stringify(direct.stdout)}`)

    logStep('running shell command')
    const shell = await request<CommandResponse>(
      'POST',
      `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/execute`,
      { command: 'echo shell-ok && uname -m && pwd', cwd: '/tmp' },
    )
    assert(shell.exit_code === 0, `expected shell exec exit 0, got ${shell.exit_code}`)
    assert(shell.stdout?.includes('shell-ok\n'), `missing shell-ok in stdout: ${JSON.stringify(shell.stdout)}`)
    assert(shell.stdout?.includes('/tmp\n'), `missing /tmp in stdout: ${JSON.stringify(shell.stdout)}`)

    logStep('creating process session')
    const session = await request<SessionRecord>(
      'POST',
      `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/session`,
      { cwd: '/', env: { SMOKE_VALUE: 'session-ok' } },
    )
    assert(session.sessionId, 'session create response missing sessionId')

    try {
      logStep('executing command in session')
      const sessionCommand = await request<CommandResponse>(
        'POST',
        `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/session/${encodeURIComponent(session.sessionId)}/exec`,
        { command: 'echo $SMOKE_VALUE && uname -m', runAsync: false },
      )
      assert(sessionCommand.exit_code === 0, `expected session exec exit 0, got ${sessionCommand.exit_code}`)
      assert(
        sessionCommand.stdout?.includes('session-ok\n'),
        `missing session-ok in stdout: ${JSON.stringify(sessionCommand.stdout)}`,
      )

      logStep('fetching session state')
      const sessionState = await request<{
        commands: Array<{ status: string; stdout?: string }>
      }>(
        'GET',
        `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/session/${encodeURIComponent(session.sessionId)}`,
      )
      assert(sessionState.commands.length === 1, `expected 1 session command, got ${sessionState.commands.length}`)
      assert(sessionState.commands[0]?.status === 'done', `expected done command, got ${sessionState.commands[0]?.status}`)
    } finally {
      logStep('deleting process session')
      await request('DELETE', `/toolbox/${encodeURIComponent(created.id)}/toolbox/process/session/${encodeURIComponent(session.sessionId)}`)
    }

    logStep(`smoke test passed for sandbox ${created.id}`)
  } finally {
    logStep(`deleting sandbox ${created.id}`)
    await request('DELETE', `/api/sandbox/${encodeURIComponent(created.id)}`)
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exit(1)
})
