import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { authedFetch, loadControlSecret } from './client-lib'

const originalControlSecret = process.env.CONTROL_SECRET
const originalBaseURL = process.env.CF_DEMO_BASE_URL

function restoreEnvVar(name: 'CONTROL_SECRET' | 'CF_DEMO_BASE_URL', value: string | undefined) {
  if (value === undefined) {
    Reflect.deleteProperty(process.env, name)
    return
  }
  process.env[name] = value
}

afterEach(() => {
  restoreEnvVar('CONTROL_SECRET', originalControlSecret)
  restoreEnvVar('CF_DEMO_BASE_URL', originalBaseURL)

  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('loadControlSecret', () => {
  it('prefers CONTROL_SECRET from the environment', () => {
    process.env.CONTROL_SECRET = 'env-secret'

    expect(loadControlSecret('/definitely/missing')).toBe('env-secret')
  })

  it('reads CONTROL_SECRET from .dev.vars in the provided cwd', () => {
    Reflect.deleteProperty(process.env, 'CONTROL_SECRET')
    const tempDir = mkdtempSync(join(tmpdir(), 'cf-demo-client-lib-'))

    try {
      writeFileSync(join(tempDir, '.dev.vars'), 'CONTROL_SECRET=file-secret\n')

      expect(loadControlSecret(tempDir)).toBe('file-secret')
    } finally {
      rmSync(tempDir, { recursive: true, force: true })
    }
  })
})

describe('authedFetch', () => {
  it('attaches the shared secret header', async () => {
    process.env.CONTROL_SECRET = 'attached-secret'

    const fetchMock = vi.fn<(input: URL, init?: RequestInit) => Promise<Response>>(
      async () => new Response('ok', { status: 200 }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await authedFetch('/api/sandbox', { method: 'POST' }, { baseUrl: 'https://example.com' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const call = fetchMock.mock.calls[0]
    expect(call).toBeDefined()
    const input = call![0]
    const init = call![1]
    expect(init).toBeDefined()
    expect(input.toString()).toBe('https://example.com/api/sandbox')
    expect(new Headers(init!.headers).get('X-Control-Secret')).toBe('attached-secret')
  })
})
