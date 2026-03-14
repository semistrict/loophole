import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'

export function loadControlSecret(cwd = process.cwd()): string {
  const envSecret = process.env.CONTROL_SECRET
  if (envSecret) {
    return envSecret
  }

  const candidates = [
    resolve(cwd, '.dev.vars'),
    resolve(cwd, 'cf-demo/.dev.vars'),
    resolve(cwd, '../.dev.vars'),
    resolve(cwd, '../cf-demo/.dev.vars'),
  ]
  const varsPath = candidates.find((candidate) => existsSync(candidate))
  if (!varsPath) {
    throw new Error('CONTROL_SECRET is not set and no cf-demo .dev.vars file was found')
  }

  const lines = readFileSync(varsPath, 'utf8').split(/\r?\n/)
  for (const line of lines) {
    if (!line.startsWith('CONTROL_SECRET=')) {
      continue
    }
    const [, ...rest] = line.split('=')
    const value = rest.join('=').trim()
    if (value) {
      return value
    }
  }

  throw new Error('CONTROL_SECRET was not found in cf-demo/.dev.vars')
}

export async function authedFetch(
  path: string,
  init: RequestInit = {},
  options: { baseUrl?: string; secret?: string } = {},
): Promise<Response> {
  const baseUrl = options.baseUrl ?? process.env.CF_DEMO_BASE_URL ?? 'http://localhost:7935'
  const secret = options.secret ?? loadControlSecret()
  const url = new URL(path, baseUrl)
  const headers = new Headers(init.headers)
  headers.set('X-Control-Secret', secret)
  return fetch(url, { ...init, headers })
}
