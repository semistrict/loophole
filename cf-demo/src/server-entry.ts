import { getContainer } from '@cloudflare/containers'
import tanstackHandler from '@tanstack/react-start/server-entry'

export { SandboxContainer } from './container'
export { Scheduler } from './scheduler'

const AUTH_COOKIE = 'loophole_control_secret'
const API_PREFIX = /^\/api\//
const VOLUME_PREFIX = /^\/v\//
const DEBUG_PREFIX = /^\/debug\//
const SANDBOX_PREFIX = /^\/sandbox(?:\/|$)/
const TOOLBOX_PREFIX = /^\/toolbox(?:\/|$)/
const CONTROL_PREFIX = /^\/debug\/control\/([^/]+)(?:\/(.*))?$/

function schedulerStub(env: Env) {
  return env.SCHEDULER.get(env.SCHEDULER.idFromName('scheduler'))
}

function getCookie(request: Request, name: string): string | null {
  const cookieHeader = request.headers.get('Cookie')
  if (!cookieHeader) {
    return null
  }

  for (const cookie of cookieHeader.split(';')) {
    const [rawName, ...rest] = cookie.trim().split('=')
    if (rawName === name) {
      return decodeURIComponent(rest.join('='))
    }
  }

  return null
}

function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  const encoder = new TextEncoder()
  const aBuf = encoder.encode(a)
  const bBuf = encoder.encode(b)
  // crypto.subtle.timingSafeEqual is not available in Workers;
  // manual constant-time compare.
  let result = 0
  for (let i = 0; i < aBuf.length; i++) {
    result |= aBuf[i] ^ bBuf[i]
  }
  return result === 0
}

function hasValidSecret(request: Request, env: Env): boolean {
  const headerSecret = request.headers.get('X-Control-Secret')
  if (headerSecret && constantTimeEqual(headerSecret, env.CONTROL_SECRET)) {
    return true
  }
  const cookieSecret = getCookie(request, AUTH_COOKIE)
  return cookieSecret !== null && constantTimeEqual(cookieSecret, env.CONTROL_SECRET)
}

function isRuntimePath(pathname: string): boolean {
  return API_PREFIX.test(pathname)
    || VOLUME_PREFIX.test(pathname)
    || DEBUG_PREFIX.test(pathname)
    || SANDBOX_PREFIX.test(pathname)
    || TOOLBOX_PREFIX.test(pathname)
}

function unauthorized(): Response {
  return new Response('unauthorized', { status: 401 })
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health') {
      return new Response('ok', { status: 200 })
    }

    // Authenticate via X-Control-Secret header or cookie.
    // The ?secret= query param flow was removed to avoid leaking
    // the secret in URLs (server logs, browser history, Referer headers).
    if (!hasValidSecret(request, env)) {
      return unauthorized()
    }

    const controlMatch = url.pathname.match(CONTROL_PREFIX)
    if (controlMatch) {
      const containerName = decodeURIComponent(controlMatch[1])
      const rest = controlMatch[2] || 'status'
      const container = getContainer(env.SANDBOX as any, containerName)
      const state = await container.getState()
      if (state.status !== 'running') {
        await container.startAndWaitForPorts()
      }
      const body =
        request.method !== 'GET' && request.method !== 'HEAD'
          ? await request.arrayBuffer()
          : null
      return container.fetch(
        new Request(`http://container/control/${rest}${url.search}`, {
          method: request.method,
          headers: request.headers,
          body,
        }),
      )
    }

    if (isRuntimePath(url.pathname)) {
      return schedulerStub(env).fetch(request)
    }

    return (tanstackHandler as any).fetch(request, env, ctx)
  },
}
