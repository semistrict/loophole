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

function hasValidSecret(request: Request, env: Env): boolean {
  const headerSecret = request.headers.get('X-Control-Secret')
  if (headerSecret === env.CONTROL_SECRET) {
    return true
  }
  return getCookie(request, AUTH_COOKIE) === env.CONTROL_SECRET
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

function authRedirect(url: URL, secret: string): Response {
  const redirectURL = new URL(url.toString())
  redirectURL.searchParams.delete('secret')
  const secure = url.protocol === 'https:' ? '; Secure' : ''
  return new Response(null, {
    status: 302,
    headers: {
      Location: redirectURL.toString(),
      'Set-Cookie': `${AUTH_COOKIE}=${encodeURIComponent(secret)}; HttpOnly${secure}; SameSite=Strict; Path=/`,
    },
  })
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname === '/health') {
      return new Response('ok', { status: 200 })
    }

    const providedSecret = url.searchParams.get('secret')
    if (providedSecret !== null) {
      if (providedSecret !== env.CONTROL_SECRET) {
        return unauthorized()
      }
      return authRedirect(url, providedSecret)
    }

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
