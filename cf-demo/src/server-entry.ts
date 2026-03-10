import tanstackHandler from '@tanstack/react-start/server-entry'

export { SandboxContainer } from './container'
export { Scheduler } from './scheduler'
export { VolumeActor } from './volume-actor'

const VOLUME_PREFIX = /^\/v\/([^/]+)\/(.*)/  // /v/{volume}/{rest}
const API_PREFIX = /^\/api\//                 // /api/*
const DEBUG_PREFIX = /^\/debug\//             // /debug/*

export default {
  fetch(request: Request, ...args: unknown[]) {
    const url = new URL(request.url)

    // Per-volume routes → VolumeActor DO
    const volMatch = url.pathname.match(VOLUME_PREFIX)
    if (volMatch) {
      const volume = decodeURIComponent(volMatch[1])
      const rest = volMatch[2]
      const env = args[0] as Env
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

    // Global API + debug routes → Scheduler DO
    if (API_PREFIX.test(url.pathname) || DEBUG_PREFIX.test(url.pathname)) {
      const env = args[0] as Env
      const scheduler = env.SCHEDULER.get(env.SCHEDULER.idFromName('scheduler'))
      return scheduler.fetch(request)
    }

    // React app (TanStack Start)
    return (tanstackHandler as any).fetch(request, ...args)
  },
}
