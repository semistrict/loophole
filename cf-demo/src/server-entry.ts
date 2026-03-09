import tanstackHandler from '@tanstack/react-start/server-entry'
import { resolveContainer } from './lib/container'

export { SandboxContainer } from './container'

// All /c/:id/sandbox/* requests are proxied directly to the container.
// TanStack Start splat routes don't reliably match these, so we intercept here.
const SANDBOX_PREFIX = /^\/c\/([^/]+)\/sandbox\//

export default {
  fetch(request: Request, ...args: unknown[]) {
    const url = new URL(request.url)
    const match = url.pathname.match(SANDBOX_PREFIX)
    if (match) {
      const containerId = decodeURIComponent(match[1])
      const rest = url.pathname.slice(match[0].length)
      const container = resolveContainer(containerId)
      return container.fetch(
        new Request(`http://container/sandbox/${rest}${url.search}`, {
          method: request.method,
          headers: request.headers,
          body: request.method !== 'GET' ? request.body : undefined,
        }),
      )
    }
    return (tanstackHandler as any).fetch(request, ...args)
  },
}
