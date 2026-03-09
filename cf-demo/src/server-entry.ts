import tanstackHandler from '@tanstack/react-start/server-entry'
import { resolveContainer } from './lib/container'

export { SandboxContainer } from './container'

// WebSocket upgrades can't go through TanStack Start — intercept them here.
// Path: /c/:id/sandbox/shell?volume=...
const WS_PREFIX = /^\/c\/([^/]+)\/sandbox\//

export default {
  fetch(request: Request, ...args: unknown[]) {
    if (request.headers.get('Upgrade') === 'websocket') {
      const url = new URL(request.url)
      const match = url.pathname.match(WS_PREFIX)
      if (!match) {
        return new Response('WebSocket path must be /c/:id/sandbox/...', { status: 400 })
      }
      const containerId = decodeURIComponent(match[1])
      const rest = url.pathname.slice(match[0].length) // e.g. "shell"
      const container = resolveContainer(containerId)
      return container.fetch(
        new Request(`http://container/sandbox/${rest}${url.search}`, {
          headers: request.headers,
        }),
      )
    }
    return (tanstackHandler as any).fetch(request, ...args)
  },
}
