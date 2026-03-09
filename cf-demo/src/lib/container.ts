import { env } from 'cloudflare:workers'
import { getContainer } from '@cloudflare/containers'

/**
 * Resolve a container by name. The server never hardcodes a container name —
 * it always comes from the URL path.
 */
export function resolveContainer(name: string) {
  return getContainer(env.SANDBOX, name)
}
