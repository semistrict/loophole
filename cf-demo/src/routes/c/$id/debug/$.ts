import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/$')({
  server: {
    handlers: {
      GET: async ({ request, params }) => proxy(request, params.id, params._splat ?? ''),
      POST: async ({ request, params }) => proxy(request, params.id, params._splat ?? ''),
    },
  },
})

async function proxy(request: Request, containerId: string, path: string): Promise<Response> {
  try {
    const container = resolveContainer(containerId)
    const url = new URL(request.url)
    const containerReq = new Request(`http://container/${path}${url.search}`, {
      method: request.method,
      headers: request.headers,
      body: request.method !== 'GET' ? request.body : undefined,
    })
    const res = await container.fetch(containerReq)
    return new Response(res.body, {
      status: res.status,
      headers: { 'content-type': res.headers.get('content-type') ?? 'application/json' },
    })
  } catch (err) {
    return Response.json({ error: String(err) }, { status: 502 })
  }
}
