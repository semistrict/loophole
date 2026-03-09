import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/test')({
  server: {
    handlers: {
      GET: async ({ params }) => {
        try {
          const container = resolveContainer(params.id)
          const state = await container.getState()
          let fetchResult: string
          try {
            const res = await container.fetch(new Request('http://container/status'))
            fetchResult = `status=${res.status} body=${await res.text()}`
          } catch (err) {
            fetchResult = `fetch error: ${err}`
          }
          return Response.json({ status: state.status, fetchResult })
        } catch (err) {
          return Response.json({ error: String(err) }, { status: 500 })
        }
      },
    },
  },
})
