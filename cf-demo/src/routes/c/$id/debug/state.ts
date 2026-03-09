import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/state')({
  server: {
    handlers: {
      GET: async ({ params }) => {
        try {
          const state = await resolveContainer(params.id).getState()
          return Response.json({ status: state.status })
        } catch (err) {
          return Response.json({ error: String(err) }, { status: 500 })
        }
      },
    },
  },
})
