import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/destroy')({
  server: {
    handlers: {
      POST: async ({ params }) => {
        try {
          await resolveContainer(params.id).destroy()
          return Response.json({ ok: true })
        } catch (err) {
          return Response.json({ error: String(err) }, { status: 500 })
        }
      },
    },
  },
})
