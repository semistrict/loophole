import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/start')({
  server: {
    handlers: {
      POST: async ({ params }) => {
        try {
          await resolveContainer(params.id).start()
          return Response.json({ ok: true })
        } catch (err) {
          return Response.json({ error: String(err) }, { status: 500 })
        }
      },
    },
  },
})
