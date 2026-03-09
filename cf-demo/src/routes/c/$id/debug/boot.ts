import { createFileRoute } from '@tanstack/react-router'
import { resolveContainer } from '@/lib/container'

export const Route = createFileRoute('/c/$id/debug/boot')({
  server: {
    handlers: {
      POST: async ({ params }) => {
        try {
          const container = resolveContainer(params.id)
          await container.destroy().catch(() => {})
          await container.startAndWaitForPorts()
          const state = await container.getState()
          return Response.json({ ok: true, status: state.status })
        } catch (err) {
          const state = await resolveContainer(params.id).getState().catch(() => null)
          return Response.json({ error: String(err), state }, { status: 500 })
        }
      },
    },
  },
})
