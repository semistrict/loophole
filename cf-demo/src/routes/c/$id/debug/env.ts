import { createFileRoute } from '@tanstack/react-router'
import { env } from 'cloudflare:workers'

export const Route = createFileRoute('/c/$id/debug/env')({
  server: {
    handlers: {
      GET: async () => {
        return Response.json({
          R2_ENDPOINT: env.R2_ENDPOINT ? `set (${env.R2_ENDPOINT.length} chars)` : 'NOT SET',
          R2_BUCKET: env.R2_BUCKET ? `set (${env.R2_BUCKET})` : 'NOT SET',
          R2_ACCESS_KEY: env.R2_ACCESS_KEY ? `set (${env.R2_ACCESS_KEY.length} chars)` : 'NOT SET',
          R2_SECRET_KEY: env.R2_SECRET_KEY ? `set (${env.R2_SECRET_KEY.length} chars)` : 'NOT SET',
        })
      },
    },
  },
})
