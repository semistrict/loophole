import { createFileRoute, redirect } from '@tanstack/react-router'

const DEFAULT_CONTAINER = 'cf-singleton-container'

export const Route = createFileRoute('/')({
  beforeLoad: () => {
    throw redirect({ to: '/c/$id', params: { id: DEFAULT_CONTAINER } })
  },
})
