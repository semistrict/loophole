import { createFileRoute, useParams } from '@tanstack/react-router'
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { getContainerState } from '@/lib/api'
import ErrorBoundary from '@/components/ErrorBoundary'
import Header from '@/components/Header'
import Sidebar from '@/components/Sidebar'
import Terminal from '@/components/Terminal'

export const Route = createFileRoute('/c/$id/')({ component: App })

function App() {
  const { id: containerId } = useParams({ from: '/c/$id/' })
  const [selectedVolume, setSelectedVolume] = useState<string | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState(true)

  const containerQuery = useQuery({
    queryKey: ['container-state', containerId],
    queryFn: () => getContainerState({ data: { containerId } }),
    staleTime: 30_000,
  })

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      <Header
        terminalTitle={selectedVolume ?? ''}
        sidebarOpen={sidebarOpen}
        onToggleSidebar={() => setSidebarOpen((v) => !v)}
      />
      <div className="flex-1 flex min-h-0">
        {sidebarOpen && (
          <ErrorBoundary>
            <Sidebar
              containerId={containerId}
              selectedVolume={selectedVolume}
              onSelectVolume={setSelectedVolume}
              bellFlash={false}
            />
          </ErrorBoundary>
        )}
        <ErrorBoundary>
          <Terminal containerId={containerId} volume={selectedVolume} />
        </ErrorBoundary>
      </div>
      <footer className="h-6 flex items-center px-3 gap-3 border-t border-border bg-background text-xs font-mono text-muted-foreground select-none">
        <span className="flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
          {selectedVolume ? `connected` : 'ready'}
        </span>
        <span className="flex-1" />
        <span className="text-muted-foreground/60">
          {containerId}
          {containerQuery.data && ` [${containerQuery.data.status}]`}
        </span>
      </footer>
    </div>
  )
}
