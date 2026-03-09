import { createFileRoute, useParams } from '@tanstack/react-router'
import { useState, useCallback, useRef } from 'react'
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
  const [terminalTitle, setTerminalTitle] = useState('')
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [bellFlash, setBellFlash] = useState(false)
  const bellTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const handleBell = useCallback(() => {
    setBellFlash(true)
    if (bellTimerRef.current) clearTimeout(bellTimerRef.current)
    bellTimerRef.current = setTimeout(() => setBellFlash(false), 500)
  }, [])

  const containerQuery = useQuery({
    queryKey: ['container-state', containerId],
    queryFn: () => getContainerState({ data: { containerId } }),
    staleTime: 30_000,
  })

  const handleTitleChange = useCallback((title: string) => {
    setTerminalTitle(title)
  }, [])

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      <Header
        terminalTitle={terminalTitle}
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
              bellFlash={bellFlash}
            />
          </ErrorBoundary>
        )}
        <ErrorBoundary>
        <Terminal
          containerId={containerId}
          volume={selectedVolume}
          onTitleChange={handleTitleChange}
          onBell={handleBell}
        />
        </ErrorBoundary>
      </div>
      <footer className="h-6 flex items-center px-3 gap-3 border-t border-border bg-background text-xs font-mono text-muted-foreground select-none">
        <span className="flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
          {selectedVolume ? `connected — ${selectedVolume}` : 'ready'}
        </span>
        <span className="flex-1" />
        <span className="text-muted-foreground/60">
          {containerId}
          {containerQuery.data && ` [${containerQuery.data.status}]`}
        </span>
        <span>{terminalTitle || 'bash'}</span>
      </footer>
    </div>
  )
}
