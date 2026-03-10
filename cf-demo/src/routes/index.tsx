import { createFileRoute } from '@tanstack/react-router'
import { useState } from 'react'

import ErrorBoundary from '@/components/ErrorBoundary'
import Header from '@/components/Header'
import Sidebar from '@/components/Sidebar'
import Terminal from '@/components/Terminal'

export const Route = createFileRoute('/')({ component: App })

function App() {
  const [selectedVolume, setSelectedVolume] = useState<string | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState(true)

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
              selectedVolume={selectedVolume}
              onSelectVolume={setSelectedVolume}
              bellFlash={false}
            />
          </ErrorBoundary>
        )}
        <ErrorBoundary>
          <Terminal volume={selectedVolume} />
        </ErrorBoundary>
      </div>
      <footer className="h-6 flex items-center px-3 gap-3 border-t border-border bg-background text-xs font-mono text-muted-foreground select-none">
        <span className="flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
          {selectedVolume ? 'connected' : 'ready'}
        </span>
      </footer>
    </div>
  )
}
