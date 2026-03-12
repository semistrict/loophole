import { createFileRoute } from '@tanstack/react-router'
import { useState } from 'react'

import ErrorBoundary from '@/components/ErrorBoundary'
import Header from '@/components/Header'
import Sidebar from '@/components/Sidebar'

export const Route = createFileRoute('/')({ component: App })

function App() {
  const [selectedVolume, setSelectedVolume] = useState<string | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      <Header
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
          <main className="flex-1 min-h-0 flex items-center justify-center bg-background">
            <div className="max-w-md px-6 text-center">
              <h1 className="text-sm font-mono font-semibold text-foreground uppercase tracking-widest">
                {selectedVolume ?? 'Loophole'}
              </h1>
              <p className="mt-3 text-sm text-muted-foreground">
                {selectedVolume
                  ? 'Terminal access has been removed. Use the volume actions in the sidebar to inspect files, checkpoint, clone, or delete this volume.'
                  : 'Select a volume in the sidebar to inspect its files and manage checkpoints or clones.'}
              </p>
            </div>
          </main>
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
