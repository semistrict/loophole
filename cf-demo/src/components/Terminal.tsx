import { useState, useCallback, useEffect } from 'react'
import { Plus, X, AlertTriangle } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTerminalSessions } from '@/hooks/useTerminalSessions'
import TerminalRenderer from './TerminalRenderer'

interface TerminalProps {
  containerId: string
  volume: string | null
}

export default function Terminal({ containerId, volume }: TerminalProps) {
  const {
    sessions, screens, leaseError,
    createSession, breakLease, dismissLeaseError,
    writeToSession, resizeSession, killSession,
  } = useTerminalSessions(containerId)

  const [activeTab, setActiveTab] = useState<string | null>(null)
  const [breaking, setBreaking] = useState(false)

  // Auto-select the first open session, or create one when a volume is selected.
  useEffect(() => {
    if (!volume) return
    const existing = sessions.find((s) => s.volume === volume && !s.closed)
    if (existing) {
      if (!activeTab || !sessions.find((s) => s.id === activeTab && !s.closed)) {
        setActiveTab(existing.id)
      }
      return
    }
    if (leaseError?.volume === volume) return
    createSession(volume).then((id) => setActiveTab(id)).catch(() => {})
  }, [volume, sessions.length])

  // If active tab gets closed/removed, switch to another.
  useEffect(() => {
    if (activeTab && sessions.find((s) => s.id === activeTab)) return
    const open = sessions.find((s) => !s.closed)
    setActiveTab(open?.id ?? null)
  }, [sessions, activeTab])

  const handleNewTab = useCallback(() => {
    if (!volume) return
    createSession(volume).then((id) => setActiveTab(id)).catch(() => {})
  }, [volume, createSession])

  const handleCloseTab = useCallback(
    (id: string, e: React.MouseEvent) => {
      e.stopPropagation()
      killSession(id)
    },
    [killSession],
  )

  const handleBreakLease = useCallback(async () => {
    if (!leaseError) return
    setBreaking(true)
    try {
      await breakLease(leaseError.volume)
      // Retry creating the session after breaking the lease.
      const id = await createSession(leaseError.volume)
      setActiveTab(id)
    } catch {
      // breakLease or createSession failed — leaseError state will update
    } finally {
      setBreaking(false)
    }
  }, [leaseError, breakLease, createSession])

  const activeScreen = activeTab ? screens[activeTab] : null
  const openSessions = sessions.filter((s) => !s.closed || s.id === activeTab)

  if (!volume) {
    return (
      <div className="flex-1 flex items-center justify-center" style={{ background: '#0a0a0a' }}>
        <span className="font-mono text-sm" style={{ color: '#555' }}>
          Select a volume to open a shell
        </span>
      </div>
    )
  }

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Tab bar */}
      <div
        className="flex items-center h-8 shrink-0 select-none overflow-x-auto"
        style={{ background: '#111', borderBottom: '1px solid #222' }}
      >
        {openSessions.map((s) => {
          const label = s.title || s.volume
          return (
            <button
              key={s.id}
              onClick={() => setActiveTab(s.id)}
              className={cn(
                'group flex items-center gap-1.5 px-3 h-full text-xs font-mono transition-colors shrink-0',
                s.id === activeTab
                  ? 'bg-[#0a0a0a] text-[#e4e4e7]'
                  : 'text-[#777] hover:text-[#aaa] hover:bg-[#1a1a1a]',
                s.closed && 'opacity-50',
              )}
            >
              <span className="truncate max-w-40">
                {label}
                {s.closed ? ' (closed)' : ''}
              </span>
              <span
                onClick={(e) => handleCloseTab(s.id, e)}
                className="p-0.5 rounded opacity-0 group-hover:opacity-100 hover:bg-[#333] transition-opacity"
              >
                <X size={10} />
              </span>
            </button>
          )
        })}
        <button
          onClick={handleNewTab}
          className="flex items-center justify-center w-7 h-full text-[#555] hover:text-[#aaa] hover:bg-[#1a1a1a] transition-colors shrink-0"
          title="New terminal"
        >
          <Plus size={12} />
        </button>
      </div>

      {/* Lease error dialog */}
      {leaseError && (
        <div className="flex-1 flex items-center justify-center" style={{ background: '#0a0a0a' }}>
          <div
            className="max-w-md rounded-lg p-6 space-y-4"
            style={{ background: '#1a1a1a', border: '1px solid #333' }}
          >
            <div className="flex items-start gap-3">
              <AlertTriangle size={20} className="shrink-0 mt-0.5" style={{ color: '#e5a00d' }} />
              <div className="space-y-2">
                <h3 className="font-mono text-sm font-medium" style={{ color: '#e4e4e7' }}>
                  Volume in use by another instance
                </h3>
                <p className="font-mono text-xs leading-relaxed" style={{ color: '#888' }}>
                  {leaseError.message}
                </p>
                <p className="font-mono text-xs leading-relaxed" style={{ color: '#888' }}>
                  Would you like to request the other instance to release this volume?
                  The remote daemon will flush pending writes and unmount cleanly.
                </p>
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <button
                onClick={dismissLeaseError}
                className="px-3 py-1.5 rounded text-xs font-mono transition-colors"
                style={{ color: '#888', background: '#222', border: '1px solid #333' }}
              >
                Cancel
              </button>
              <button
                onClick={handleBreakLease}
                disabled={breaking}
                className="px-3 py-1.5 rounded text-xs font-mono transition-colors disabled:opacity-50"
                style={{ color: '#fff', background: '#b45309', border: '1px solid #d97706' }}
              >
                {breaking ? 'Requesting...' : 'Transfer ownership'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Terminal area */}
      {!leaseError && (activeScreen ? (
        <TerminalRenderer
          screen={activeScreen}
          onInput={(data) => activeTab && writeToSession(activeTab, data)}
          onResize={(cols, rows) => activeTab && resizeSession(activeTab, cols, rows)}
        />
      ) : (
        <div className="flex-1 flex items-center justify-center" style={{ background: '#0a0a0a' }}>
          <span className="font-mono text-xs" style={{ color: '#555' }}>
            {sessions.some((s) => !s.closed)
              ? 'Connecting...'
              : 'Starting terminal...'}
          </span>
        </div>
      ))}
    </div>
  )
}
