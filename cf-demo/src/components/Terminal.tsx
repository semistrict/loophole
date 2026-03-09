import { useEffect, useRef } from 'react'
import { Terminal as XTerm } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import { WebLinksAddon } from '@xterm/addon-web-links'
import '@xterm/xterm/css/xterm.css'
import { shellWebSocketUrl } from '@/lib/api'

interface TerminalProps {
  containerId: string
  volume: string | null
  onTitleChange?: (title: string) => void
  onBell?: () => void
}

export default function Terminal({ containerId, volume, onTitleChange, onBell }: TerminalProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const termRef = useRef<XTerm | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const fitRef = useRef<FitAddon | null>(null)
  const connectedVolumeRef = useRef<string | null>(null)

  // Create xterm instance once on mount, never destroy until unmount.
  useEffect(() => {
    if (!containerRef.current) return

    const term = new XTerm({
      cursorBlink: true,
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', Menlo, monospace",
      fontSize: 13,
      lineHeight: 1.2,
      theme: {
        background: '#0a0a0a',
        foreground: '#e0e0e0',
        cursor: '#e0e0e0',
        selectionBackground: '#ffffff30',
        black: '#1a1a1a',
        red: '#ff6b6b',
        green: '#69db7c',
        yellow: '#ffd43b',
        blue: '#74c0fc',
        magenta: '#da77f2',
        cyan: '#66d9e8',
        white: '#e0e0e0',
        brightBlack: '#555555',
        brightRed: '#ff8787',
        brightGreen: '#8ce99a',
        brightYellow: '#ffe066',
        brightBlue: '#91d5ff',
        brightMagenta: '#e599f7',
        brightCyan: '#99e9f2',
        brightWhite: '#ffffff',
      },
    })

    const fit = new FitAddon()
    term.loadAddon(fit)
    term.loadAddon(new WebLinksAddon())

    termRef.current = term
    fitRef.current = fit

    term.open(containerRef.current)
    fit.fit()

    // Window resize → fit
    const onResize = () => fit.fit()
    window.addEventListener('resize', onResize)

    // ResizeObserver for container size changes
    const observer = new ResizeObserver(() => fit.fit())
    observer.observe(containerRef.current)

    return () => {
      window.removeEventListener('resize', onResize)
      observer.disconnect()
      term.dispose()
      termRef.current = null
      fitRef.current = null
    }
  }, [])

  // Connect/reconnect WebSocket when volume changes.
  // The xterm instance stays alive — we just swap the WS connection.
  useEffect(() => {
    const term = termRef.current
    if (!term) return

    // Close existing connection
    if (wsRef.current) {
      wsRef.current.close()
      wsRef.current = null
    }

    if (!volume) {
      if (connectedVolumeRef.current) {
        term.write('\r\n\x1b[90m[disconnected]\x1b[0m\r\n')
        connectedVolumeRef.current = null
      }
      return
    }

    // Clear terminal for new session
    if (connectedVolumeRef.current !== volume) {
      term.reset()
      term.write(`\x1b[90mConnecting to ${volume}...\x1b[0m\r\n`)
    }
    connectedVolumeRef.current = volume

    const ws = new WebSocket(shellWebSocketUrl(containerId, volume))
    ws.binaryType = 'arraybuffer'
    wsRef.current = ws

    ws.onopen = () => {
      ws.send(
        JSON.stringify({
          type: 'resize',
          cols: term.cols,
          rows: term.rows,
        }),
      )
    }

    ws.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        term.write(new Uint8Array(event.data))
      }
    }

    ws.onclose = () => {
      term.write('\r\n\x1b[90m[session ended]\x1b[0m\r\n')
    }

    // Terminal input → WebSocket
    const dataDisposable = term.onData((data) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(new TextEncoder().encode(data))
      }
    })

    // Resize events
    const resizeDisposable = term.onResize(({ cols, rows }) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'resize', cols, rows }))
      }
    })

    return () => {
      dataDisposable.dispose()
      resizeDisposable.dispose()
      ws.close()
      wsRef.current = null
    }
  }, [containerId, volume])

  // Register title/bell callbacks (separate effect so volume changes don't re-register xterm listeners)
  useEffect(() => {
    const term = termRef.current
    if (!term) return

    const titleDisposable = term.onTitleChange((title) => onTitleChange?.(title))
    const bellDisposable = term.onBell(() => onBell?.())

    return () => {
      titleDisposable.dispose()
      bellDisposable.dispose()
    }
  }, [onTitleChange, onBell])

  return (
    <div className="flex-1 flex flex-col min-h-0 relative">
      <div
        ref={containerRef}
        className="flex-1 min-h-0"
        style={{ backgroundColor: '#0a0a0a' }}
      />
      {!volume && (
        <div className="absolute inset-0 flex items-center justify-center bg-[#0a0a0a]/80 text-muted-foreground font-mono text-sm">
          Select a volume to open a shell
        </div>
      )}
    </div>
  )
}
