/**
 * Terminal session manager — manages WebSocket connections to
 * server-side PTY sessions backed by the midterm VT emulator.
 *
 * WS protocol (server → client):
 *   screen  – full state on connect {rows, cursor, width, height, title}
 *   update  – changed rows only {rows: {idx: html}, cursor, title?}
 *   closed  – PTY exited
 *
 * WS protocol (client → server):
 *   input   – keyboard data {type:"input", data:"..."}
 *   resize  – terminal resize {type:"resize", cols, rows}
 */

import { useRef, useState, useCallback, useEffect } from 'react'

export interface TerminalSession {
  id: string
  volume: string
  title: string
  closed: boolean
}

export interface TerminalScreen {
  rows: string[]
  cursor: { x: number; y: number; visible: boolean }
  width: number
  height: number
}

export interface LeaseError {
  volume: string
  message: string
}

interface SessionInternal {
  ws: WebSocket
}

let sessionCounter = 0

export function useTerminalSessions(containerId: string) {
  const [sessions, setSessions] = useState<TerminalSession[]>([])
  const [screens, setScreens] = useState<Record<string, TerminalScreen>>({})
  const [leaseError, setLeaseError] = useState<LeaseError | null>(null)
  const internalsRef = useRef(new Map<string, SessionInternal>())
  const restoredRef = useRef(false)

  function sandboxUrl(path: string) {
    return `/c/${encodeURIComponent(containerId)}/sandbox/${path}`
  }

  function connectSession(sessionId: string) {
    // Don't reconnect if already connected.
    if (internalsRef.current.has(sessionId)) return

    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const wsUrl = `${proto}//${window.location.host}/c/${encodeURIComponent(containerId)}/sandbox/pty/${encodeURIComponent(sessionId)}/ws`
    const ws = new WebSocket(wsUrl)

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data as string)
        switch (msg.type) {
          case 'screen':
            setScreens((prev) => ({
              ...prev,
              [sessionId]: {
                rows: msg.rows,
                cursor: msg.cursor,
                width: msg.width,
                height: msg.height,
              },
            }))
            if (msg.title) {
              setSessions((prev) =>
                prev.map((s) => (s.id === sessionId ? { ...s, title: msg.title } : s)),
              )
            }
            break
          case 'update': {
            setScreens((prev) => {
              const current = prev[sessionId]
              if (!current) return prev
              const newRows = [...current.rows]
              for (const [idx, html] of Object.entries(msg.rows as Record<string, string>)) {
                newRows[Number(idx)] = html
              }
              return {
                ...prev,
                [sessionId]: {
                  ...current,
                  rows: newRows,
                  cursor: msg.cursor,
                },
              }
            })
            if (msg.title) {
              setSessions((prev) =>
                prev.map((s) => (s.id === sessionId ? { ...s, title: msg.title } : s)),
              )
            }
            break
          }
          case 'closed':
            setSessions((prev) =>
              prev.map((s) => (s.id === sessionId ? { ...s, closed: true } : s)),
            )
            break
        }
      } catch {
        // Malformed JSON — skip
      }
    }

    ws.onclose = () => {
      internalsRef.current.delete(sessionId)
      setSessions((prev) =>
        prev.map((s) => (s.id === sessionId ? { ...s, closed: true } : s)),
      )
    }

    internalsRef.current.set(sessionId, { ws })
    return ws
  }

  // Restore existing sessions from the daemon on mount.
  useEffect(() => {
    if (restoredRef.current) return
    restoredRef.current = true

    fetch(sandboxUrl('pty'))
      .then((res) => res.json() as Promise<{ sessions?: Array<{ id: string; volume: string; title: string }> }>)
      .then((data) => {
        const existing = data.sessions ?? []
        if (existing.length === 0) return
        setSessions((prev) => {
          const knownIds = new Set(prev.map((s) => s.id))
          const toAdd = existing
            .filter((s) => !knownIds.has(s.id))
            .map((s) => ({ id: s.id, volume: s.volume, title: s.title || '', closed: false }))
          return [...prev, ...toAdd]
        })
        for (const s of existing) {
          connectSession(s.id)
        }
      })
      .catch(() => {})
  }, [containerId])

  const createSession = useCallback(
    async (volume: string, cols = 120, rows = 30): Promise<string> => {
      const id = `${volume}-${++sessionCounter}`

      const res = await fetch(sandboxUrl('pty'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, volume, cols, rows }),
      })
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string }
        const msg = body.error ?? `Failed to create PTY (${res.status})`
        if (msg.includes('lease held by')) {
          setLeaseError({ volume, message: msg })
          throw new Error(msg)
        }
        throw new Error(msg)
      }

      setLeaseError(null)
      setSessions((prev) => [...prev, { id, volume, title: '', closed: false }])
      connectSession(id)
      return id
    },
    [containerId],
  )

  const breakLease = useCallback(
    async (volume: string): Promise<void> => {
      const res = await fetch(sandboxUrl('break-lease'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume, force: false }),
      })
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string }
        throw new Error(body.error ?? `Break lease failed (${res.status})`)
      }
      setLeaseError(null)
    },
    [containerId],
  )

  const dismissLeaseError = useCallback(() => setLeaseError(null), [])

  function writeToSession(id: string, data: string) {
    const internal = internalsRef.current.get(id)
    if (!internal || internal.ws.readyState !== WebSocket.OPEN) return
    internal.ws.send(JSON.stringify({ type: 'input', data }))
  }

  function resizeSession(id: string, cols: number, rows: number) {
    const internal = internalsRef.current.get(id)
    if (!internal || internal.ws.readyState !== WebSocket.OPEN) return
    internal.ws.send(JSON.stringify({ type: 'resize', cols, rows }))
  }

  function killSession(id: string) {
    const internal = internalsRef.current.get(id)
    if (internal) {
      internal.ws.close()
      internalsRef.current.delete(id)
    }
    setSessions((prev) => prev.filter((s) => s.id !== id))
    setScreens((prev) => {
      const next = { ...prev }
      delete next[id]
      return next
    })

    fetch(sandboxUrl(`pty/${encodeURIComponent(id)}`), { method: 'DELETE' }).catch(() => {})
  }

  return {
    sessions,
    screens,
    leaseError,
    createSession,
    breakLease,
    dismissLeaseError,
    writeToSession,
    resizeSession,
    killSession,
  }
}
