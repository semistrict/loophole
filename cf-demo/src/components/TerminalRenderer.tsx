/**
 * HTML-based terminal renderer.
 *
 * Renders rows as HTML <div>s with styled <span>s produced by the
 * server-side midterm VT emulator. Handles keyboard input via a
 * hidden <textarea> (required for mobile virtual keyboards).
 * No xterm.js dependency.
 */

import { useRef, useEffect, useState, useCallback } from 'react'
import type { TerminalScreen } from '@/hooks/useTerminalSessions'

interface TerminalRendererProps {
  screen: TerminalScreen
  onInput: (data: string) => void
  onResize?: (cols: number, rows: number) => void
  className?: string
}

export default function TerminalRenderer({
  screen,
  onInput,
  onResize,
  className = '',
}: TerminalRendererProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const measureRef = useRef<HTMLSpanElement>(null)
  const [charSize, setCharSize] = useState({ width: 7.8, height: 18 })

  // Measure character dimensions from a hidden probe element.
  useEffect(() => {
    const el = measureRef.current
    if (!el) return
    const rect = el.getBoundingClientRect()
    if (rect.width > 0 && rect.height > 0) {
      setCharSize({ width: rect.width, height: rect.height })
    }
  }, [])

  // Auto-focus textarea for keyboard input.
  useEffect(() => {
    textareaRef.current?.focus()
  }, [])

  // Focus textarea when container is clicked (skip if selecting text).
  const handleContainerClick = useCallback(() => {
    const selection = window.getSelection()
    if (selection && selection.toString().length > 0) return
    textareaRef.current?.focus()
  }, [])

  // Observe container size for resize notifications.
  useEffect(() => {
    const el = containerRef.current
    if (!el || !onResize || charSize.width === 0) return
    const ro = new ResizeObserver(() => {
      const { width, height } = el.getBoundingClientRect()
      const cols = Math.floor((width - 8) / charSize.width)
      const rows = Math.floor(height / charSize.height)
      if (cols > 0 && rows > 0) onResize(cols, rows)
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [charSize, onResize])

  // Blink cursor via direct DOM mutation (no re-renders).
  const cursorRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    let on = true
    const interval = setInterval(() => {
      on = !on
      if (cursorRef.current) {
        cursorRef.current.style.opacity = on ? '0.7' : '0'
      }
    }, 530)
    return () => clearInterval(interval)
  }, [])

  // Handle composing state (IME / iOS autocomplete).
  const composingRef = useRef(false)

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (composingRef.current) return

    // Let browser handle Cmd-shortcuts (copy, paste, etc.)
    if (e.metaKey) return

    let data = ''

    if (e.ctrlKey && !e.altKey) {
      if (e.key === 'v' || e.key === 'V') return
      if ((e.key === 'c' || e.key === 'C') && window.getSelection()?.toString()) return
      if (e.key.length === 1) {
        const code = e.key.toLowerCase().charCodeAt(0)
        if (code >= 97 && code <= 122) {
          data = String.fromCharCode(code - 96)
        }
      }
    } else if (e.altKey && !e.ctrlKey) {
      if (e.key.length === 1) data = '\x1b' + e.key
    } else if (!e.ctrlKey && !e.altKey) {
      switch (e.key) {
        case 'Enter':
          data = '\r'
          break
        case 'Backspace':
          data = '\x7f'
          break
        case 'Tab':
          data = '\t'
          break
        case 'Escape':
          data = '\x1b'
          break
        case 'ArrowUp':
          data = '\x1b[A'
          break
        case 'ArrowDown':
          data = '\x1b[B'
          break
        case 'ArrowRight':
          data = '\x1b[C'
          break
        case 'ArrowLeft':
          data = '\x1b[D'
          break
        case 'Home':
          data = '\x1b[H'
          break
        case 'End':
          data = '\x1b[F'
          break
        case 'Delete':
          data = '\x1b[3~'
          break
        case 'PageUp':
          data = '\x1b[5~'
          break
        case 'PageDown':
          data = '\x1b[6~'
          break
        default:
          if (e.key.length === 1) return
      }
    }

    if (data) {
      e.preventDefault()
      onInput(data)
    }
  }

  function handleTextareaInput(e: React.FormEvent<HTMLTextAreaElement>) {
    if (composingRef.current) return
    const ta = e.currentTarget
    const text = ta.value
    if (text) {
      onInput(text)
      ta.value = ''
    }
  }

  function handlePaste(e: React.ClipboardEvent) {
    e.preventDefault()
    const text = e.clipboardData.getData('text')
    if (text) onInput(text)
  }

  return (
    <div
      ref={containerRef}
      className={`relative overflow-hidden outline-none h-full select-text ${className}`}
      style={{
        background: '#0a0a0a',
        color: '#e4e4e7',
        fontFamily: "ui-monospace, 'JetBrains Mono', SFMono-Regular, Menlo, Monaco, Consolas, monospace",
        fontSize: '13px',
        lineHeight: `${charSize.height}px`,
        padding: '4px',
      }}
      onClick={handleContainerClick}
    >
      {/* Hidden textarea for keyboard input */}
      <textarea
        ref={textareaRef}
        onKeyDown={handleKeyDown}
        onInput={handleTextareaInput}
        onPaste={handlePaste}
        onCompositionStart={() => {
          composingRef.current = true
        }}
        onCompositionEnd={(e) => {
          composingRef.current = false
          const ta = e.currentTarget
          const text = ta.value
          if (text) {
            onInput(text)
            ta.value = ''
          }
        }}
        autoCapitalize="off"
        autoCorrect="off"
        autoComplete="off"
        spellCheck={false}
        aria-label="Terminal input"
        style={{
          position: 'absolute',
          opacity: 0,
          left: 0,
          top: 0,
          width: '1px',
          height: '1px',
          padding: 0,
          border: 'none',
          outline: 'none',
          resize: 'none',
          overflow: 'hidden',
          fontSize: '16px',
          transform: 'scale(0)',
          transformOrigin: '0 0',
        }}
      />

      {/* Hidden probe to measure monospace character dimensions */}
      <span
        ref={measureRef}
        aria-hidden
        style={{
          position: 'absolute',
          visibility: 'hidden',
          whiteSpace: 'pre',
          fontFamily: 'inherit',
          fontSize: 'inherit',
          lineHeight: 'inherit',
        }}
      >
        X
      </span>

      {screen.rows.map((html, y) => (
        <div
          key={y}
          style={{ height: `${charSize.height}px`, whiteSpace: 'pre' }}
          dangerouslySetInnerHTML={{ __html: html || '\u00a0' }}
        />
      ))}

      {/* Cursor overlay */}
      {screen.cursor.visible && (
        <div
          ref={cursorRef}
          style={{
            position: 'absolute',
            left: `${screen.cursor.x * charSize.width + 4}px`,
            top: `${screen.cursor.y * charSize.height + 4}px`,
            width: `${charSize.width}px`,
            height: `${charSize.height}px`,
            background: '#e4e4e7',
            opacity: 0.7,
            pointerEvents: 'none',
          }}
        />
      )}
    </div>
  )
}
