import { PanelLeft, Plus } from 'lucide-react'

interface HeaderProps {
  sidebarOpen: boolean
  onToggleSidebar: () => void
  onNewSandbox?: () => void
}

export default function Header({
  sidebarOpen,
  onToggleSidebar,
  onNewSandbox,
}: HeaderProps) {
  return (
    <header className="h-10 flex items-center px-3 gap-3 border-b border-border bg-background select-none">
      <button
        onClick={onToggleSidebar}
        className="p-1 rounded hover:bg-accent transition-colors text-muted-foreground hover:text-foreground"
        aria-label={sidebarOpen ? 'Close sidebar' : 'Open sidebar'}
      >
        <PanelLeft size={16} />
      </button>
      <span className="text-xs font-mono font-bold tracking-wider text-foreground uppercase">
        Loophole
      </span>
      <span className="flex-1" />
      {onNewSandbox && (
        <button
          onClick={onNewSandbox}
          className="p-1 rounded hover:bg-accent transition-colors text-muted-foreground hover:text-foreground"
          aria-label="New sandbox"
        >
          <Plus size={16} />
        </button>
      )}
    </header>
  )
}
