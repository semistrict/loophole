import { useState, useCallback, useEffect } from 'react'
import { ChevronRight, Folder, File } from 'lucide-react'
import { readDir } from '@/lib/api'
import type { DirEntry } from '@/lib/types'
import { cn } from '@/lib/utils'

interface FileTreeProps {
  volume: string
}

interface TreeNode {
  entry: DirEntry
  path: string
  children?: TreeNode[]
  loaded: boolean
  expanded: boolean
}

function FileTreeNode({
  node,
  depth,
  onToggle,
}: {
  node: TreeNode
  depth: number
  onToggle: (node: TreeNode) => void
}) {
  return (
    <div>
      <button
        className={cn(
          'flex items-center gap-1 w-full text-left py-0.5 px-1 text-xs font-mono',
          'hover:bg-accent/50 rounded-sm transition-colors',
          'text-muted-foreground hover:text-foreground',
        )}
        style={{ paddingLeft: `${depth * 12 + 4}px` }}
        onClick={() => node.entry.isDir && onToggle(node)}
      >
        {node.entry.isDir ? (
          <>
            <ChevronRight
              size={12}
              className={cn(
                'shrink-0 transition-transform',
                node.expanded && 'rotate-90',
              )}
            />
            <Folder size={12} className="shrink-0 text-muted-foreground" />
          </>
        ) : (
          <>
            <span className="w-3 shrink-0" />
            <File size={12} className="shrink-0 text-muted-foreground" />
          </>
        )}
        <span className="truncate">{node.entry.name}</span>
      </button>
      {node.expanded && node.children?.map((child) => (
        <FileTreeNode
          key={child.path}
          node={child}
          depth={depth + 1}
          onToggle={onToggle}
        />
      ))}
    </div>
  )
}

export default function FileTree({ volume }: FileTreeProps) {
  const [roots, setRoots] = useState<TreeNode[]>([])
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const loadDir = useCallback(
    async (dirPath: string): Promise<TreeNode[]> => {
      const raw = await readDir({ data: { volume, path: dirPath } })
      // Server functions may wrap the response; extract the array.
      const entries: DirEntry[] = Array.isArray(raw) ? raw : (raw as any).result ?? (raw as any).data ?? []
      entries.sort((a: DirEntry, b: DirEntry) => {
        if (a.isDir !== b.isDir) return a.isDir ? -1 : 1
        return a.name.localeCompare(b.name)
      })
      return entries.map((entry: DirEntry) => ({
        entry,
        path: dirPath === '/' ? `/${entry.name}` : `${dirPath}/${entry.name}`,
        loaded: false,
        expanded: false,
      }))
    },
    [volume],
  )

  const loadRoot = useCallback(async () => {
    try {
      setError(null)
      const nodes = await loadDir('/')
      setRoots(nodes)
      setLoaded(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : typeof err === 'string' ? err : JSON.stringify(err))
    }
  }, [loadDir])

  // Load on first render
  useEffect(() => {
    if (!loaded && !error) {
      loadRoot()
    }
  }, [loaded, error, loadRoot])

  const toggleNode = useCallback(
    async (target: TreeNode) => {
      if (!target.entry.isDir) return

      if (!target.loaded) {
        try {
          const children = await loadDir(target.path)
          target.children = children
          target.loaded = true
        } catch {
          return
        }
      }

      target.expanded = !target.expanded
      setRoots([...roots]) // trigger re-render
    },
    [roots, loadDir],
  )

  if (error) {
    return (
      <div className="text-xs text-destructive px-2 py-1 font-mono">{error}</div>
    )
  }

  if (!loaded) {
    return (
      <div className="text-xs text-muted-foreground px-2 py-1 font-mono">
        Loading...
      </div>
    )
  }

  if (roots.length === 0) {
    return (
      <div className="text-xs text-muted-foreground px-2 py-1 font-mono">
        Empty
      </div>
    )
  }

  return (
    <div className="overflow-y-auto">
      {roots.map((node) => (
        <FileTreeNode
          key={node.path}
          node={node}
          depth={0}
          onToggle={toggleNode}
        />
      ))}
    </div>
  )
}
