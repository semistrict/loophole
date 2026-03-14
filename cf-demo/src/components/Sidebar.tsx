import { useEffect, useRef, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  HardDrive,
  Plus,
  Camera,
  Copy,
  Trash2,
  RefreshCw,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  listVolumes,
  createVolume,
  checkpointVolume,
  cloneVolume,
  deleteVolume,
} from '@/lib/api'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import FileTree from './FileTree'

interface SidebarProps {
  selectedVolume: string | null
  onSelectVolume: (volume: string | null) => void
  bellFlash: boolean
}

type ModalState =
  | { type: 'create' }
  | { type: 'checkpoint'; volume: string }
  | { type: 'clone'; volume: string }
  | { type: 'delete'; volume: string }
  | null

export default function Sidebar({
  selectedVolume,
  onSelectVolume,
  bellFlash,
}: SidebarProps) {
  const [modal, setModal] = useState<ModalState>(null)
  const [modalError, setModalError] = useState<string | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const queryClient = useQueryClient()

  const volumesQuery = useQuery({
    queryKey: ['volumes'],
    queryFn: () => listVolumes(),
  })

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['volumes'] })

  const createMutation = useMutation({
    mutationFn: (input: { volume: string }) => createVolume(input),
    onSuccess: (_result, variables) => {
      setModal(null)
      onSelectVolume(variables.volume)
      invalidate()
    },
    onError: (err) => setModalError(err instanceof Error ? err.message : JSON.stringify(err)),
  })

  const checkpointMutation = useMutation({
    mutationFn: (input: { volume: string }) => checkpointVolume(input),
    onSuccess: () => { setModal(null); invalidate() },
    onError: (err) => setModalError(err instanceof Error ? err.message : JSON.stringify(err)),
  })

  const cloneMutation = useMutation({
    mutationFn: (input: { volume: string; clone: string }) => cloneVolume(input),
    onSuccess: (_result, variables) => {
      setModal(null)
      onSelectVolume(variables.clone)
      invalidate()
    },
    onError: (err) => setModalError(err instanceof Error ? err.message : JSON.stringify(err)),
  })

  const deleteMutation = useMutation({
    mutationFn: (input: { volume: string }) => deleteVolume(input),
    onSuccess: (_result, variables) => {
      setModal(null)
      if (selectedVolume === variables.volume) {
        onSelectVolume(null)
      }
      invalidate()
    },
    onError: (err) => setModalError(err instanceof Error ? err.message : JSON.stringify(err)),
  })

  const submitting =
    createMutation.isPending ||
    checkpointMutation.isPending ||
    cloneMutation.isPending ||
    deleteMutation.isPending

  const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!modal) return
    setModalError(null)

    const form = new FormData(e.currentTarget)

    switch (modal.type) {
      case 'create': {
        const name = (form.get('name') as string).trim()
        if (!name) { setModalError('Name is required'); return }
        createMutation.mutate({ volume: name })
        break
      }
      case 'checkpoint': {
        checkpointMutation.mutate({ volume: modal.volume })
        break
      }
      case 'clone': {
        const name = (form.get('name') as string).trim()
        if (!name) { setModalError('Name is required'); return }
        cloneMutation.mutate({ volume: modal.volume, clone: name })
        break
      }
      case 'delete':
        deleteMutation.mutate({ volume: modal.volume })
        break
    }
  }

  const rawData = volumesQuery.data
  const volumes: string[] = Array.isArray(rawData) ? rawData : []

  useEffect(() => {
    if (!selectedVolume || volumesQuery.isLoading) {
      return
    }
    if (!volumes.includes(selectedVolume)) {
      onSelectVolume(null)
    }
  }, [onSelectVolume, selectedVolume, volumes, volumesQuery.isLoading])

  return (
    <aside
      className={cn(
        'w-60 flex-shrink-0 flex flex-col border-r border-border bg-background overflow-hidden transition-shadow duration-500',
        bellFlash && 'shadow-[inset_0_0_12px_rgba(255,200,50,0.3)]',
      )}
    >
      {/* Volumes */}
      <div className="flex-shrink-0">
        <div className="flex items-center justify-between px-3 py-2 border-b border-border">
          <span className="text-[10px] font-mono font-semibold text-muted-foreground uppercase tracking-widest">
            Volumes
          </span>
          <div className="flex items-center gap-0.5">
            <Button variant="ghost" size="icon-xs" onClick={() => invalidate()} title="Refresh">
              <RefreshCw size={12} />
            </Button>
            <Button variant="ghost" size="icon-xs" onClick={() => setModal({ type: 'create' })} title="Create volume">
              <Plus size={12} />
            </Button>
          </div>
        </div>

        {volumesQuery.error && (
          <div className="px-3 py-1 text-xs text-destructive font-mono">
            {typeof volumesQuery.error.message === 'string' ? volumesQuery.error.message : JSON.stringify(volumesQuery.error)}
          </div>
        )}

        <div className="overflow-y-auto max-h-48">
          {volumesQuery.isLoading && (
            <div className="px-3 py-2 text-xs text-muted-foreground font-mono">Loading...</div>
          )}
          {volumes.map((vol) => (
            <div
              key={vol}
              className={cn(
                'group flex items-center gap-2 px-3 py-1.5 cursor-pointer text-xs font-mono transition-colors',
                selectedVolume === vol
                  ? 'bg-accent text-accent-foreground'
                  : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground',
              )}
              onClick={() => onSelectVolume(vol)}
            >
              <HardDrive size={12} className="shrink-0" />
              <span className="truncate flex-1">{vol}</span>
              <div className="hidden group-hover:flex items-center gap-0.5">
                <button
                  onClick={(e) => { e.stopPropagation(); setModal({ type: 'checkpoint', volume: vol }) }}
                  className="p-0.5 rounded hover:bg-background/50"
                  title="Checkpoint"
                >
                  <Camera size={10} />
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); setModal({ type: 'clone', volume: vol }) }}
                  className="p-0.5 rounded hover:bg-background/50"
                  title="Clone"
                >
                  <Copy size={10} />
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); setModal({ type: 'delete', volume: vol }) }}
                  className="p-0.5 rounded hover:bg-background/50 text-destructive"
                  title="Delete"
                >
                  <Trash2 size={10} />
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Files */}
      {selectedVolume && (
        <div className="flex-1 flex flex-col min-h-0 border-t border-border">
          <div className="px-3 py-2 border-b border-border">
            <span className="text-[10px] font-mono font-semibold text-muted-foreground uppercase tracking-widest">
              Files
            </span>
          </div>
          <div className="flex-1 overflow-y-auto">
            <FileTree key={selectedVolume} volume={selectedVolume} />
          </div>
        </div>
      )}

      {/* Create Dialog */}
      <Dialog open={modal?.type === 'create'} onOpenChange={(open) => { if (!open) setModal(null); setModalError(null) }}>
        <DialogContent>
          <form onSubmit={handleSubmit}>
            <DialogHeader>
              <DialogTitle>Create Volume</DialogTitle>
              <DialogDescription>Create a new loophole volume.</DialogDescription>
            </DialogHeader>
            <div className="grid gap-3 py-4">
              <div className="grid gap-1.5">
                <Label htmlFor="create-name">Name</Label>
                <Input ref={inputRef} id="create-name" name="name" placeholder="my-volume" autoFocus />
              </div>
              {modalError && <p className="text-xs text-destructive">{modalError}</p>}
            </div>
            <DialogFooter>
              <Button type="submit" disabled={submitting} size="sm">
                {submitting ? 'Creating...' : 'Create'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Checkpoint Dialog */}
      <Dialog open={modal?.type === 'checkpoint'} onOpenChange={(open) => { if (!open) setModal(null); setModalError(null) }}>
        <DialogContent>
          <form onSubmit={handleSubmit}>
            <DialogHeader>
              <DialogTitle>Checkpoint</DialogTitle>
              <DialogDescription>
                Create a checkpoint of <span className="font-mono text-foreground">{modal?.type === 'checkpoint' ? modal.volume : ''}</span>. The system will assign the checkpoint ID.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-3 py-4">
              {modalError && <p className="text-xs text-destructive">{modalError}</p>}
            </div>
            <DialogFooter>
              <Button type="submit" disabled={submitting} size="sm">
                {submitting ? 'Creating...' : 'Checkpoint'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Clone Dialog */}
      <Dialog open={modal?.type === 'clone'} onOpenChange={(open) => { if (!open) setModal(null); setModalError(null) }}>
        <DialogContent>
          <form onSubmit={handleSubmit}>
            <DialogHeader>
              <DialogTitle>Clone</DialogTitle>
              <DialogDescription>
                Clone <span className="font-mono text-foreground">{modal?.type === 'clone' ? modal.volume : ''}</span> into a new volume.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-3 py-4">
              <div className="grid gap-1.5">
                <Label htmlFor="clone-name">Clone name</Label>
                <Input id="clone-name" name="name" placeholder="my-volume-clone" autoFocus />
              </div>
              {modalError && <p className="text-xs text-destructive">{modalError}</p>}
            </div>
            <DialogFooter>
              <Button type="submit" disabled={submitting} size="sm">
                {submitting ? 'Cloning...' : 'Clone'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Delete Dialog */}
      <Dialog open={modal?.type === 'delete'} onOpenChange={(open) => { if (!open) setModal(null); setModalError(null) }}>
        <DialogContent>
          <form onSubmit={handleSubmit}>
            <DialogHeader>
              <DialogTitle>Delete Volume</DialogTitle>
              <DialogDescription>
                Permanently delete <span className="font-mono text-foreground">{modal?.type === 'delete' ? modal.volume : ''}</span>? This cannot be undone.
              </DialogDescription>
            </DialogHeader>
            {modalError && <p className="text-xs text-destructive py-2">{modalError}</p>}
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={() => setModal(null)} type="button">Cancel</Button>
              <Button variant="destructive" type="submit" disabled={submitting} size="sm">
                {submitting ? 'Deleting...' : 'Delete'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </aside>
  )
}
