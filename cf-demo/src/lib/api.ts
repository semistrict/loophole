import { createServerFn } from '@tanstack/react-start'
import type { DirEntry } from './types'

export type { DirEntry }

async function schedulerJson<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string }
    throw new Error(body.error ?? res.statusText)
  }
  return (await res.json()) as T
}

function schedulerFetch(path: string, init?: RequestInit): Promise<Response> {
  const { env } = require('cloudflare:workers') as { env: Env }
  const scheduler = env.SCHEDULER.get(env.SCHEDULER.idFromName('scheduler'))
  return scheduler.fetch(
    new Request(`http://scheduler${path}`, init),
  )
}

function volumeFetch(volume: string, path: string, init?: RequestInit): Promise<Response> {
  const { env } = require('cloudflare:workers') as { env: Env }
  const actor = env.VOLUMES.get(env.VOLUMES.idFromName(volume))
  const headers = new Headers(init?.headers)
  headers.set('X-Volume', volume)
  return actor.fetch(
    new Request(`http://volume/${path}`, { ...init, headers }),
  )
}

export const listVolumes = createServerFn({ method: 'GET' })
  .handler(async () => {
    const res = await schedulerFetch('/api/volumes')
    const result = await schedulerJson<{ volumes?: string[] }>(res)
    return result.volumes ?? []
  })

export const createVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { volume: string }) => data)
  .handler(async ({ data }) => {
    const res = await schedulerFetch('/api/volumes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ volume: data.volume }),
    })
    await schedulerJson(res)
  })

export const snapshotVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { mountpoint: string; name: string }) => data)
  .handler(async ({ data }) => {
    const res = await schedulerFetch('/debug/snapshot', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mountpoint: data.mountpoint, name: data.name }),
    })
    await schedulerJson(res)
  })

export const cloneVolume = createServerFn({ method: 'POST' })
  .inputValidator(
    (data: { mountpoint: string; clone: string; cloneMountpoint: string }) => data,
  )
  .handler(async ({ data }) => {
    const res = await schedulerFetch('/debug/clone', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        mountpoint: data.mountpoint,
        clone: data.clone,
        clone_mountpoint: data.cloneMountpoint,
      }),
    })
    await schedulerJson(res)
  })

export const deleteVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { volume: string }) => data)
  .handler(async ({ data }) => {
    const res = await schedulerFetch('/api/volumes/' + encodeURIComponent(data.volume), {
      method: 'DELETE',
    })
    await schedulerJson(res)
  })

export const readDir = createServerFn({ method: 'GET' })
  .inputValidator((data: { volume: string; path: string }) => data)
  .handler(async ({ data }): Promise<DirEntry[]> => {
    const params = new URLSearchParams({ volume: data.volume, path: data.path })
    const res = await volumeFetch(data.volume, `sandbox/readdir?${params}`)
    return schedulerJson<DirEntry[]>(res)
  })
