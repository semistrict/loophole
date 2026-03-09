import { createServerFn } from '@tanstack/react-start'
import { resolveContainer } from './container'
import type { DirEntry } from './types'

export type { DirEntry }

async function containerJson<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string }
    throw new Error(body.error ?? res.statusText)
  }
  return (await res.json()) as T
}

export const getContainerState = createServerFn({ method: 'GET' })
  .inputValidator((data: { containerId: string }) => data)
  .handler(async ({ data }) => {
    const c = resolveContainer(data.containerId)
    const state = await c.getState()
    return { status: state.status }
  })

export const listVolumes = createServerFn({ method: 'GET' })
  .inputValidator((data: { containerId: string }) => data)
  .handler(async ({ data }) => {
    const res = await resolveContainer(data.containerId).fetch(
      new Request('http://container/volumes'),
    )
    const result = await containerJson<{ volumes?: string[] }>(res)
    return result.volumes ?? []
  })

export const createVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { containerId: string; volume: string }) => data)
  .handler(async ({ data }) => {
    const res = await resolveContainer(data.containerId).fetch(
      new Request('http://container/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume: data.volume }),
      }),
    )
    await containerJson(res)
  })

export const snapshotVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { containerId: string; mountpoint: string; name: string }) => data)
  .handler(async ({ data }) => {
    const res = await resolveContainer(data.containerId).fetch(
      new Request('http://container/snapshot', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mountpoint: data.mountpoint, name: data.name }),
      }),
    )
    await containerJson(res)
  })

export const cloneVolume = createServerFn({ method: 'POST' })
  .inputValidator(
    (data: { containerId: string; mountpoint: string; clone: string; cloneMountpoint: string }) => data,
  )
  .handler(async ({ data }) => {
    const res = await resolveContainer(data.containerId).fetch(
      new Request('http://container/clone', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mountpoint: data.mountpoint,
          clone: data.clone,
          clone_mountpoint: data.cloneMountpoint,
        }),
      }),
    )
    await containerJson(res)
  })

export const deleteVolume = createServerFn({ method: 'POST' })
  .inputValidator((data: { containerId: string; volume: string }) => data)
  .handler(async ({ data }) => {
    const res = await resolveContainer(data.containerId).fetch(
      new Request('http://container/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume: data.volume }),
      }),
    )
    await containerJson(res)
  })

export const readDir = createServerFn({ method: 'GET' })
  .inputValidator((data: { containerId: string; volume: string; path: string }) => data)
  .handler(async ({ data }): Promise<DirEntry[]> => {
    const params = new URLSearchParams({ volume: data.volume, path: data.path })
    const res = await resolveContainer(data.containerId).fetch(
      new Request(`http://container/sandbox/readdir?${params}`),
    )
    return containerJson<DirEntry[]>(res)
  })

