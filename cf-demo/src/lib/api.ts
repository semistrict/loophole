import type { DirEntry } from './types'

export type { DirEntry }

async function runtimeJson<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string }
    throw new Error(body.error ?? res.statusText)
  }
  return (await res.json()) as T
}

function runtimeFetch(path: string, init?: RequestInit): Promise<Response> {
  return fetch(path, {
    credentials: 'same-origin',
    ...init,
  })
}

export async function listVolumes(): Promise<string[]> {
  const res = await runtimeFetch('/api/volumes')
  const result = await runtimeJson<{ volumes?: string[] }>(res)
  return result.volumes ?? []
}

export async function createVolume(data: { volume: string }): Promise<void> {
  const res = await runtimeFetch('/api/volumes', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ volume: data.volume }),
  })
  await runtimeJson(res)
}

export async function checkpointVolume(data: { volume: string }): Promise<void> {
  const res = await runtimeFetch(`/api/volumes/${encodeURIComponent(data.volume)}/checkpoint`, {
    method: 'POST',
  })
  await runtimeJson(res)
}

export async function cloneVolume(data: { volume: string; clone: string }): Promise<void> {
  const res = await runtimeFetch(`/api/volumes/${encodeURIComponent(data.volume)}/clone`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clone: data.clone }),
  })
  await runtimeJson(res)
}

export async function deleteVolume(data: { volume: string }): Promise<void> {
  const res = await runtimeFetch('/api/volumes/' + encodeURIComponent(data.volume), {
    method: 'DELETE',
  })
  await runtimeJson(res)
}

export async function readDir(data: { volume: string; path: string }): Promise<DirEntry[]> {
  const params = new URLSearchParams({ path: data.path })
  const res = await runtimeFetch(`/v/${encodeURIComponent(data.volume)}/readdir?${params}`)
  return runtimeJson<DirEntry[]>(res)
}
