use crate::cache_repo::{BlockRef, CacheRepo, EvictionBlockRef};
pub use crate::cache_repo::{CHUNK_SIZE, RecoveryWork, ZeroOpKind};
use anyhow::{Context, Result};
use lru::LruCache;
use metrics::{counter, gauge};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

pub trait BlockLocker: Send + Sync {
    fn block_lock(&self, block_idx: u64) -> Arc<AsyncMutex<()>>;
}

#[derive(Clone)]
struct LruEntry {
    locker: Arc<dyn BlockLocker>,
    size: u64,
}

pub struct SqliteCache {
    dir: PathBuf,
    repo: CacheRepo,
    volume_ids: dashmap::DashMap<String, i64>,
    lru: Mutex<LruCache<(i64, u64), LruEntry>>,
    used_bytes: AtomicU64,
    max_bytes: u64,
}

static GLOBAL: OnceLock<SqliteCache> = OnceLock::new();

pub async fn init(dir: PathBuf, max_bytes: u64) -> Result<()> {
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating cache root {}", dir.display()))?;
    let dir = dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", dir.display()))?;
    let db_path = dir.join("cache.db");
    let repo = CacheRepo::open(&db_path).await?;
    let cache = SqliteCache {
        dir,
        repo,
        volume_ids: dashmap::DashMap::new(),
        lru: Mutex::new(LruCache::new(NonZeroUsize::new(1_000_000).unwrap())),
        used_bytes: AtomicU64::new(0),
        max_bytes,
    };
    gauge!("cache.max_bytes").set(max_bytes as f64);

    GLOBAL
        .set(cache)
        .map_err(|_| anyhow::anyhow!("SqliteCache already initialized"))
}

pub fn get() -> &'static SqliteCache {
    GLOBAL
        .get()
        .expect("SqliteCache not initialized; call cache::init first")
}

impl SqliteCache {
    pub async fn init_store(
        &self,
        store_id: &str,
        parent_store_id: Option<&str>,
        locker: Arc<dyn BlockLocker>,
    ) -> Result<()> {
        let volume_id = self.repo.init_store(store_id, parent_store_id).await?;
        self.volume_ids.insert(store_id.to_string(), volume_id);
        self.populate_lru_from_db(volume_id, locker).await
    }

    pub async fn read(
        &self,
        store_id: &str,
        block_idx: u64,
        offset_within_block: u64,
        len: usize,
    ) -> Option<Result<Vec<u8>>> {
        if len == 0 {
            return Some(Ok(Vec::new()));
        }

        let r = async {
            let volume_id = self.volume_id(store_id).await?;
            let Some(BlockRef {
                block_id,
                downloaded_thru,
            }) = self.repo.lookup_block(volume_id, block_idx).await?
            else {
                counter!("cache.miss").increment(1);
                return Ok(None);
            };

            let first_chunk = offset_within_block / CHUNK_SIZE;
            let last_chunk = (offset_within_block + len as u64 - 1) / CHUNK_SIZE;
            if let Some(limit) = downloaded_thru
                && last_chunk > limit as u64
            {
                counter!("cache.miss").increment(1);
                return Ok(None);
            }

            let chunks = self
                .repo
                .read_chunks_in_range(block_id, first_chunk, last_chunk)
                .await?;

            let mut out = vec![0u8; len];
            for (chunk_idx, data) in chunks {
                let chunk_start = chunk_idx * CHUNK_SIZE;
                let req_start = offset_within_block;
                let req_end = offset_within_block + len as u64;
                let chunk_end = chunk_start + CHUNK_SIZE;
                let copy_start = req_start.max(chunk_start);
                let copy_end = req_end.min(chunk_end);
                if copy_start >= copy_end {
                    continue;
                }

                let src_off = (copy_start - chunk_start) as usize;
                let dst_off = (copy_start - req_start) as usize;
                let copy_len = (copy_end - copy_start) as usize;
                if src_off >= data.len() {
                    continue;
                }
                let src_end = (src_off + copy_len).min(data.len());
                let actual = src_end - src_off;
                out[dst_off..dst_off + actual].copy_from_slice(&data[src_off..src_end]);
            }

            counter!("cache.hit").increment(1);
            self.lru_promote(volume_id, block_idx);
            Ok(Some(out))
        };

        match r.await {
            Ok(Some(v)) => Some(Ok(v)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    pub async fn pwrite(
        &self,
        store_id: &str,
        block_idx: u64,
        offset_within_block: u64,
        data: &[u8],
    ) -> Result<()> {
        counter!("cache.pwrite.total").increment(1);
        let volume_id = self.volume_id(store_id).await?;
        let block_id = self.repo.ensure_block_row(volume_id, block_idx).await?;

        let start_chunk = offset_within_block / CHUNK_SIZE;
        let end_chunk = (offset_within_block + data.len() as u64 - 1) / CHUNK_SIZE;

        for chunk_idx in start_chunk..=end_chunk {
            let mut chunk = self
                .repo
                .load_chunk(block_id, chunk_idx)
                .await?
                .unwrap_or_else(|| vec![0u8; CHUNK_SIZE as usize]);
            if chunk.len() < CHUNK_SIZE as usize {
                chunk.resize(CHUNK_SIZE as usize, 0);
            }

            let chunk_start = chunk_idx * CHUNK_SIZE;
            let write_start = offset_within_block.max(chunk_start);
            let write_end = (offset_within_block + data.len() as u64).min(chunk_start + CHUNK_SIZE);
            let src_start = (write_start - offset_within_block) as usize;
            let dst_start = (write_start - chunk_start) as usize;
            let write_len = (write_end - write_start) as usize;
            chunk[dst_start..dst_start + write_len]
                .copy_from_slice(&data[src_start..src_start + write_len]);

            if chunk.iter().all(|b| *b == 0) {
                self.repo.delete_chunk(block_id, chunk_idx).await?;
            } else {
                self.repo.upsert_chunk(block_id, chunk_idx, &chunk).await?;
            }
        }

        let chunk_count = self.repo.chunk_count(block_id).await?;
        let size = chunk_count * CHUNK_SIZE;
        self.repo
            .set_block_size_and_downloaded(block_id, size, None)
            .await?;

        self.lru_insert(volume_id, block_idx, Arc::new(NoopLocker), size);
        gauge!("cache.used_bytes").set(self.used_bytes.load(Ordering::Relaxed) as f64);
        Ok(())
    }

    pub async fn insert_from_stream(
        &self,
        store_id: &str,
        block_idx: u64,
        locker: Arc<dyn BlockLocker>,
        mut reader: impl AsyncRead + Unpin,
    ) -> Result<u64> {
        let volume_id = self.volume_id(store_id).await?;
        let block_id = self.repo.ensure_block_row(volume_id, block_idx).await?;
        self.repo.delete_all_chunks(block_id).await?;
        self.repo.reset_stream_download_state(block_id).await?;

        let mut total = 0u64;
        let mut chunk_idx = 0u64;
        let mut cached_bytes = 0u64;
        let mut read_buf = vec![0u8; CHUNK_SIZE as usize];
        let mut pending = Vec::with_capacity(CHUNK_SIZE as usize * 2);
        loop {
            let n = reader
                .read(&mut read_buf)
                .await
                .context("reading stream chunk")?;
            if n == 0 {
                break;
            }
            total += n as u64;
            pending.extend_from_slice(&read_buf[..n]);

            while pending.len() >= CHUNK_SIZE as usize {
                let chunk = &pending[..CHUNK_SIZE as usize];
                if chunk.iter().any(|b| *b != 0) {
                    self.repo.upsert_chunk(block_id, chunk_idx, chunk).await?;
                    cached_bytes += CHUNK_SIZE;
                }
                self.repo.set_downloaded_thru(block_id, chunk_idx).await?;
                chunk_idx += 1;
                pending.drain(..CHUNK_SIZE as usize);
            }
        }

        if !pending.is_empty() {
            if pending.iter().any(|b| *b != 0) {
                self.repo.upsert_chunk(block_id, chunk_idx, &pending).await?;
                cached_bytes += pending.len() as u64;
            }
            self.repo.set_downloaded_thru(block_id, chunk_idx).await?;
        }

        self.repo
            .set_block_size_and_downloaded(block_id, cached_bytes, None)
            .await?;

        counter!("cache.insert.total").increment(1);
        counter!("cache.insert.bytes").increment(total);
        self.lru_insert(volume_id, block_idx, locker, cached_bytes);
        self.evict_if_needed().await?;
        Ok(total)
    }

    pub async fn ensure_sparse_block(
        &self,
        store_id: &str,
        block_idx: u64,
        _block_size: u64,
        locker: Arc<dyn BlockLocker>,
    ) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        let _ = self.repo.ensure_block_row(volume_id, block_idx).await?;
        self.lru_insert(volume_id, block_idx, locker, 0);
        self.evict_if_needed().await
    }

    pub async fn set_block_len(
        &self,
        store_id: &str,
        block_idx: u64,
        new_len: u64,
        locker: Arc<dyn BlockLocker>,
    ) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        let Some(block) = self.repo.lookup_block(volume_id, block_idx).await? else {
            return Ok(());
        };
        let block_id = block.block_id;

        if new_len == 0 {
            self.repo.delete_all_chunks(block_id).await?;
        } else {
            let full_chunks = new_len / CHUNK_SIZE;
            let rem = new_len % CHUNK_SIZE;
            self.repo.delete_chunks_after(block_id, full_chunks).await?;
            if rem == 0 {
                self.repo.delete_chunk(block_id, full_chunks).await?;
            } else if let Some(mut last) = self.repo.load_chunk(block_id, full_chunks).await?
                && last.len() > rem as usize
            {
                last.truncate(rem as usize);
                if last.iter().all(|b| *b == 0) {
                    self.repo.delete_chunk(block_id, full_chunks).await?;
                } else {
                    self.repo.upsert_chunk(block_id, full_chunks, &last).await?;
                }
            }
        }

        let chunk_count = self.repo.chunk_count(block_id).await?;
        let size = chunk_count * CHUNK_SIZE;
        self.repo
            .set_block_size_and_downloaded(block_id, size, block.downloaded_thru)
            .await?;
        self.lru_insert(volume_id, block_idx, locker, size);
        Ok(())
    }

    pub async fn has_block(&self, store_id: &str, block_idx: u64) -> Result<bool> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.has_block(volume_id, block_idx).await
    }

    pub async fn try_mark_dirty(&self, store_id: &str, block_idx: u64) -> Result<bool> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.try_mark_dirty(volume_id, block_idx).await
    }

    pub async fn prepare_upload(
        &self,
        store_id: &str,
        block_idx: u64,
        block_size: u64,
    ) -> Result<PathBuf> {
        let volume_id = self.volume_id(store_id).await?;
        let block_id = self.repo.begin_upload(volume_id, block_idx).await?;

        let path = self.dir.join(format!(
            "upload-{store_id}-{block_idx:016x}-{}.bin",
            std::process::id()
        ));
        let mut f = tokio::fs::File::create(&path)
            .await
            .with_context(|| format!("creating upload temp file {}", path.display()))?;

        let chunks = self.repo.read_all_chunks(block_id).await?;
        let total_chunks = if block_size == 0 {
            0
        } else {
            (block_size - 1) / CHUNK_SIZE + 1
        };
        let zero_chunk = vec![0u8; CHUNK_SIZE as usize];
        let mut expected_idx = 0u64;

        for (chunk_idx, data) in chunks {
            while expected_idx < chunk_idx && expected_idx < total_chunks {
                let n = chunk_write_len(block_size, expected_idx) as usize;
                f.write_all(&zero_chunk[..n]).await?;
                expected_idx += 1;
            }
            if expected_idx >= total_chunks {
                break;
            }
            let n = chunk_write_len(block_size, expected_idx) as usize;
            let used = n.min(data.len());
            if used > 0 {
                f.write_all(&data[..used]).await?;
            }
            if n > used {
                f.write_all(&zero_chunk[..n - used]).await?;
            }
            expected_idx += 1;
        }

        while expected_idx < total_chunks {
            let n = chunk_write_len(block_size, expected_idx) as usize;
            f.write_all(&zero_chunk[..n]).await?;
            expected_idx += 1;
        }

        f.flush().await?;
        f.sync_data().await?;
        Ok(path)
    }

    pub async fn mark_dirty(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.mark_dirty(volume_id, block_idx).await
    }

    pub async fn clear_dirty(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.clear_dirty(volume_id, block_idx).await
    }

    pub async fn clear_uploading(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.clear_uploading(volume_id, block_idx).await
    }

    pub async fn is_dirty(&self, store_id: &str, block_idx: u64) -> Result<bool> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.is_dirty(volume_id, block_idx).await
    }

    pub async fn remove_block(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        if self.repo.remove_block(volume_id, block_idx).await? {
            self.lru_remove(volume_id, block_idx);
        }
        Ok(())
    }

    pub async fn queue_zero_op(
        &self,
        store_id: &str,
        block_idx: u64,
        kind: ZeroOpKind,
    ) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.queue_zero_op(volume_id, block_idx, kind).await
    }

    pub async fn clear_zero_op(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.clear_zero_op(volume_id, block_idx).await
    }

    pub async fn complete_zero_op(&self, store_id: &str, block_idx: u64) -> Result<()> {
        self.clear_zero_op(store_id, block_idx).await
    }

    pub async fn recover(&self, store_id: &str) -> Result<RecoveryWork> {
        let volume_id = self.volume_id(store_id).await?;
        let recovery = self.repo.recover(volume_id).await?;
        counter!("cache.recover.dirty_blocks").increment(recovery.dirty_blocks.len() as u64);
        Ok(recovery)
    }

    pub async fn evict_if_needed(&self) -> Result<()> {
        if self.max_bytes == 0 {
            return Ok(());
        }
        if self.used_bytes.load(Ordering::Relaxed) <= self.max_bytes {
            return Ok(());
        }
        let target = (self.max_bytes as f64 * 0.9) as u64;
        let candidates: Vec<((i64, u64), LruEntry)> = {
            let lru = self.lru.lock().unwrap();
            lru.iter().rev().map(|(k, v)| (*k, v.clone())).collect()
        };

        for ((volume_id, block_idx), entry) in candidates {
            if self.used_bytes.load(Ordering::Relaxed) <= target {
                break;
            }
            let lock = entry.locker.block_lock(block_idx);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };

            let Some(EvictionBlockRef {
                block_id,
                dirty,
                uploading,
            }) = self
                .repo
                .lookup_block_for_eviction(volume_id, block_idx)
                .await?
            else {
                self.lru_remove(volume_id, block_idx);
                continue;
            };
            if dirty || uploading {
                continue;
            }

            self.repo.delete_block_by_id(block_id).await?;
            counter!("cache.evict.total").increment(1);
            counter!("cache.evict.bytes").increment(entry.size);
            self.lru_remove(volume_id, block_idx);
            gauge!("cache.used_bytes").set(self.used_bytes.load(Ordering::Relaxed) as f64);
            debug!(volume_id, block = block_idx, "evicted cache block");
        }
        Ok(())
    }

    async fn populate_lru_from_db(
        &self,
        volume_id: i64,
        locker: Arc<dyn BlockLocker>,
    ) -> Result<()> {
        for (block_idx, size) in self.repo.list_blocks_for_lru(volume_id).await? {
            self.lru_insert(volume_id, block_idx, Arc::clone(&locker), size);
        }
        Ok(())
    }

    async fn volume_id(&self, store_id: &str) -> Result<i64> {
        if let Some(v) = self.volume_ids.get(store_id) {
            return Ok(*v);
        }
        let id = self.repo.lookup_volume_id(store_id).await?;
        self.volume_ids.insert(store_id.to_string(), id);
        Ok(id)
    }

    fn lru_promote(&self, volume_id: i64, block_idx: u64) {
        self.lru.lock().unwrap().promote(&(volume_id, block_idx));
    }

    fn lru_insert(&self, volume_id: i64, block_idx: u64, locker: Arc<dyn BlockLocker>, size: u64) {
        let mut lru = self.lru.lock().unwrap();
        match lru.put((volume_id, block_idx), LruEntry { locker, size }) {
            None => {
                self.used_bytes.fetch_add(size, Ordering::Relaxed);
            }
            Some(old) if old.size != size => {
                if size > old.size {
                    self.used_bytes
                        .fetch_add(size - old.size, Ordering::Relaxed);
                } else {
                    self.used_bytes
                        .fetch_sub(old.size - size, Ordering::Relaxed);
                }
            }
            Some(_) => {}
        }
    }

    fn lru_remove(&self, volume_id: i64, block_idx: u64) {
        if let Some(entry) = self.lru.lock().unwrap().pop(&(volume_id, block_idx)) {
            self.used_bytes.fetch_sub(entry.size, Ordering::Relaxed);
        }
    }
}

fn chunk_write_len(block_size: u64, chunk_idx: u64) -> u64 {
    let start = chunk_idx * CHUNK_SIZE;
    if start >= block_size {
        return 0;
    }
    (block_size - start).min(CHUNK_SIZE)
}

struct NoopLocker;

impl BlockLocker for NoopLocker {
    fn block_lock(&self, _block_idx: u64) -> Arc<AsyncMutex<()>> {
        Arc::new(AsyncMutex::new(()))
    }
}
