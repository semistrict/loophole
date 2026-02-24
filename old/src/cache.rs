use crate::cache_repo::{CacheRepo, EvictionBlockRef};
pub use crate::cache_repo::{RecoveryWork, ZeroOpKind};
use anyhow::{Context, Result};
use lru::LruCache;
use metrics::{counter, gauge};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use crate::store::BlockLockMap;

#[derive(Clone)]
struct LruEntry {
    locker: Arc<BlockLockMap>,
}

pub struct DiskCache {
    dir: PathBuf,
    repo: CacheRepo,
    volume_ids: dashmap::DashMap<String, i64>,
    /// Maps (volume_id, block_idx) → LRU entry. Size tracking uses block_size.
    lru: Mutex<LruCache<(i64, u64), LruEntry>>,
    used_bytes: AtomicU64,
    max_bytes: u64,
    block_size: AtomicU64,
}

static GLOBAL: OnceLock<DiskCache> = OnceLock::new();

pub async fn init(dir: PathBuf, max_bytes: u64) -> Result<()> {
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating cache root {}", dir.display()))?;
    let dir = dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", dir.display()))?;
    let db_path = dir.join("cache.db");
    let repo = CacheRepo::open(&db_path).await?;
    let cache = DiskCache {
        dir,
        repo,
        volume_ids: dashmap::DashMap::new(),
        lru: Mutex::new(LruCache::new(NonZeroUsize::new(1_000_000).unwrap())),
        used_bytes: AtomicU64::new(0),
        max_bytes,
        block_size: AtomicU64::new(0),
    };
    gauge!("cache.max_bytes").set(max_bytes as f64);

    let _ = GLOBAL.set(cache);
    Ok(())
}

pub fn get() -> &'static DiskCache {
    GLOBAL
        .get()
        .expect("DiskCache not initialized; call cache::init first")
}

impl DiskCache {
    /// Returns the path for a block data file.
    pub fn block_path(&self, store_id: &str, block_idx: u64) -> PathBuf {
        self.dir.join(store_id).join(format!("{block_idx:016x}"))
    }

    /// Returns the cache root directory (for temp file creation).
    pub fn cache_dir(&self) -> &std::path::Path {
        &self.dir
    }

    pub async fn init_store(
        &self,
        store_id: &str,
        parent_store_id: Option<&str>,
        block_size: u64,
        locker: Arc<BlockLockMap>,
    ) -> Result<()> {
        let volume_id = self.repo.init_store(store_id, parent_store_id).await?;
        self.volume_ids.insert(store_id.to_string(), volume_id);

        // Store the block_size so LRU can use it.
        let prev = self.block_size.load(Ordering::Relaxed);
        if prev == 0 && block_size > 0 {
            self.block_size.store(block_size, Ordering::Relaxed);
        }

        // Ensure store subdirectory exists for block files.
        let store_dir = self.dir.join(store_id);
        std::fs::create_dir_all(&store_dir)
            .with_context(|| format!("creating cache store dir {}", store_dir.display()))?;

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

            // Check if block row exists and is populated.
            if !self.repo.is_populated(volume_id, block_idx).await? {
                counter!("cache.miss").increment(1);
                return Ok(None);
            }

            let path = self.block_path(store_id, block_idx);
            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    counter!("cache.miss").increment(1);
                    return Ok(None);
                }
                Err(e) => return Err(e).context("opening block file for read"),
            };

            let mut buf = vec![0u8; len];
            let n = file
                .read_at(&mut buf, offset_within_block)
                .context("pread block file")?;
            // If file is shorter than requested, the tail stays as zeros.
            let _ = n;

            counter!("cache.hit").increment(1);
            self.lru_promote(volume_id, block_idx);
            Ok(Some(buf))
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
        locker: Arc<BlockLockMap>,
    ) -> Result<()> {
        counter!("cache.pwrite.total").increment(1);
        let volume_id = self.volume_id(store_id).await?;
        let _ = self.repo.ensure_block_row(volume_id, block_idx).await?;

        let path = self.block_path(store_id, block_idx);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("opening block file for pwrite {}", path.display()))?;

        file.write_at(data, offset_within_block)
            .context("pwrite block file")?;
        file.sync_data().context("fdatasync block file")?;

        // Mark populated since we have data on disk now.
        self.repo.mark_populated(volume_id, block_idx).await?;

        let bs = self.block_size.load(Ordering::Relaxed);
        self.lru_insert(volume_id, block_idx, locker, bs);
        gauge!("cache.used_bytes").set(self.used_bytes.load(Ordering::Relaxed) as f64);
        Ok(())
    }

    pub async fn insert_from_stream(
        &self,
        store_id: &str,
        block_idx: u64,
        locker: Arc<BlockLockMap>,
        mut reader: impl AsyncRead + Unpin,
    ) -> Result<u64> {
        let volume_id = self.volume_id(store_id).await?;
        let _ = self.repo.ensure_block_row(volume_id, block_idx).await?;

        let path = self.block_path(store_id, block_idx);
        let mut file = tokio::fs::File::create(&path)
            .await
            .with_context(|| format!("creating block file {}", path.display()))?;

        let mut total = 0u64;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf).await.context("reading stream")?;
            if n == 0 {
                break;
            }
            total += n as u64;
            tokio::io::AsyncWriteExt::write_all(&mut file, &buf[..n])
                .await
                .context("writing block file")?;
        }

        tokio::io::AsyncWriteExt::flush(&mut file).await?;
        file.sync_data().await?;

        self.repo.mark_populated(volume_id, block_idx).await?;

        counter!("cache.insert.total").increment(1);
        counter!("cache.insert.bytes").increment(total);
        let bs = self.block_size.load(Ordering::Relaxed);
        self.lru_insert(volume_id, block_idx, locker, bs);
        self.evict_if_needed().await?;
        Ok(total)
    }

    pub async fn ensure_sparse_block(
        &self,
        store_id: &str,
        block_idx: u64,
        block_size: u64,
        locker: Arc<BlockLockMap>,
    ) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        let _ = self.repo.ensure_block_row(volume_id, block_idx).await?;

        // Create the file with block_size length (sparse).
        let path = self.block_path(store_id, block_idx);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("creating sparse block {}", path.display()))?;
        file.set_len(block_size)
            .context("setting sparse block length")?;

        self.repo.mark_populated(volume_id, block_idx).await?;
        self.lru_insert(volume_id, block_idx, locker, block_size);
        self.evict_if_needed().await
    }

    pub async fn set_block_len(
        &self,
        store_id: &str,
        block_idx: u64,
        new_len: u64,
        locker: Arc<BlockLockMap>,
    ) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        if !self.repo.has_block(volume_id, block_idx).await? {
            return Ok(());
        }

        let path = self.block_path(store_id, block_idx);
        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&path) {
            file.set_len(new_len).context("truncating block file")?;
        }

        let bs = self.block_size.load(Ordering::Relaxed);
        self.lru_insert(volume_id, block_idx, locker, bs);
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

    pub async fn begin_upload(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.begin_upload(volume_id, block_idx).await
    }

    pub async fn list_dirty_blocks(&self, store_id: &str) -> Result<Vec<u64>> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.list_dirty_blocks(volume_id).await
    }

    pub async fn is_dirty(&self, store_id: &str, block_idx: u64) -> Result<bool> {
        let volume_id = self.volume_id(store_id).await?;
        self.repo.is_dirty(volume_id, block_idx).await
    }

    pub async fn remove_block(&self, store_id: &str, block_idx: u64) -> Result<()> {
        let volume_id = self.volume_id(store_id).await?;
        if self.repo.remove_block(volume_id, block_idx).await? {
            // Delete the block file.
            let path = self.block_path(store_id, block_idx);
            let _ = std::fs::remove_file(&path);
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

        let bs = self.block_size.load(Ordering::Relaxed);
        for ((volume_id, block_idx), entry) in candidates {
            if self.used_bytes.load(Ordering::Relaxed) <= target {
                break;
            }
            let lock = entry.locker.lock(block_idx);
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

            // Find the store_id for this volume to delete the file.
            // We need to iterate volume_ids to find which store matches.
            let store_id_opt: Option<String> = self
                .volume_ids
                .iter()
                .find(|e| *e.value() == volume_id)
                .map(|e| e.key().clone());

            self.repo.delete_block_by_id(block_id).await?;
            if let Some(sid) = store_id_opt {
                let path = self.block_path(&sid, block_idx);
                let _ = std::fs::remove_file(&path);
            }

            counter!("cache.evict.total").increment(1);
            counter!("cache.evict.bytes").increment(bs);
            self.lru_remove(volume_id, block_idx);
            gauge!("cache.used_bytes").set(self.used_bytes.load(Ordering::Relaxed) as f64);
            debug!(volume_id, block = block_idx, "evicted cache block");
        }
        Ok(())
    }

    async fn populate_lru_from_db(&self, volume_id: i64, locker: Arc<BlockLockMap>) -> Result<()> {
        let bs = self.block_size.load(Ordering::Relaxed);
        for block_idx in self.repo.list_blocks_for_lru(volume_id).await? {
            self.lru_insert(volume_id, block_idx, Arc::clone(&locker), bs);
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

    fn lru_insert(&self, volume_id: i64, block_idx: u64, locker: Arc<BlockLockMap>, size: u64) {
        let mut lru = self.lru.lock().unwrap();
        if lru
            .put((volume_id, block_idx), LruEntry { locker })
            .is_none()
        {
            self.used_bytes.fetch_add(size, Ordering::Relaxed);
        }
    }

    fn lru_remove(&self, volume_id: i64, block_idx: u64) {
        let bs = self.block_size.load(Ordering::Relaxed);
        if self
            .lru
            .lock()
            .unwrap()
            .pop(&(volume_id, block_idx))
            .is_some()
        {
            self.used_bytes.fetch_sub(bs, Ordering::Relaxed);
        }
    }
}
