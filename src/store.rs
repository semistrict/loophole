use crate::cache;
use crate::cache::ZeroOpKind;
use crate::s3::{BlockIndex, block_key, list_blocks, normalize_prefix, read_state, write_state};
pub use crate::s3::{S3Access, State};
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use dashmap::DashSet;
use metrics::{counter, gauge};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc, watch};
use tracing::{debug, info, instrument, warn};

/// Per-block striped lock shared between the Store and the cache for eviction safety.
/// Uses a fixed array of 1024 mutexes; block index is mapped via modulo.
pub struct BlockLockMap {
    stripes: Box<[Mutex<()>; 1024]>,
}

impl BlockLockMap {
    fn new() -> Self {
        Self {
            stripes: Box::new(std::array::from_fn(|_| Mutex::new(()))),
        }
    }

    pub(crate) fn lock(&self, block_idx: u64) -> &Mutex<()> {
        &self.stripes[(block_idx as usize) % self.stripes.len()]
    }
}

// ---------------------------------------------------------------------------
// Trait: abstracts the read/write interface so fs.rs can be tested with mocks.
// ---------------------------------------------------------------------------

pub trait BlockStorage: Send + Sync {
    fn read_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: usize,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send + '_;
    fn write_block<'a>(
        &'a self,
        block_idx: u64,
        offset_within_block: u64,
        data: &'a [u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
    fn zero_block(
        &self,
        block_idx: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_;
    /// Atomically zero a range within a block (read-modify-write under lock).
    fn zero_range(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_;
}

impl<S: S3Access> BlockStorage for Store<S> {
    async fn read_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        self.do_read_block(block_idx, offset_within_block, len)
            .await
    }
    async fn write_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        data: &[u8],
    ) -> Result<()> {
        self.do_write_block(block_idx, offset_within_block, data)
            .await
    }
    async fn zero_block(&self, block_idx: u64) -> Result<()> {
        self.do_zero_block(block_idx).await
    }
    async fn zero_range(&self, block_idx: u64, offset_within_block: u64, len: u64) -> Result<()> {
        self.do_zero_range(block_idx, offset_within_block, len)
            .await
    }
}

pub struct Store<S: S3Access = Client> {
    pub(crate) s3: S,
    pub(crate) bucket: String,
    /// Resolved key prefix, always without a trailing slash.
    pub(crate) prefix: String,
    pub id: String,
    pub state: State,

    /// S3-existence index for this store's own prefix.
    /// Updated after each successful S3 upload so evicted cache files can be
    /// re-fetched from S3 rather than mistakenly treated as all-zeros.
    pub(crate) local_index: DashSet<u64>,
    /// Ancestor stores from direct parent to root, each with their S3 index.
    pub(crate) ancestors: Vec<(String, BlockIndex)>,

    /// Guards all writes to this store.
    /// Normal writes take read() (shared). Snapshot/clone takes write() (exclusive).
    write_lock: RwLock<()>,
    /// Per-block lock; shared with cache for eviction safety.
    pub(crate) block_locks: Arc<BlockLockMap>,
    /// Limits concurrent S3 uploads.
    pub(crate) upload_slots: Arc<Semaphore>,
    /// Limits concurrent background S3 downloads.
    download_slots: Arc<Semaphore>,
    /// Deduplicates in-flight background block fetches (keyed by block_idx).
    inflight_downloads: Arc<DashSet<u64>>,

    /// Blocks known to be explicitly zeroed (tombstones or punched holes).
    pub(crate) zero_blocks: DashSet<u64>,

    /// True once snapshot/clone has frozen this store.
    frozen: AtomicBool,

    /// Send block indices here to trigger background upload.
    pub(crate) upload_tx: mpsc::Sender<u64>,

    /// Blocks currently pending upload (written but not yet confirmed in S3).
    pub(crate) pending_uploads: DashSet<u64>,
    /// Incremented on each upload completion; flush() subscribes to wait.
    pub(crate) upload_epoch: watch::Sender<u64>,
}

impl<S: S3Access> Store<S> {
    fn validate_store_id(store_id: &str) -> Result<()> {
        anyhow::ensure!(!store_id.is_empty(), "store ID must not be empty");
        anyhow::ensure!(
            !store_id.contains('/') && !store_id.contains('\\'),
            "store ID must not contain path separators: {store_id:?}"
        );
        anyhow::ensure!(
            store_id != "." && store_id != "..",
            "store ID must not be '.' or '..': {store_id:?}"
        );
        Ok(())
    }

    /// Create a brand-new store by writing its initial state.json to S3.
    pub async fn format(
        s3: &S,
        bucket: &str,
        prefix: &str,
        store_id: &str,
        block_size: u64,
        volume_size: u64,
    ) -> Result<()> {
        Self::validate_store_id(store_id)?;
        let prefix = normalize_prefix(prefix);
        let state = State {
            parent_id: None,
            block_size,
            volume_size,
            children: vec![],
        };
        write_state(s3, bucket, &prefix, store_id, &state).await?;
        info!(
            store = %store_id,
            block_size,
            volume_size,
            "store formatted"
        );
        Ok(())
    }

    #[instrument(skip(s3), fields(store = %store_id))]
    pub async fn load(
        s3: S,
        bucket: String,
        prefix: String,
        store_id: String,
        max_uploads: usize,
        max_downloads: usize,
    ) -> Result<Arc<Self>> {
        let _timing = crate::metrics::timing!("store.load");
        Self::validate_store_id(&store_id)?;
        let prefix = normalize_prefix(&prefix);

        let state = read_state(&s3, &bucket, &prefix, &store_id).await?;
        info!(parent = ?state.parent_id, frozen = !state.children.is_empty(), "loaded state");

        let (index_set, local_tombstones) = list_blocks(&s3, &bucket, &prefix, &store_id).await?;
        debug!(
            blocks = index_set.len(),
            tombstones = local_tombstones.len(),
            "indexed local blocks"
        );
        let local_index: DashSet<u64> = index_set.into_iter().collect();
        let zero_blocks: DashSet<u64> = local_tombstones.into_iter().collect();

        // Walk ancestors from direct parent to root.
        let mut ancestors: Vec<(String, BlockIndex)> = Vec::new();
        let mut current_parent = state.parent_id.clone();
        while let Some(pid) = current_parent {
            let pstate = read_state(&s3, &bucket, &prefix, &pid).await?;
            let (mut pindex, ptombstones) = list_blocks(&s3, &bucket, &prefix, &pid).await?;
            // Ancestor tombstones mean those blocks are zero in the ancestor —
            // they don't provide real data, so exclude them from the index.
            for t in &ptombstones {
                pindex.remove(t);
            }
            debug!(store = %pid, blocks = pindex.len(), tombstones = ptombstones.len(), "indexed ancestor");
            current_parent = pstate.parent_id.clone();
            ancestors.push((pid, pindex));
        }

        let block_locks = Arc::new(BlockLockMap::new());

        // Init cache subdirs BEFORE recover, since recover reads the directory.
        cache::get()
            .init_store(
                &store_id,
                state.parent_id.as_deref(),
                Arc::clone(&block_locks),
            )
            .await?;
        for (ancestor_id, _) in &ancestors {
            cache::get()
                .init_store(
                    ancestor_id,
                    None,
                    Arc::clone(&block_locks),
                )
                .await?;
        }

        // Recover dirty/uploading blocks left over from a previous run.
        let recovery = cache::get().recover(&store_id).await?;
        for op in &recovery.pending_zero_ops {
            let _ = op.kind;
            zero_blocks.insert(op.block_idx);
        }
        info!(
            dirty = recovery.dirty_blocks.len(),
            zero_ops = recovery.pending_zero_ops.len(),
            "recovered pending operations"
        );
        counter!("store.load.recovered_dirty_blocks").increment(recovery.dirty_blocks.len() as u64);
        gauge!("store.load.indexed_blocks").set(local_index.len() as f64);

        let upload_slots = Arc::new(Semaphore::new(max_uploads));
        let download_slots = Arc::new(Semaphore::new(max_downloads));
        let inflight_downloads = Arc::new(DashSet::new());
        let (upload_tx, upload_rx) = mpsc::channel::<u64>(1024);
        let pending_uploads: DashSet<u64> = DashSet::new();
        let (upload_epoch, _) = watch::channel(0u64);

        let frozen = AtomicBool::new(!state.children.is_empty());

        let store = Arc::new(Self {
            s3,
            bucket,
            prefix,
            id: store_id,
            state,
            local_index,
            ancestors,
            zero_blocks,
            write_lock: RwLock::new(()),
            block_locks,
            upload_slots,
            download_slots,
            inflight_downloads,
            frozen,
            upload_tx,
            pending_uploads,
            upload_epoch,
        });

        // Spawn the uploader BEFORE sending recovered blocks so the consumer
        // is ready; using .send().await avoids silently dropping blocks if
        // the channel would otherwise be full.
        store.spawn_uploader(upload_rx);

        let mut to_requeue: HashSet<u64> = HashSet::new();
        for idx in recovery.dirty_blocks {
            to_requeue.insert(idx);
        }
        for op in recovery.pending_zero_ops {
            to_requeue.insert(op.block_idx);
        }
        for idx in to_requeue {
            store.pending_uploads.insert(idx);
            store
                .upload_tx
                .send(idx)
                .await
                .context("queuing recovered dirty block")?;
        }

        Ok(store)
    }

    fn spawn_uploader(self: &Arc<Self>, rx: mpsc::Receiver<u64>) {
        crate::uploader::spawn(Arc::clone(self), rx);
    }

    fn block_lock(&self, block_idx: u64) -> &Mutex<()> {
        self.block_locks.lock(block_idx)
    }

    /// Find which store in the chain owns this block in S3, or None if absent.
    fn find_s3_owner(&self, block_idx: u64) -> Option<&str> {
        if self.local_index.contains(&block_idx) {
            return Some(&self.id);
        }
        for (id, index) in self.ancestors.iter() {
            if index.contains(&block_idx) {
                return Some(id);
            }
        }
        None
    }

    /// Mark a block as explicitly zeroed.
    ///
    /// Updates local state immediately (zero_blocks, cache cleanup) and enqueues
    /// the S3 tombstone/delete to the upload queue so the FUSE thread is not
    /// blocked on network I/O.
    async fn do_zero_block(&self, block_idx: u64) -> Result<()> {
        counter!("store.zero_block.total").increment(1);
        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is frozen, cannot write"
        );

        let block_lock = self.block_lock(block_idx);
        let _block_guard = block_lock.lock().await;

        self.zero_blocks.insert(block_idx);
        gauge!("store.zero_blocks").set(self.zero_blocks.len() as f64);

        cache::get().remove_block(&self.id, block_idx).await?;
        let ancestor_has_block = self
            .ancestors
            .iter()
            .any(|(_, idx)| idx.contains(&block_idx));
        let zero_kind = if ancestor_has_block {
            ZeroOpKind::Tombstone
        } else {
            ZeroOpKind::Delete
        };
        cache::get()
            .queue_zero_op(&self.id, block_idx, zero_kind)
            .await?;

        // Enqueue to the uploader — it will check zero_blocks at processing
        // time and perform the appropriate S3 operation (tombstone or delete).
        self.pending_uploads.insert(block_idx);
        self.upload_tx
            .send(block_idx)
            .await
            .context("upload_tx send failed")?;

        Ok(())
    }

    /// Atomically zero a byte range within a single block (read-modify-write
    /// under the block lock so concurrent writes cannot be lost).
    async fn do_zero_range(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: u64,
    ) -> Result<()> {
        counter!("store.zero_range.total").increment(1);
        let block_size = self.state.block_size;

        // Full block → delegate to the tombstone path.
        if offset_within_block == 0 && len == block_size {
            return self.do_zero_block(block_idx).await;
        }

        // Read the full block, zero the range, write back as a full-block write.
        // do_write_block acquires the block lock internally, and between read and
        // write another write could slip in. To prevent that, we hold the block
        // lock across both operations.
        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is frozen, cannot write"
        );

        let block_lock = self.block_lock(block_idx);
        let _block_guard = block_lock.lock().await;

        // Read the full block under lock.
        let mut data = if self.zero_blocks.contains(&block_idx) {
            vec![0u8; block_size as usize]
        } else {
            // do_read_block does not acquire the block lock, so no deadlock.
            self.do_read_block(block_idx, 0, block_size as usize)
                .await?
        };

        // Zero the requested range.
        let start = offset_within_block as usize;
        let end = start + len as usize;
        data[start..end].fill(0);

        // Write back the full block. We already hold write_lock + block_lock,
        // so call the inner write logic directly to avoid re-acquiring them.
        if self.zero_blocks.remove(&block_idx).is_some() {
            cache::get().clear_zero_op(&self.id, block_idx).await?;
        }
        self.do_write_block_locked(block_idx, &data).await
    }

    /// Read exactly `len` bytes at `offset_within_block` from `block_idx`.
    /// Returns zeros for blocks that don't exist anywhere in the chain.
    async fn do_read_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let _timing = crate::metrics::timing!("store.read");
        counter!("store.read.bytes").increment(len as u64);

        // Explicitly zeroed blocks return zeros immediately — must be checked
        // before any cache lookups because ancestor caches may still hold data.
        if self.zero_blocks.contains(&block_idx) {
            debug!(block = block_idx, "zero block, returning zeros");
            counter!("store.read.zeros").increment(1);
            return Ok(vec![0u8; len]);
        }

        // Check local store cache first (handles dirty/in-flight blocks).
        if let Some(result) = cache::get()
            .read(&self.id, block_idx, offset_within_block, len)
            .await
        {
            counter!("store.read.cache_hit").increment(1);
            return result.context("cache read failed");
        }

        // Check ancestor caches before going to S3.
        for (ancestor_id, ancestor_index) in self.ancestors.iter() {
            if ancestor_index.contains(&block_idx) {
                if let Some(result) = cache::get()
                    .read(ancestor_id, block_idx, offset_within_block, len)
                    .await
                {
                    counter!("store.read.cache_hit").increment(1);
                    return result.context("ancestor cache read failed");
                }
                // This ancestor owns the block; stop checking further ancestors.
                break;
            }
        }

        let Some(owner_id) = self.find_s3_owner(block_idx) else {
            debug!(block = block_idx, "block absent, returning zeros");
            counter!("store.read.zeros").increment(1);
            return Ok(vec![0u8; len]);
        };

        counter!("store.read.cache_miss").increment(1);
        counter!("store.read.s3_fetch").increment(1);

        let key = block_key(&self.prefix, owner_id, block_idx);
        debug!(
            block = block_idx,
            owner = owner_id,
            %key,
            offset = offset_within_block,
            len,
            "range read from S3"
        );

        // Range GET — only the exact bytes needed for this request.
        let bytes = self
            .s3
            .get_byte_range(&self.bucket, &key, offset_within_block, len)
            .await
            .context("get_byte_range failed")?;

        // Spawn a background full-block fetch to warm the disk cache so future
        // reads avoid S3. Works for both local-store blocks and ancestor blocks.
        if self.inflight_downloads.insert(block_idx) {
            gauge!("store.inflight_downloads").set(self.inflight_downloads.len() as f64);
            let download_slots = Arc::clone(&self.download_slots);
            if let Ok(_permit) = download_slots.try_acquire_owned() {
                let s3 = self.s3.clone();
                let bucket = self.bucket.clone();
                let key_clone = key.clone();
                let inflight = Arc::clone(&self.inflight_downloads);
                let locker = Arc::clone(&self.block_locks);
                let owner_id_owned = owner_id.to_string();
                let block_size = self.state.block_size;

                tokio::spawn(async move {
                    let _guard = locker.lock(block_idx).lock().await;

                    match s3.get_body(&bucket, &key_clone).await {
                        Ok(body) => match cache::get()
                            .insert_from_stream(
                                &owner_id_owned,
                                block_idx,
                                Arc::clone(&locker),
                                body.into_async_read().take(block_size),
                            )
                            .await
                        {
                            Ok(written) => {
                                if written < block_size
                                    && let Err(e) = cache::get()
                                        .set_block_len(
                                            &owner_id_owned,
                                            block_idx,
                                            block_size,
                                            Arc::clone(&locker),
                                        )
                                        .await
                                {
                                    warn!(block = block_idx, error = %e, "set_block_len failed");
                                }
                                debug!(block = block_idx, owner = %owner_id_owned, "background fetch complete");
                            }
                            Err(e) => {
                                warn!(block = block_idx, error = %e, "insert_from_stream failed");
                            }
                        },
                        Err(e) => {
                            warn!(block = block_idx, error = %e, "background fetch failed");
                        }
                    }

                    inflight.remove(&block_idx);
                    gauge!("store.inflight_downloads").set(inflight.len() as f64);
                    drop(_permit);
                });
            } else {
                // We inserted as inflight before trying to acquire a slot.
                // If no slot is available, roll back so a future read can retry.
                self.inflight_downloads.remove(&block_idx);
            }
        }

        Ok(bytes)
    }

    /// Write a full block to cache and enqueue upload.
    /// Caller MUST already hold write_lock (read) and the block lock.
    async fn do_write_block_locked(&self, block_idx: u64, data: &[u8]) -> Result<()> {
        let block_size = self.state.block_size;
        debug_assert_eq!(data.len() as u64, block_size);

        // Ensure cache file exists (create or truncate to block_size).
        if !cache::get().has_block(&self.id, block_idx).await? {
            cache::get()
                .ensure_sparse_block(
                    &self.id,
                    block_idx,
                    block_size,
                    Arc::clone(&self.block_locks),
                )
                .await?;
        }

        let dirty_is_new = cache::get().try_mark_dirty(&self.id, block_idx).await?;
        cache::get()
            .pwrite(
                &self.id,
                block_idx,
                0,
                data,
                Arc::clone(&self.block_locks),
            )
            .await?;

        self.pending_uploads.insert(block_idx);
        if dirty_is_new {
            self.upload_tx
                .send(block_idx)
                .await
                .context("upload_tx send failed")?;
        }

        Ok(())
    }

    /// Write `data` at `offset_within_block` within `block_idx`.
    /// Writes to disk cache first (write-back), then enqueues async S3 upload.
    ///
    /// If the write is a full-block zero write, it is converted to a
    /// `do_zero_block` call instead of a normal upload.
    async fn do_write_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        data: &[u8],
    ) -> Result<()> {
        let _timing = crate::metrics::timing!("store.write");
        counter!("store.write.bytes").increment(data.len() as u64);
        let block_size = self.state.block_size;

        // Detect full-block zero writes upfront so we can short-circuit.
        let is_full_block_zero = offset_within_block == 0
            && data.len() as u64 == block_size
            && data.iter().all(|&b| b == 0);

        if is_full_block_zero {
            return self.do_zero_block(block_idx).await;
        }

        // Shared write lock — blocks if snapshot/clone is in progress.
        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is frozen, cannot write"
        );

        let block_lock = self.block_lock(block_idx);
        let _block_guard = block_lock.lock().await;

        // If this block was previously zeroed, clear that status now.
        // Remember whether it was zeroed so we don't try to fetch stale S3
        // data — the uploader may not have processed the zero yet, so the
        // S3 object could still contain the old (pre-zero) data.
        let was_zeroed = self.zero_blocks.remove(&block_idx).is_some();
        if was_zeroed {
            cache::get().clear_zero_op(&self.id, block_idx).await?;
        }

        // Determine the S3 key to fetch from if the cache file is absent.
        let has_cache = cache::get().has_block(&self.id, block_idx).await?;
        let fetch_key: Option<String> = if has_cache {
            None
        } else if was_zeroed {
            // Block was explicitly zeroed — treat as brand-new regardless of
            // local_index state (the uploader may not have deleted the S3
            // object yet, so fetching would return stale pre-zero data).
            None
        } else if self.local_index.contains(&block_idx) {
            // Block was previously written + uploaded; cache file was evicted.
            // Re-fetch from our own S3 prefix (not from an ancestor).
            Some(block_key(&self.prefix, &self.id, block_idx))
        } else {
            // New block, or block lives only in an ancestor.
            self.ancestors
                .iter()
                .find(|(_, idx)| idx.contains(&block_idx))
                .map(|(id, _)| block_key(&self.prefix, id, block_idx))
        };

        if has_cache {
            // Already cached locally.
        } else if let Some(key) = fetch_key {
            // Stream the existing block from S3 directly into cache, then pad with
            // sparse zeros to the fixed block size if the object is short.
            let body = self
                .s3
                .get_body(&self.bucket, &key)
                .await
                .with_context(|| format!("fetching block {key}"))?;
            let written = cache::get()
                .insert_from_stream(
                    &self.id,
                    block_idx,
                    Arc::clone(&self.block_locks),
                    body.into_async_read().take(block_size),
                )
                .await?;
            if written < block_size {
                cache::get()
                    .set_block_len(
                        &self.id,
                        block_idx,
                        block_size,
                        Arc::clone(&self.block_locks),
                    )
                    .await?;
            }
        } else {
            // Brand-new block: create a sparse zeroed file.
            cache::get()
                .ensure_sparse_block(
                    &self.id,
                    block_idx,
                    block_size,
                    Arc::clone(&self.block_locks),
                )
                .await?;
        }

        let dirty_is_new = cache::get().try_mark_dirty(&self.id, block_idx).await?;
        cache::get()
            .pwrite(
                &self.id,
                block_idx,
                offset_within_block,
                data,
                Arc::clone(&self.block_locks),
            )
            .await?;

        // Mark as pending and enqueue BEFORE releasing the block lock so the
        // uploader cannot complete and remove from pending_uploads in the gap.
        self.pending_uploads.insert(block_idx);
        gauge!("store.pending_uploads").set(self.pending_uploads.len() as f64);
        if dirty_is_new {
            self.upload_tx
                .send(block_idx)
                .await
                .context("upload_tx send failed")?;
        }

        drop(_block_guard);

        // write_lock.read() released here.
        Ok(())
    }

    /// Wait until all blocks that are currently pending upload have been
    /// confirmed in S3.  Blocks dirtied *after* this call starts are not
    /// waited on — only the snapshot taken at entry matters.
    pub async fn flush(&self) -> Result<()> {
        let _timing = crate::metrics::timing!("store.flush");
        let snapshot: Vec<u64> = self.pending_uploads.iter().map(|r| *r).collect();
        gauge!("store.flush.pending_blocks").set(snapshot.len() as f64);
        if snapshot.is_empty() {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let mut rx = self.upload_epoch.subscribe();

        loop {
            if snapshot
                .iter()
                .all(|idx| !self.pending_uploads.contains(idx))
            {
                return Ok(());
            }
            tokio::select! {
                result = rx.changed() => {
                    if result.is_err() {
                        anyhow::bail!("upload channel closed during flush");
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let remaining: Vec<u64> = snapshot.iter()
                        .filter(|idx| self.pending_uploads.contains(idx))
                        .copied()
                        .collect();
                    anyhow::bail!(
                        "flush timed out after 5 minutes — {} blocks still pending: {:?}",
                        remaining.len(),
                        &remaining[..remaining.len().min(10)]
                    );
                }
            }
        }
    }

    /// Freeze this store and register a new child store for continued writes.
    /// After this call the current store is immutable; mount a new Store with
    /// `new_store_id` to continue writing.
    pub async fn snapshot(&self, new_store_id: String) -> Result<()> {
        let _timing = crate::metrics::timing!("store.snapshot");
        Self::validate_store_id(&new_store_id)?;
        // Exclusive write lock — drains all in-flight writes before freezing.
        let _write_guard = self.write_lock.write().await;

        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is already frozen, cannot snapshot"
        );

        self.flush().await.context("flush before snapshot failed")?;

        // Write child state FIRST. If we crash before updating the parent, the
        // child is an orphan but no data is lost. If we crash after updating the
        // parent, recovery can detect the missing child via the children list.
        let child_state = State {
            parent_id: Some(self.id.clone()),
            block_size: self.state.block_size,
            volume_size: self.state.volume_size,
            children: vec![],
        };
        write_state(
            &self.s3,
            &self.bucket,
            &self.prefix,
            &new_store_id,
            &child_state,
        )
        .await?;

        let frozen_state = State {
            children: vec![new_store_id.clone()],
            ..self.state.clone()
        };
        write_state(
            &self.s3,
            &self.bucket,
            &self.prefix,
            &self.id,
            &frozen_state,
        )
        .await?;
        self.frozen.store(true, Ordering::Release);

        info!(frozen = %self.id, child = %new_store_id, "snapshot created");
        Ok(())
    }

    /// Freeze this store and create two child stores (continuation + clone).
    pub async fn clone_store(&self, continuation_id: String, clone_id: String) -> Result<()> {
        let _timing = crate::metrics::timing!("store.clone");
        Self::validate_store_id(&continuation_id)?;
        Self::validate_store_id(&clone_id)?;
        let _write_guard = self.write_lock.write().await;

        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is already frozen, cannot clone"
        );

        self.flush().await.context("flush before clone failed")?;

        // Write both child states before freezing the parent (crash-safe ordering).
        for child_id in [&continuation_id, &clone_id] {
            let child_state = State {
                parent_id: Some(self.id.clone()),
                block_size: self.state.block_size,
                volume_size: self.state.volume_size,
                children: vec![],
            };
            write_state(&self.s3, &self.bucket, &self.prefix, child_id, &child_state).await?;
        }

        let frozen_state = State {
            children: vec![continuation_id.clone(), clone_id.clone()],
            ..self.state.clone()
        };
        write_state(
            &self.s3,
            &self.bucket,
            &self.prefix,
            &self.id,
            &frozen_state,
        )
        .await?;
        self.frozen.store(true, Ordering::Release);

        info!(
            frozen = %self.id,
            continuation = %continuation_id,
            clone = %clone_id,
            "clone created"
        );
        Ok(())
    }
}

#[path = "store_tests.rs"]
#[cfg(test)]
mod tests;
