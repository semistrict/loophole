use crate::cache;
use crate::cache::ZeroOpKind;
use crate::s3::{BlockIndex, block_key, list_blocks, normalize_prefix, read_state, write_state};
pub use crate::s3::{S3Access, State};
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use dashmap::DashSet;
use metrics::{counter, gauge};

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, watch};
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
// Dirty block set — returned by acquire_dirty_blocks(), holds block lock guards
// ---------------------------------------------------------------------------

pub(crate) struct DirtyBlockSet<'a> {
    pub dirty_blocks: Vec<u64>,
    pub zero_block_indices: Vec<u64>,
    /// Block lock guards — held until caller drops this struct.
    /// Sorted by block index (deadlock-free).
    pub guards: Vec<(u64, tokio::sync::MutexGuard<'a, ()>)>,
    /// The requested_generation observed while write_lock was held.
    /// This is the maximum generation this cycle can claim to satisfy.
    pub generation: u64,
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
    /// True once `close()` has been called.
    closed: AtomicBool,

    // -- Upload generation mechanism --
    /// Bumped by flush() to request an upload cycle.
    pub(crate) requested_generation: AtomicU64,
    /// Bumped by the uploader after each cycle completes. Carries (generation, Option<error_message>).
    pub(crate) completed_generation: watch::Sender<(u64, Option<String>)>,
    /// Notifies the upload loop to wake up immediately.
    pub(crate) flush_notify: Notify,
    /// Signals uploader shutdown.
    uploader_shutdown_tx: watch::Sender<bool>,
    /// Join handle for uploader task.
    uploader_task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
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
            frozen_at: None,
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
        info!(parent = ?state.parent_id, frozen = state.frozen_at.is_some(), "loaded state");

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
            for t in &ptombstones {
                pindex.remove(t);
            }
            debug!(store = %pid, blocks = pindex.len(), tombstones = ptombstones.len(), "indexed ancestor");
            current_parent = pstate.parent_id.clone();
            ancestors.push((pid, pindex));
        }

        let block_locks = Arc::new(BlockLockMap::new());

        // Init cache subdirs BEFORE recover.
        cache::get()
            .init_store(
                &store_id,
                state.parent_id.as_deref(),
                state.block_size,
                Arc::clone(&block_locks),
            )
            .await?;
        for (ancestor_id, _) in &ancestors {
            cache::get()
                .init_store(
                    ancestor_id,
                    None,
                    state.block_size,
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
        let (completed_generation, _) = watch::channel((0u64, None));
        let (uploader_shutdown_tx, uploader_shutdown_rx) = watch::channel(false);
        let frozen = AtomicBool::new(state.frozen_at.is_some());

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
            closed: AtomicBool::new(false),
            requested_generation: AtomicU64::new(0),
            completed_generation,
            flush_notify: Notify::new(),
            uploader_shutdown_tx,
            uploader_task: StdMutex::new(None),
        });

        // Spawn the uploader.
        store.spawn_uploader(uploader_shutdown_rx);

        // If there's recovered dirty work, request an upload cycle.
        if !recovery.dirty_blocks.is_empty() || !recovery.pending_zero_ops.is_empty() {
            store.requested_generation.fetch_add(1, Ordering::Release);
            store.flush_notify.notify_one();
        }

        Ok(store)
    }

    fn spawn_uploader(self: &Arc<Self>, shutdown_rx: watch::Receiver<bool>) {
        let task = crate::uploader::spawn(Arc::clone(self), shutdown_rx);
        if let Ok(mut slot) = self.uploader_task.lock() {
            *slot = Some(task);
        }
    }

    async fn shutdown_uploader(&self) {
        let _ = self.uploader_shutdown_tx.send(true);
        self.flush_notify.notify_one();
        let task = if let Ok(mut slot) = self.uploader_task.lock() {
            slot.take()
        } else {
            None
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    fn block_lock(&self, block_idx: u64) -> &Mutex<()> {
        self.block_locks.lock(block_idx)
    }

    /// Drain in-flight writes, snapshot dirty state, lock all dirty blocks.
    /// Returns dirty set with guards held. Write lock is released before return.
    pub(crate) async fn acquire_dirty_blocks(&self) -> Result<DirtyBlockSet<'_>> {
        // 1. Acquire write_lock.write() — all in-flight writes complete
        let _write_guard = self.write_lock.write().await;

        // Capture the generation while write lock is held — this is the maximum
        // generation this cycle can claim to satisfy, since any flush() that
        // bumped the generation before this point has had its writes drained.
        let generation = self.requested_generation.load(Ordering::Acquire);

        // 2. List dirty blocks + zero ops (consistent — no writes in flight)
        let dirty_blocks = cache::get().list_dirty_blocks(&self.id).await?;
        let zero_block_indices: Vec<u64> = self.zero_blocks.iter().map(|r| *r).collect();

        // 3. Build sorted deduplicated index list
        let mut all_indices: Vec<u64> = dirty_blocks
            .iter()
            .copied()
            .chain(zero_block_indices.iter().copied())
            .collect();
        all_indices.sort_unstable();
        all_indices.dedup();

        // 4. Acquire all block locks (sorted order)
        let mut guards = Vec::with_capacity(all_indices.len());
        for &block_idx in &all_indices {
            let lock = self.block_locks.lock(block_idx);
            guards.push((block_idx, lock.lock().await));
        }

        // 5. write_lock released here (drop _write_guard)
        //    Writes can resume on non-locked blocks.

        Ok(DirtyBlockSet {
            dirty_blocks,
            zero_block_indices,
            guards,
            generation,
        })
    }

    /// Drain in-flight writes, flush all dirty blocks to S3, shut down the
    /// uploader, create child state(s) in S3, and mark this store as frozen.
    ///
    /// `children` is the list of child store IDs to create (1 for snapshot,
    /// 2 for clone). The caller (VolumeManager) decides the semantics.
    ///
    /// The caller must already hold the volume-level exclusive lock so no
    /// new FUSE writes can start.
    /// Flush all dirty blocks, shut down the uploader, and mark this store
    /// as frozen.  No further writes are allowed after this returns.
    pub(crate) async fn freeze(&self) -> Result<()> {
        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is already frozen"
        );
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "store is closed"
        );

        self.flush().await?;
        self.shutdown_uploader().await;

        let frozen_state = State {
            frozen_at: Some(chrono::Utc::now().to_rfc3339()),
            ..self.state.clone()
        };
        write_state(&self.s3, &self.bucket, &self.prefix, &self.id, &frozen_state).await?;
        self.frozen.store(true, Ordering::Release);

        info!(frozen = %self.id, "store frozen");
        Ok(())
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
    async fn do_zero_block(&self, block_idx: u64) -> Result<()> {
        counter!("store.zero_block.total").increment(1);
        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "store is closed, cannot write"
        );
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

        Ok(())
    }

    /// Atomically zero a byte range within a single block.
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

        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "store is closed, cannot write"
        );
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
            self.do_read_block(block_idx, 0, block_size as usize)
                .await?
        };

        // Zero the requested range.
        let start = offset_within_block as usize;
        let end = start + len as usize;
        data[start..end].fill(0);

        // Write back the full block.
        if self.zero_blocks.remove(&block_idx).is_some() {
            cache::get().clear_zero_op(&self.id, block_idx).await?;
        }
        self.do_write_block_locked(block_idx, &data).await
    }

    /// Read exactly `len` bytes at `offset_within_block` from `block_idx`.
    async fn do_read_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let _timing = crate::metrics::timing!("store.read");
        counter!("store.read.bytes").increment(len as u64);

        if self.zero_blocks.contains(&block_idx) {
            debug!(block = block_idx, "zero block, returning zeros");
            counter!("store.read.zeros").increment(1);
            return Ok(vec![0u8; len]);
        }

        // Check local store cache first.
        if let Some(result) = cache::get()
            .read(&self.id, block_idx, offset_within_block, len)
            .await
        {
            counter!("store.read.cache_hit").increment(1);
            return result.context("cache read failed");
        }

        // Check ancestor caches.
        for (ancestor_id, ancestor_index) in self.ancestors.iter() {
            if ancestor_index.contains(&block_idx) {
                if let Some(result) = cache::get()
                    .read(ancestor_id, block_idx, offset_within_block, len)
                    .await
                {
                    counter!("store.read.cache_hit").increment(1);
                    return result.context("ancestor cache read failed");
                }
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

        let bytes = self
            .s3
            .get_byte_range(&self.bucket, &key, offset_within_block, len)
            .await
            .context("get_byte_range failed")?;

        // Spawn background full-block fetch to warm cache.
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
                self.inflight_downloads.remove(&block_idx);
            }
        }

        Ok(bytes)
    }

    /// Write a full block to cache.
    /// Caller MUST already hold write_lock (read) and the block lock.
    async fn do_write_block_locked(&self, block_idx: u64, data: &[u8]) -> Result<()> {
        let block_size = self.state.block_size;
        debug_assert_eq!(data.len() as u64, block_size);

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

        let _ = cache::get().try_mark_dirty(&self.id, block_idx).await?;
        cache::get()
            .pwrite(&self.id, block_idx, 0, data, Arc::clone(&self.block_locks))
            .await?;

        Ok(())
    }

    /// Write `data` at `offset_within_block` within `block_idx`.
    async fn do_write_block(
        &self,
        block_idx: u64,
        offset_within_block: u64,
        data: &[u8],
    ) -> Result<()> {
        let _timing = crate::metrics::timing!("store.write");
        counter!("store.write.bytes").increment(data.len() as u64);
        let block_size = self.state.block_size;

        // Detect full-block zero writes.
        let is_full_block_zero = offset_within_block == 0
            && data.len() as u64 == block_size
            && data.iter().all(|&b| b == 0);

        if is_full_block_zero {
            return self.do_zero_block(block_idx).await;
        }

        let _write_guard = self.write_lock.read().await;
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "store is closed, cannot write"
        );
        anyhow::ensure!(
            !self.frozen.load(Ordering::Acquire),
            "store is frozen, cannot write"
        );

        let block_lock = self.block_lock(block_idx);
        let _block_guard = block_lock.lock().await;

        let was_zeroed = self.zero_blocks.remove(&block_idx).is_some();
        if was_zeroed {
            cache::get().clear_zero_op(&self.id, block_idx).await?;
        }

        let has_cache = cache::get().has_block(&self.id, block_idx).await?;
        let fetch_key: Option<String> = if has_cache || was_zeroed {
            None
        } else if self.local_index.contains(&block_idx) {
            Some(block_key(&self.prefix, &self.id, block_idx))
        } else {
            self.ancestors
                .iter()
                .find(|(_, idx)| idx.contains(&block_idx))
                .map(|(id, _)| block_key(&self.prefix, id, block_idx))
        };

        if has_cache {
            // Already cached.
        } else if let Some(key) = fetch_key {
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
            cache::get()
                .ensure_sparse_block(
                    &self.id,
                    block_idx,
                    block_size,
                    Arc::clone(&self.block_locks),
                )
                .await?;
        }

        let _ = cache::get().try_mark_dirty(&self.id, block_idx).await?;
        cache::get()
            .pwrite(
                &self.id,
                block_idx,
                offset_within_block,
                data,
                Arc::clone(&self.block_locks),
            )
            .await?;

        drop(_block_guard);
        Ok(())
    }

    /// Read `size` bytes starting at byte `offset` within the volume.
    /// Splits across block boundaries automatically.
    pub async fn read(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let volume_size = self.state.volume_size;
        let block_size = self.state.block_size;

        if offset >= volume_size || size == 0 {
            return Ok(vec![]);
        }
        let read_end = offset.saturating_add(size as u64).min(volume_size);
        let first_block = offset / block_size;
        let last_block = (read_end - 1) / block_size;
        let mut buf = Vec::with_capacity((read_end - offset) as usize);

        for block_idx in first_block..=last_block {
            let block_start = block_idx * block_size;
            let slice_start = offset.saturating_sub(block_start);
            let slice_end = (read_end - block_start).min(block_size);
            let len = (slice_end - slice_start) as usize;
            buf.extend_from_slice(&self.do_read_block(block_idx, slice_start, len).await?);
        }
        Ok(buf)
    }

    /// Write `data` starting at byte `offset` within the volume.
    /// Splits across block boundaries automatically.
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let volume_size = self.state.volume_size;
        let block_size = self.state.block_size;
        anyhow::ensure!(offset < volume_size, "write starts past end of volume");

        let max_len = (volume_size - offset) as usize;
        let data = &data[..data.len().min(max_len)];
        let write_end = offset + data.len() as u64;
        let first_block = offset / block_size;
        let last_block = (write_end - 1) / block_size;
        let mut data_pos = 0usize;

        for block_idx in first_block..=last_block {
            let block_start = block_idx * block_size;
            let write_start = offset.saturating_sub(block_start);
            let block_end = (write_end - block_start).min(block_size);
            let write_len = (block_end - write_start) as usize;

            let chunk = &data[data_pos..data_pos + write_len];
            data_pos += write_len;

            self.do_write_block(block_idx, write_start, chunk).await?;
        }
        Ok(())
    }

    /// Wait until all currently dirty blocks have been uploaded to S3.
    /// The uploader's `acquire_dirty_blocks()` provides the write-lock consistency
    /// guarantee — no need to hold write_lock here.
    pub async fn flush(&self) -> Result<()> {
        let _timing = crate::metrics::timing!("store.flush");

        // Compute target = current + 1, then use fetch_max so concurrent callers
        // all coalesce on the same generation instead of each bumping by 1.
        let current = self.requested_generation.load(Ordering::Acquire);
        let target_gen = current + 1;
        self.requested_generation
            .fetch_max(target_gen, Ordering::Release);
        self.flush_notify.notify_one();

        // Wait until completed_generation >= our requested generation.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let mut rx = self.completed_generation.subscribe();
        loop {
            {
                let val = rx.borrow();
                if val.0 >= target_gen {
                    if let Some(ref err) = val.1 {
                        anyhow::bail!("upload cycle failed: {err}");
                    }
                    return Ok(());
                }
            }
            tokio::select! {
                result = rx.changed() => {
                    if result.is_err() {
                        anyhow::bail!("upload channel closed during flush");
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let remaining = cache::get().list_dirty_blocks(&self.id).await.unwrap_or_default();
                    anyhow::bail!(
                        "flush timed out after 5 minutes — {} blocks still dirty: {:?}",
                        remaining.len(),
                        &remaining[..remaining.len().min(10)]
                    );
                }
            }
        }
    }

}

impl<S: S3Access> Store<S> {
    pub async fn close(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        {
            let _guard = self.write_lock.write().await;
            self.closed.store(true, Ordering::SeqCst);
        }
        let result = self.flush().await;
        self.shutdown_uploader().await;
        result
    }
}

impl<S: S3Access> Drop for Store<S> {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::SeqCst) {
            tracing::error!(store = %self.id, "Store dropped without calling close()");
        }
    }
}

#[path = "store_tests.rs"]
#[cfg(test)]
mod tests;
