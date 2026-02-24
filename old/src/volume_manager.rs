//! VolumeManager: owns per-volume locks and swappable store references.
//!
//! FUSE writes hold the volume lock (shared); snapshot/clone hold it exclusive.
//! The store pointer is swapped atomically via ArcSwap while the exclusive lock
//! is held, so no write can observe the frozen store.

use crate::inode::GlobalIno;
use crate::s3::{self, S3Access, State};
use crate::store::Store;
use anyhow::Result;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tracing::info;

/// Low-level FUSE volume inodes start here.
const FIRST_VOLUME_INO: u64 = 100;

struct VolumeSlot<S: S3Access> {
    /// Per-volume lock: shared for writes, exclusive for snapshot/clone.
    lock: Arc<RwLock<()>>,
    /// Swappable store reference.
    store: ArcSwap<Store<S>>,
    ino: GlobalIno,
}

pub struct VolumeManager<S: S3Access> {
    s3: S,
    bucket: String,
    /// User-supplied prefix, e.g. "" or "myapp". Passed to Store::load (which
    /// appends "/stores" internally).
    prefix: String,
    /// `prefix` + "/stores", e.g. "stores" or "myapp/stores". Used for direct
    /// S3 state/block key calls that bypass Store::load.
    stores_prefix: String,
    max_uploads: usize,
    max_downloads: usize,
    volumes: DashMap<String, VolumeSlot<S>>,
    ino_map: DashMap<u64, String>,
    next_ino: AtomicU64,
}

impl<S: S3Access> VolumeManager<S> {
    pub fn new(
        s3: S,
        bucket: String,
        prefix: String,
        max_uploads: usize,
        max_downloads: usize,
    ) -> Self {
        let stores_prefix = s3::normalize_prefix(&prefix);
        Self {
            s3,
            bucket,
            prefix,
            stores_prefix,
            max_uploads,
            max_downloads,
            volumes: DashMap::new(),
            ino_map: DashMap::new(),
            next_ino: AtomicU64::new(FIRST_VOLUME_INO),
        }
    }

    /// Register a volume with its store. Returns the assigned inode.
    pub fn add_volume(&self, name: String, store: Arc<Store<S>>) -> GlobalIno {
        let raw = self.next_ino.fetch_add(1, Ordering::Relaxed);
        let ino = GlobalIno::from_raw(raw);
        self.ino_map.insert(raw, name.clone());
        self.volumes.insert(
            name,
            VolumeSlot {
                lock: Arc::new(RwLock::new(())),
                store: ArcSwap::from(store),
                ino,
            },
        );
        ino
    }

    /// Get a store by volume name (lock-free load).
    pub fn get_store(&self, name: &str) -> Option<Arc<Store<S>>> {
        self.volumes.get(name).map(|slot| slot.store.load_full())
    }

    /// Get a store by inode (lock-free load, for FUSE reads).
    pub fn get_store_by_ino(&self, ino: GlobalIno) -> Option<(String, Arc<Store<S>>)> {
        let name = self.ino_map.get(&ino.raw())?;
        let slot = self.volumes.get(name.value())?;
        Some((name.value().clone(), slot.store.load_full()))
    }

    /// Acquire the volume lock (shared) and load the current store.
    /// Used by FUSE writes — blocks if a snapshot/clone is in progress.
    pub async fn write_lock_by_ino(
        &self,
        ino: GlobalIno,
    ) -> Option<(Arc<Store<S>>, OwnedRwLockReadGuard<()>)> {
        let name = self.ino_map.get(&ino.raw())?;
        let slot = self.volumes.get(name.value())?;
        let lock = Arc::clone(&slot.lock);
        let guard = lock.read_owned().await;
        let store = slot.store.load_full();
        Some((store, guard))
    }

    /// List all volume names.
    pub fn volume_names(&self) -> Vec<String> {
        self.volumes.iter().map(|e| e.key().clone()).collect()
    }

    /// Iterate volumes for readdir: (name, ino, volume_size).
    pub fn iter_volumes(&self) -> Vec<(String, GlobalIno, u64)> {
        self.volumes
            .iter()
            .map(|e| {
                let store = e.value().store.load();
                (e.key().clone(), e.value().ino, store.state.volume_size)
            })
            .collect()
    }

    /// Get the inode for a volume by name.
    pub fn get_ino(&self, name: &str) -> Option<GlobalIno> {
        self.volumes.get(name).map(|slot| slot.ino)
    }

    /// Get volume_size and block_size for a volume by inode.
    pub fn volume_info_by_ino(&self, ino: GlobalIno) -> Option<(u64, u64)> {
        let name = self.ino_map.get(&ino.raw())?;
        let slot = self.volumes.get(name.value())?;
        let store = slot.store.load();
        Some((store.state.volume_size, store.state.block_size))
    }

    /// Write a child state to S3 and add it to the parent's children list.
    async fn create_child(&self, parent_id: &str, child_id: &str, parent_state: &State) -> Result<()> {
        let child_state = State {
            parent_id: Some(parent_id.to_string()),
            block_size: parent_state.block_size,
            volume_size: parent_state.volume_size,
            frozen_at: None,
            children: vec![],
        };
        s3::write_state(&self.s3, &self.bucket, &self.stores_prefix, child_id, &child_state).await?;

        // Add child to parent's children list (read-modify-write).
        let mut parent = s3::read_state(&self.s3, &self.bucket, &self.stores_prefix, parent_id).await?;
        parent.children.push(child_id.to_string());
        s3::write_state(&self.s3, &self.bucket, &self.stores_prefix, parent_id, &parent).await?;
        Ok(())
    }

    /// Snapshot a volume: freeze the current store, load a continuation store,
    /// and swap the volume pointer so the volume stays writable.
    ///
    /// The frozen parent IS the snapshot — a read-only point-in-time reference.
    /// The continuation (new child) becomes the writable successor.
    pub async fn snapshot(&self, volume_name: &str, snapshot_name: &str) -> Result<()> {
        let _timing = crate::metrics::timing!("volume_manager.snapshot");
        let slot = self
            .volumes
            .get(volume_name)
            .ok_or_else(|| anyhow::anyhow!("volume {volume_name:?} not found"))?;

        // Exclusive lock — drains all FUSE writes.
        let _guard = slot.lock.write().await;
        let store = slot.store.load_full();

        let continuation_id = uuid::Uuid::new_v4().to_string();

        // Freeze: flush + shutdown uploader + mark frozen in S3.
        store.freeze().await?;

        // Create continuation child in S3.
        self.create_child(&store.id, &continuation_id, &store.state).await?;

        // Load continuation store (fresh uploader, inherits parent data).
        let continuation = Store::load(
            self.s3.clone(),
            self.bucket.clone(),
            self.prefix.clone(),
            continuation_id.clone(),
            self.max_uploads,
            self.max_downloads,
        )
        .await?;

        // Atomic swap — new writes go to the continuation.
        slot.store.store(continuation);

        // Update volume name → store mapping in S3.
        crate::names::update_volume_ref(
            &self.s3,
            &self.bucket,
            &self.prefix,
            volume_name,
            &continuation_id,
        )
        .await?;

        // Close the old (now frozen) store to release resources.
        store.close().await?;

        info!(
            volume = %volume_name,
            snapshot = %snapshot_name,
            frozen = %store.id,
            continuation = %continuation_id,
            "snapshot complete — volume still writable"
        );

        Ok(())
    }

    /// Clone a volume: freeze the current store, create two children
    /// (continuation + clone), and swap the volume pointer.
    ///
    /// The clone is NOT automatically mounted — it's a name → store mapping
    /// in S3 that can be mounted separately.
    pub async fn clone_volume(&self, volume_name: &str, clone_name: &str) -> Result<()> {
        let _timing = crate::metrics::timing!("volume_manager.clone");
        let slot = self
            .volumes
            .get(volume_name)
            .ok_or_else(|| anyhow::anyhow!("volume {volume_name:?} not found"))?;

        // Exclusive lock — drains all FUSE writes.
        let _guard = slot.lock.write().await;
        let store = slot.store.load_full();

        let continuation_id = uuid::Uuid::new_v4().to_string();
        let clone_id = uuid::Uuid::new_v4().to_string();

        // Freeze: flush + shutdown uploader + mark frozen in S3.
        store.freeze().await?;

        // Create both children in S3.
        self.create_child(&store.id, &continuation_id, &store.state).await?;
        self.create_child(&store.id, &clone_id, &store.state).await?;

        // Load continuation store.
        let continuation = Store::load(
            self.s3.clone(),
            self.bucket.clone(),
            self.prefix.clone(),
            continuation_id.clone(),
            self.max_uploads,
            self.max_downloads,
        )
        .await?;

        // Atomic swap.
        slot.store.store(continuation);

        // Update volume name → continuation, create clone name → clone.
        crate::names::update_volume_ref(
            &self.s3,
            &self.bucket,
            &self.prefix,
            volume_name,
            &continuation_id,
        )
        .await?;
        crate::names::create_volume_ref(
            &self.s3,
            &self.bucket,
            &self.prefix,
            clone_name,
            &clone_id,
        )
        .await?;

        // Close the old (now frozen) store.
        store.close().await?;

        info!(
            volume = %volume_name,
            clone = %clone_name,
            frozen = %store.id,
            continuation = %continuation_id,
            clone_id = %clone_id,
            "clone complete — volume still writable"
        );

        Ok(())
    }

    /// Close all volumes.
    pub async fn close_all(&self) -> Result<()> {
        for entry in self.volumes.iter() {
            let store = entry.value().store.load_full();
            store.close().await?;
        }
        Ok(())
    }
}

#[path = "volume_manager_tests.rs"]
#[cfg(test)]
mod tests;
