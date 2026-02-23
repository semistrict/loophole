//! Virtual `.loophole/` control directory for snapshot and clone operations.
//!
//! Shared by all filesystem backends (FUSE low-level, FUSE+lwext4, NFS).
//!
//! Layout:
//!   .loophole/
//!     snapshots/
//!       {name}   — creating this file triggers a snapshot; reading returns status JSON
//!     clones/
//!       {name}   — creating this file triggers a clone; reading returns status JSON

use crate::store::{S3Access, Store};
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

// Well-known virtual inode numbers. Chosen high to avoid collision with ext4 inodes.
pub const CTL_DIR_INO: u64 = u64::MAX - 10;
pub const SNAPSHOTS_DIR_INO: u64 = u64::MAX - 11;
pub const CLONES_DIR_INO: u64 = u64::MAX - 12;
const VIRTUAL_INO_START: u64 = u64::MAX - 100;

/// Returns true if this inode belongs to the virtual control tree.
pub fn is_virtual_ino(ino: u64) -> bool {
    ino >= VIRTUAL_INO_START
}

/// A virtual directory entry (name + inode + is_dir).
pub struct CtlEntry {
    pub name: String,
    pub ino: u64,
    pub is_dir: bool,
}

/// Status of a snapshot or clone operation.
#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum OpStatus {
    Pending,
    Complete {
        #[serde(skip_serializing_if = "Option::is_none")]
        continuation: Option<String>,
    },
    Error {
        error: String,
    },
}

/// Shared control-directory state. Embed one of these in each filesystem impl.
pub struct Ctl<S: S3Access> {
    store: Arc<Store<S>>,
    /// inode → status JSON bytes
    virtual_files: DashMap<u64, Vec<u8>>,
    /// snapshot name → inode
    snapshot_files: DashMap<String, u64>,
    /// clone name → inode
    clone_files: DashMap<String, u64>,
    next_virtual_ino: AtomicU64,
}

impl<S: S3Access> Ctl<S> {
    pub fn new(store: Arc<Store<S>>) -> Self {
        Self {
            store,
            virtual_files: DashMap::new(),
            snapshot_files: DashMap::new(),
            clone_files: DashMap::new(),
            next_virtual_ino: AtomicU64::new(VIRTUAL_INO_START - 1),
        }
    }

    fn alloc_ino(&self) -> u64 {
        self.next_virtual_ino.fetch_sub(1, Ordering::Relaxed)
    }

    fn set_status(&self, ino: u64, status: &OpStatus) {
        let json = serde_json::to_vec_pretty(status).unwrap_or_default();
        self.virtual_files.insert(ino, json);
    }

    // ── Lookup ────────────────────────────────────────────────────────

    /// Try to resolve a lookup in the virtual tree. Returns `Some(ino)` if
    /// the parent+name belongs to the control directory, `None` otherwise.
    pub fn lookup(&self, parent_ino: u64, name: &str) -> Option<u64> {
        match (parent_ino, name) {
            (_, ".loophole") => Some(CTL_DIR_INO), // parent is root — caller checks
            (CTL_DIR_INO, "snapshots") => Some(SNAPSHOTS_DIR_INO),
            (CTL_DIR_INO, "clones") => Some(CLONES_DIR_INO),
            (CTL_DIR_INO, "..") => None, // caller should map to its own root
            (SNAPSHOTS_DIR_INO, "..") => Some(CTL_DIR_INO),
            (CLONES_DIR_INO, "..") => Some(CTL_DIR_INO),
            (SNAPSHOTS_DIR_INO, n) => self.snapshot_files.get(n).map(|v| *v),
            (CLONES_DIR_INO, n) => self.clone_files.get(n).map(|v| *v),
            _ => None,
        }
    }

    // ── Read ──────────────────────────────────────────────────────────

    /// Read the status JSON for a virtual file. Returns `(data_slice, eof)`.
    pub fn read(&self, ino: u64, offset: u64, count: u32) -> Option<(Vec<u8>, bool)> {
        self.virtual_files.get(&ino).map(|data| {
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            let eof = end >= data.len();
            (data[start..end].to_vec(), eof)
        })
    }

    /// Get the size of a virtual file's status JSON.
    pub fn file_size(&self, ino: u64) -> Option<u64> {
        self.virtual_files.get(&ino).map(|d| d.len() as u64)
    }

    // ── Readdir ───────────────────────────────────────────────────────

    /// List entries for a virtual directory. Returns `None` if `ino` is not
    /// a virtual directory.
    pub fn readdir(&self, ino: u64) -> Option<Vec<CtlEntry>> {
        match ino {
            CTL_DIR_INO => Some(vec![
                CtlEntry {
                    name: "snapshots".into(),
                    ino: SNAPSHOTS_DIR_INO,
                    is_dir: true,
                },
                CtlEntry {
                    name: "clones".into(),
                    ino: CLONES_DIR_INO,
                    is_dir: true,
                },
            ]),
            SNAPSHOTS_DIR_INO => Some(
                self.snapshot_files
                    .iter()
                    .map(|e| CtlEntry {
                        name: e.key().clone(),
                        ino: *e.value(),
                        is_dir: false,
                    })
                    .collect(),
            ),
            CLONES_DIR_INO => Some(
                self.clone_files
                    .iter()
                    .map(|e| CtlEntry {
                        name: e.key().clone(),
                        ino: *e.value(),
                        is_dir: false,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    // ── Create (triggers operations) ──────────────────────────────────

    /// Create a virtual file. Returns `Some((ino, size))` if the parent is a
    /// virtual snapshot/clone dir, `None` otherwise.
    /// The creation triggers the actual snapshot/clone operation.
    pub async fn create(&self, parent_ino: u64, name: &str) -> Option<Result<(u64, u64), String>> {
        match parent_ino {
            SNAPSHOTS_DIR_INO => {
                if self.snapshot_files.contains_key(name) {
                    return Some(Err("already exists".into()));
                }
                let ino = self.alloc_ino();
                self.set_status(ino, &OpStatus::Pending);
                self.snapshot_files.insert(name.to_string(), ino);

                let status = self.do_snapshot(name.to_string()).await;
                self.set_status(ino, &status);

                let size = self.file_size(ino).unwrap_or(0);
                Some(Ok((ino, size)))
            }
            CLONES_DIR_INO => {
                if self.clone_files.contains_key(name) {
                    return Some(Err("already exists".into()));
                }
                let ino = self.alloc_ino();
                self.set_status(ino, &OpStatus::Pending);
                self.clone_files.insert(name.to_string(), ino);

                let status = self.do_clone(name.to_string()).await;
                self.set_status(ino, &status);

                let size = self.file_size(ino).unwrap_or(0);
                Some(Ok((ino, size)))
            }
            _ => None,
        }
    }

    // ── Operations ────────────────────────────────────────────────────

    async fn do_snapshot(&self, new_store_id: String) -> OpStatus {
        info!(new_store = %new_store_id, "snapshot requested via .loophole/snapshots");
        match self.store.snapshot(new_store_id).await {
            Ok(()) => OpStatus::Complete { continuation: None },
            Err(e) => OpStatus::Error {
                error: format!("{e:#}"),
            },
        }
    }

    async fn do_clone(&self, clone_id: String) -> OpStatus {
        let continuation_id = format!("{}-cont-{}", self.store.id, ts_secs());
        info!(
            clone = %clone_id,
            continuation = %continuation_id,
            "clone requested via .loophole/clones"
        );
        match self
            .store
            .clone_store(continuation_id.clone(), clone_id)
            .await
        {
            Ok(()) => OpStatus::Complete {
                continuation: Some(continuation_id),
            },
            Err(e) => OpStatus::Error {
                error: format!("{e:#}"),
            },
        }
    }
}

fn ts_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
