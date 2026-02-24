//! Virtual `.loophole/` control directory for snapshot and clone operations.
//!
//! Layout (multi-volume):
//!   .loophole/
//!     snapshots/
//!       <volume_name>/
//!         {name}   — creating this file triggers a snapshot; reading returns status JSON
//!     clones/
//!       <volume_name>/
//!         {name}   — creating this file triggers a clone; reading returns status JSON

use crate::inode::{
    GlobalIno, VirtualIno, ALLOC_START, CLONES_DIR_INO, CTL_DIR_INO, SNAPSHOTS_DIR_INO,
};
use crate::store::S3Access;
use crate::volume_manager::VolumeManager;
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

/// A virtual directory entry (name + inode + is_dir).
pub struct CtlEntry {
    pub name: String,
    pub ino: GlobalIno,
    pub is_dir: bool,
}

/// Status of a snapshot or clone operation.
#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum OpStatus {
    Pending,
    Complete,
    Error {
        error: String,
    },
}

/// Shared control-directory state. Embed one of these in each filesystem impl.
pub struct Ctl<S: S3Access> {
    vm: Arc<VolumeManager<S>>,
    /// VirtualIno → status JSON bytes
    virtual_files: DashMap<VirtualIno, Vec<u8>>,
    /// Per-volume snapshot subdirectory inodes: volume_name → VirtualIno
    snapshot_vol_dirs: DashMap<String, VirtualIno>,
    /// Per-volume clone subdirectory inodes: volume_name → VirtualIno
    clone_vol_dirs: DashMap<String, VirtualIno>,
    /// Snapshot operation files: "volume_name/filename" → VirtualIno
    snapshot_files: DashMap<String, VirtualIno>,
    /// Clone operation files: "volume_name/filename" → VirtualIno
    clone_files: DashMap<String, VirtualIno>,
    next_virtual_ino: AtomicU64,
}

impl<S: S3Access> Ctl<S> {
    pub fn new(vm: Arc<VolumeManager<S>>) -> Self {
        let ctl = Self {
            vm,
            virtual_files: DashMap::new(),
            snapshot_vol_dirs: DashMap::new(),
            clone_vol_dirs: DashMap::new(),
            snapshot_files: DashMap::new(),
            clone_files: DashMap::new(),
            next_virtual_ino: AtomicU64::new(ALLOC_START),
        };
        for name in ctl.vm.volume_names() {
            ctl.ensure_vol_dirs(&name);
        }
        ctl
    }

    fn alloc_virt(&self) -> VirtualIno {
        let raw = self.next_virtual_ino.fetch_sub(1, Ordering::Relaxed);
        VirtualIno::new(raw)
    }

    fn set_status(&self, ino: VirtualIno, status: &OpStatus) {
        let json = serde_json::to_vec_pretty(status).unwrap_or_default();
        self.virtual_files.insert(ino, json);
    }

    /// Ensure per-volume subdirectories exist for snapshots/ and clones/.
    fn ensure_vol_dirs(&self, volume_name: &str) -> (VirtualIno, VirtualIno) {
        let snap_ino = *self
            .snapshot_vol_dirs
            .entry(volume_name.to_string())
            .or_insert_with(|| self.alloc_virt());
        let clone_ino = *self
            .clone_vol_dirs
            .entry(volume_name.to_string())
            .or_insert_with(|| self.alloc_virt());
        (snap_ino, clone_ino)
    }

    /// Register a newly added volume's ctl directories.
    pub fn register_volume(&self, volume_name: &str) {
        self.ensure_vol_dirs(volume_name);
    }

    /// Check if a GlobalIno is a per-volume ctl subdirectory.
    fn is_vol_snapshot_dir(&self, ino: VirtualIno) -> Option<String> {
        self.snapshot_vol_dirs
            .iter()
            .find(|e| *e.value() == ino)
            .map(|e| e.key().clone())
    }

    fn is_vol_clone_dir(&self, ino: VirtualIno) -> Option<String> {
        self.clone_vol_dirs
            .iter()
            .find(|e| *e.value() == ino)
            .map(|e| e.key().clone())
    }

    /// Returns true if this is a well-known or allocated ctl directory inode.
    pub fn is_ctl_dir(&self, virt: VirtualIno) -> bool {
        virt == CTL_DIR_INO
            || virt == SNAPSHOTS_DIR_INO
            || virt == CLONES_DIR_INO
            || self.is_vol_snapshot_dir(virt).is_some()
            || self.is_vol_clone_dir(virt).is_some()
    }

    // ── Lookup ────────────────────────────────────────────────────────

    /// Try to resolve a lookup in the virtual tree.
    /// `parent` must be a virtual inode.
    /// Returns `Some(GlobalIno)` if the name resolves, `None` otherwise.
    pub fn lookup(&self, parent: VirtualIno, name: &str) -> Option<GlobalIno> {
        if parent == CTL_DIR_INO {
            return match name {
                "snapshots" => Some(GlobalIno::from_virtual(SNAPSHOTS_DIR_INO)),
                "clones" => Some(GlobalIno::from_virtual(CLONES_DIR_INO)),
                ".." => None, // caller maps to its own root
                _ => None,
            };
        }
        if parent == SNAPSHOTS_DIR_INO {
            if name == ".." {
                return Some(GlobalIno::from_virtual(CTL_DIR_INO));
            }
            return self
                .snapshot_vol_dirs
                .get(name)
                .map(|v| GlobalIno::from_virtual(*v));
        }
        if parent == CLONES_DIR_INO {
            if name == ".." {
                return Some(GlobalIno::from_virtual(CTL_DIR_INO));
            }
            return self
                .clone_vol_dirs
                .get(name)
                .map(|v| GlobalIno::from_virtual(*v));
        }
        // Check per-volume snapshot/clone dirs
        if let Some(vol_name) = self.is_vol_snapshot_dir(parent) {
            if name == ".." {
                return Some(GlobalIno::from_virtual(SNAPSHOTS_DIR_INO));
            }
            let key = format!("{vol_name}/{name}");
            return self
                .snapshot_files
                .get(&key)
                .map(|v| GlobalIno::from_virtual(*v));
        }
        if let Some(vol_name) = self.is_vol_clone_dir(parent) {
            if name == ".." {
                return Some(GlobalIno::from_virtual(CLONES_DIR_INO));
            }
            let key = format!("{vol_name}/{name}");
            return self
                .clone_files
                .get(&key)
                .map(|v| GlobalIno::from_virtual(*v));
        }
        None
    }

    /// Lookup `.loophole` from any parent. Callers should check parent == root first.
    pub fn lookup_loophole(&self) -> GlobalIno {
        GlobalIno::from_virtual(CTL_DIR_INO)
    }

    // ── Read ──────────────────────────────────────────────────────────

    /// Read the status JSON for a virtual file. Returns `(data_slice, eof)`.
    pub fn read(&self, ino: VirtualIno, offset: u64, count: u32) -> Option<(Vec<u8>, bool)> {
        self.virtual_files.get(&ino).map(|data| {
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            let eof = end >= data.len();
            (data[start..end].to_vec(), eof)
        })
    }

    /// Get the size of a virtual file's status JSON.
    pub fn file_size(&self, ino: VirtualIno) -> Option<u64> {
        self.virtual_files.get(&ino).map(|d| d.len() as u64)
    }

    // ── Readdir ───────────────────────────────────────────────────────

    /// List entries for a virtual directory. Returns `None` if `ino` is not
    /// a virtual directory.
    pub fn readdir(&self, ino: VirtualIno) -> Option<Vec<CtlEntry>> {
        if ino == CTL_DIR_INO {
            return Some(vec![
                CtlEntry {
                    name: "snapshots".into(),
                    ino: GlobalIno::from_virtual(SNAPSHOTS_DIR_INO),
                    is_dir: true,
                },
                CtlEntry {
                    name: "clones".into(),
                    ino: GlobalIno::from_virtual(CLONES_DIR_INO),
                    is_dir: true,
                },
            ]);
        }
        if ino == SNAPSHOTS_DIR_INO {
            return Some(
                self.snapshot_vol_dirs
                    .iter()
                    .map(|e| CtlEntry {
                        name: e.key().clone(),
                        ino: GlobalIno::from_virtual(*e.value()),
                        is_dir: true,
                    })
                    .collect(),
            );
        }
        if ino == CLONES_DIR_INO {
            return Some(
                self.clone_vol_dirs
                    .iter()
                    .map(|e| CtlEntry {
                        name: e.key().clone(),
                        ino: GlobalIno::from_virtual(*e.value()),
                        is_dir: true,
                    })
                    .collect(),
            );
        }
        // Per-volume snapshot dir
        if let Some(vol_name) = self.is_vol_snapshot_dir(ino) {
            let prefix = format!("{vol_name}/");
            let entries = self
                .snapshot_files
                .iter()
                .filter(|e| e.key().starts_with(&prefix))
                .map(|e| {
                    let file_name = e.key().strip_prefix(&prefix).unwrap_or(e.key());
                    CtlEntry {
                        name: file_name.to_string(),
                        ino: GlobalIno::from_virtual(*e.value()),
                        is_dir: false,
                    }
                })
                .collect();
            return Some(entries);
        }
        // Per-volume clone dir
        if let Some(vol_name) = self.is_vol_clone_dir(ino) {
            let prefix = format!("{vol_name}/");
            let entries = self
                .clone_files
                .iter()
                .filter(|e| e.key().starts_with(&prefix))
                .map(|e| {
                    let file_name = e.key().strip_prefix(&prefix).unwrap_or(e.key());
                    CtlEntry {
                        name: file_name.to_string(),
                        ino: GlobalIno::from_virtual(*e.value()),
                        is_dir: false,
                    }
                })
                .collect();
            return Some(entries);
        }
        None
    }

    // ── Create (triggers operations) ──────────────────────────────────

    /// Create a virtual file. Returns `Some((GlobalIno, size))` if the parent is a
    /// virtual per-volume snapshot/clone dir, `None` otherwise.
    pub async fn create(
        &self,
        parent: VirtualIno,
        name: &str,
    ) -> Option<Result<(GlobalIno, u64), String>> {
        // Check if parent is a per-volume snapshot dir
        if let Some(vol_name) = self.is_vol_snapshot_dir(parent) {
            let key = format!("{vol_name}/{name}");
            if self.snapshot_files.contains_key(&key) {
                return Some(Err("already exists".into()));
            }
            let virt = self.alloc_virt();
            self.set_status(virt, &OpStatus::Pending);
            self.snapshot_files.insert(key, virt);

            let status = self.do_snapshot(&vol_name, name).await;
            self.set_status(virt, &status);

            let size = self.file_size(virt).unwrap_or(0);
            return Some(Ok((GlobalIno::from_virtual(virt), size)));
        }

        // Check if parent is a per-volume clone dir
        if let Some(vol_name) = self.is_vol_clone_dir(parent) {
            let key = format!("{vol_name}/{name}");
            if self.clone_files.contains_key(&key) {
                return Some(Err("already exists".into()));
            }
            let virt = self.alloc_virt();
            self.set_status(virt, &OpStatus::Pending);
            self.clone_files.insert(key, virt);

            let status = self.do_clone(&vol_name, name).await;
            self.set_status(virt, &status);

            let size = self.file_size(virt).unwrap_or(0);
            return Some(Ok((GlobalIno::from_virtual(virt), size)));
        }

        None
    }

    // ── Operations ────────────────────────────────────────────────────

    async fn do_snapshot(&self, volume_name: &str, snapshot_name: &str) -> OpStatus {
        info!(volume = %volume_name, snapshot = %snapshot_name, "snapshot requested via .loophole/snapshots");
        match self.vm.snapshot(volume_name, snapshot_name).await {
            Ok(()) => OpStatus::Complete,
            Err(e) => OpStatus::Error {
                error: format!("{e:#}"),
            },
        }
    }

    async fn do_clone(&self, volume_name: &str, clone_name: &str) -> OpStatus {
        info!(volume = %volume_name, clone = %clone_name, "clone requested via .loophole/clones");
        match self.vm.clone_volume(volume_name, clone_name).await {
            Ok(()) => OpStatus::Complete,
            Err(e) => OpStatus::Error {
                error: format!("{e:#}"),
            },
        }
    }
}
