use crate::rpc;
use crate::store::{S3Access, Store};
use dashmap::DashMap;
use ext4_lwext4::{Error as Ext4Error, Ext4Fs, FileType as Ext4FileType, OpenFlags, SeekFrom};
use fuser::{
    AccessFlags, BsdFileFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, OpenFlags as FuseOpenFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;
const CTL_DIR_INO: u64 = u64::MAX - 1;
const RPC_INO: u64 = u64::MAX;

pub struct Ext4Fuse<S: S3Access = aws_sdk_s3::Client> {
    store: Arc<Store<S>>,
    fs: Mutex<Ext4Fs>,
    rt: tokio::runtime::Handle,
    path_to_ino: DashMap<String, u64>,
    ino_to_paths: DashMap<u64, BTreeSet<String>>,
    rpc_handles: DashMap<u64, Vec<u8>>,
    next_fh: AtomicU64,
}

impl<S: S3Access> Ext4Fuse<S> {
    pub fn new(store: Arc<Store<S>>, fs: Ext4Fs) -> Self {
        Self {
            store,
            fs: Mutex::new(fs),
            rt: tokio::runtime::Handle::current(),
            path_to_ino: DashMap::new(),
            ino_to_paths: DashMap::new(),
            rpc_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
        }
    }

    fn ext4_err_to_errno(err: &Ext4Error) -> i32 {
        match err {
            Ext4Error::Io(ioe) => ioe.raw_os_error().unwrap_or(libc::EIO),
            Ext4Error::NotFound(_) => libc::ENOENT,
            Ext4Error::AlreadyExists(_) => libc::EEXIST,
            Ext4Error::NotADirectory(_) => libc::ENOTDIR,
            Ext4Error::IsADirectory(_) => libc::EISDIR,
            Ext4Error::NotEmpty(_) => libc::ENOTEMPTY,
            Ext4Error::PermissionDenied => libc::EACCES,
            Ext4Error::NoSpace => libc::ENOSPC,
            Ext4Error::ReadOnly => libc::EROFS,
            Ext4Error::InvalidArgument(_) => libc::EINVAL,
            Ext4Error::NulError(_) => libc::EINVAL,
            Ext4Error::Filesystem(errno) => *errno,
            Ext4Error::DeviceNotFound => libc::ENODEV,
            Ext4Error::MountPointNotFound => libc::ENOENT,
            Ext4Error::NameTooLong => libc::ENAMETOOLONG,
            Ext4Error::TooManyOpenFiles => libc::EMFILE,
            Ext4Error::InvalidFilesystem => libc::EIO,
        }
    }

    fn file_type(kind: Ext4FileType) -> FileType {
        match kind {
            Ext4FileType::RegularFile => FileType::RegularFile,
            Ext4FileType::Directory => FileType::Directory,
            Ext4FileType::Symlink => FileType::Symlink,
            Ext4FileType::BlockDevice => FileType::BlockDevice,
            Ext4FileType::CharDevice => FileType::CharDevice,
            Ext4FileType::Fifo => FileType::NamedPipe,
            Ext4FileType::Socket => FileType::Socket,
            Ext4FileType::Unknown => FileType::RegularFile,
        }
    }

    fn attr_from_md(&self, ino: u64, md: &ext4_lwext4::Metadata) -> FileAttr {
        let kind = Self::file_type(md.file_type);
        let secs_to_time = |secs: u64| UNIX_EPOCH + Duration::from_secs(secs);
        let blocks = if md.blocks > 0 {
            md.blocks
        } else {
            md.size.div_ceil(512)
        };
        FileAttr {
            ino: INodeNo(ino),
            size: md.size,
            blocks,
            atime: secs_to_time(md.atime),
            mtime: secs_to_time(md.mtime),
            ctime: secs_to_time(md.ctime),
            crtime: UNIX_EPOCH,
            kind,
            perm: md.mode.min(u16::MAX as u32) as u16,
            nlink: md.nlink,
            uid: md.uid,
            gid: md.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn rpc_attr(&self) -> FileAttr {
        FileAttr {
            ino: INodeNo(RPC_INO),
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o600,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn ctl_dir_attr(&self) -> FileAttr {
        FileAttr {
            ino: INodeNo(CTL_DIR_INO),
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn root_attr(&self) -> FileAttr {
        let lock = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return FileAttr {
                    ino: INodeNo(ROOT_INO),
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                };
            }
        };
        match lock.metadata("/") {
            Ok(md) => self.attr_from_md(ROOT_INO, &md),
            Err(_) => FileAttr {
                ino: INodeNo(ROOT_INO),
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            },
        }
    }

    fn join_path(parent: &str, name: &OsStr) -> Option<String> {
        let name = name.to_str()?;
        if name.is_empty() || name == "." {
            return Some(parent.to_string());
        }
        if name.contains('/') {
            return None;
        }
        if name == ".." {
            return Some(Self::parent_path(parent));
        }
        Some(if parent == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        })
    }

    fn parent_path(path: &str) -> String {
        if path == "/" {
            return "/".to_string();
        }
        let trimmed = path.trim_end_matches('/');
        if let Some(idx) = trimmed.rfind('/') {
            if idx == 0 {
                "/".to_string()
            } else {
                trimmed[..idx].to_string()
            }
        } else {
            "/".to_string()
        }
    }

    fn track_path(&self, path: String, ino: u64) {
        if path == "/" || ino == ROOT_INO || ino == CTL_DIR_INO || ino == RPC_INO {
            return;
        }
        if let Some(old_ino) = self.path_to_ino.insert(path.clone(), ino)
            && old_ino != ino
        {
            self.untrack_single(&path, old_ino);
        }
        let mut set = self.ino_to_paths.entry(ino).or_default();
        set.insert(path);
    }

    fn untrack_single(&self, path: &str, ino: u64) {
        if let Some(mut set) = self.ino_to_paths.get_mut(&ino) {
            set.remove(path);
            if set.is_empty() {
                drop(set);
                self.ino_to_paths.remove(&ino);
            }
        }
    }

    fn untrack_path(&self, path: &str) {
        if let Some((old_path, old_ino)) = self.path_to_ino.remove(path) {
            self.untrack_single(&old_path, old_ino);
        }
    }

    fn untrack_subtree(&self, prefix: &str) {
        let mut to_remove = Vec::new();
        for entry in &self.path_to_ino {
            let path = entry.key();
            if path == prefix || path.starts_with(&format!("{prefix}/")) {
                to_remove.push(path.clone());
            }
        }
        for path in to_remove {
            self.untrack_path(&path);
        }
    }

    fn rename_subtree_paths(&self, from: &str, to: &str) {
        let mut updates = Vec::new();
        for entry in &self.path_to_ino {
            let old = entry.key();
            if old == from || old.starts_with(&format!("{from}/")) {
                let suffix = if old == from { "" } else { &old[from.len()..] };
                let new_path = format!("{to}{suffix}");
                updates.push((old.clone(), new_path, *entry.value()));
            }
        }

        for (old, new_path, ino) in updates {
            self.path_to_ino.remove(&old);
            self.untrack_single(&old, ino);
            self.track_path(new_path, ino);
        }
    }

    fn path_for_inode(&self, ino: u64) -> Option<String> {
        if ino == ROOT_INO {
            return Some("/".to_string());
        }
        self.ino_to_paths
            .get(&ino)
            .and_then(|set| set.iter().next().cloned())
    }

    fn inode_for_path(&self, path: &str) -> Option<u64> {
        if path == "/" {
            return Some(ROOT_INO);
        }
        self.path_to_ino.get(path).map(|v| *v)
    }

    fn lookup_child(
        &self,
        parent_path: &str,
        name: &str,
    ) -> std::result::Result<Option<(u64, Ext4FileType)>, Ext4Error> {
        let guard = self
            .fs
            .lock()
            .map_err(|_| Ext4Error::Filesystem(libc::EIO))?;
        let mut dir = guard.open_dir(parent_path)?;
        for entry in &mut dir {
            let entry = entry?;
            if entry.name() == name {
                return Ok(Some((entry.inode(), entry.file_type())));
            }
        }
        Ok(None)
    }

    fn sync_underlying(&self) -> std::result::Result<(), i32> {
        {
            let guard = self.fs.lock().map_err(|_| libc::EIO)?;
            guard.sync().map_err(|e| Self::ext4_err_to_errno(&e))?;
        }
        self.rt.block_on(self.store.flush()).map_err(|_| libc::EIO)
    }

    fn rpc_response(ok: bool, error: Option<String>) -> Vec<u8> {
        serde_json::to_vec(&rpc::Response { ok, error }).unwrap_or_else(|_| {
            b"{\"ok\":false,\"error\":\"failed to encode RPC response\"}".to_vec()
        })
    }

    fn handle_rpc(&self, req_bytes: &[u8]) -> Vec<u8> {
        let req: rpc::Request = match serde_json::from_slice(req_bytes) {
            Ok(req) => req,
            Err(err) => return Self::rpc_response(false, Some(format!("{err:#}"))),
        };

        let result = match req {
            rpc::Request::Snapshot { new_store_id } => self
                .sync_underlying()
                .map_err(|errno| anyhow::anyhow!("sync failed with errno {errno}"))
                .and_then(|_| self.rt.block_on(self.store.snapshot(new_store_id))),
            rpc::Request::Clone {
                continuation_id,
                clone_id,
            } => self
                .sync_underlying()
                .map_err(|errno| anyhow::anyhow!("sync failed with errno {errno}"))
                .and_then(|_| {
                    self.rt
                        .block_on(self.store.clone_store(continuation_id, clone_id))
                }),
        };

        match result {
            Ok(()) => Self::rpc_response(true, None),
            Err(err) => Self::rpc_response(false, Some(format!("{err:#}"))),
        }
    }
}

impl<S: S3Access> Filesystem for Ext4Fuse<S> {
    fn destroy(&mut self) {
        if let Err(e) = self.rt.block_on(self.store.close()) {
            warn!(error = %e, "store close failed during FUSE destroy");
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match (parent.0, name.to_str()) {
            (ROOT_INO, Some(".loophole")) => {
                reply.entry(&TTL, &self.ctl_dir_attr(), Generation(0));
                return;
            }
            (CTL_DIR_INO, Some("rpc")) => {
                reply.entry(&TTL, &self.rpc_attr(), Generation(0));
                return;
            }
            _ => {}
        }

        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let full_path = match Self::join_path(&parent_path, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let child = match self.lookup_child(&parent_path, name_str) {
            Ok(Some(v)) => v,
            Ok(None) => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
            Err(err) => {
                reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                return;
            }
        };
        let child_ino = child.0;
        self.track_path(full_path.clone(), child_ino);

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.metadata(&full_path) {
            Ok(md) => reply.entry(&TTL, &self.attr_from_md(child_ino, &md), Generation(0)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match ino.0 {
            ROOT_INO => {
                reply.attr(&TTL, &self.root_attr());
                return;
            }
            CTL_DIR_INO => {
                reply.attr(&TTL, &self.ctl_dir_attr());
                return;
            }
            RPC_INO => {
                reply.attr(&TTL, &self.rpc_attr());
                return;
            }
            _ => {}
        }

        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };

        match guard.metadata(&path) {
            Ok(md) => reply.attr(&TTL, &self.attr_from_md(ino.0, &md)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };

        if let Some(mode) = mode
            && let Err(err) = guard.set_permissions(&path, mode)
        {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }

        if uid.is_some() || gid.is_some() {
            let existing = match guard.metadata(&path) {
                Ok(md) => md,
                Err(err) => {
                    reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                    return;
                }
            };
            if let Err(err) = guard.set_owner(
                &path,
                uid.unwrap_or(existing.uid),
                gid.unwrap_or(existing.gid),
            ) {
                reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                return;
            }
        }

        if let Some(size) = size {
            match guard.open(&path, OpenFlags::WRITE) {
                Ok(mut file) => {
                    if let Err(err) = file.truncate(size) {
                        reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                        return;
                    }
                }
                Err(err) => {
                    reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                    return;
                }
            }
        }

        match guard.metadata(&path) {
            Ok(md) => reply.attr(&TTL, &self.attr_from_md(ino.0, &md)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.readlink(&path) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let path = match Self::join_path(&parent_path, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        if let Err(err) = guard.mkdir(&path, mode) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        let ino = match self.lookup_child(&parent_path, name.to_str().unwrap_or_default()) {
            Ok(Some((ino, _))) => ino,
            _ => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        self.track_path(path.clone(), ino);
        match guard.metadata(&path) {
            Ok(md) => reply.entry(&TTL, &self.attr_from_md(ino, &md), Generation(0)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let path = match Self::join_path(&parent_path, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.remove(&path) {
            Ok(()) => {
                self.untrack_path(&path);
                reply.ok();
            }
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let path = match Self::join_path(&parent_path, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.rmdir(&path) {
            Ok(()) => {
                self.untrack_subtree(&path);
                reply.ok();
            }
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let path = match Self::join_path(&parent_path, link_name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let target = match target.to_str() {
            Some(t) => t,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        if let Err(err) = guard.symlink(target, &path) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        let ino = match self.lookup_child(&parent_path, link_name.to_str().unwrap_or_default()) {
            Ok(Some((ino, _))) => ino,
            _ => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        self.track_path(path.clone(), ino);
        match guard.metadata(&path) {
            Ok(md) => reply.entry(&TTL, &self.attr_from_md(ino, &md), Generation(0)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let from_parent = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let to_parent = match self.path_for_inode(newparent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let from = match Self::join_path(&from_parent, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let to = match Self::join_path(&to_parent, newname) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.rename(&from, &to) {
            Ok(()) => {
                self.rename_subtree_paths(&from, &to);
                reply.ok();
            }
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let source = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let new_parent_path = match self.path_for_inode(newparent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let dst = match Self::join_path(&new_parent_path, newname) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        if let Err(err) = guard.link(&source, &dst) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        self.track_path(dst.clone(), ino.0);
        match guard.metadata(&dst) {
            Ok(md) => reply.entry(&TTL, &self.attr_from_md(ino.0, &md), Generation(0)),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: FuseOpenFlags, reply: ReplyOpen) {
        if ino.0 == RPC_INO {
            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            self.rpc_handles.insert(fh, Vec::new());
            reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
            return;
        }
        if self.path_for_inode(ino.0).is_none() {
            reply.error(Errno::from_i32(libc::ENOENT));
            return;
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_path = match self.path_for_inode(parent.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let path = match Self::join_path(&parent_path, name) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        if let Err(err) = guard.open(&path, OpenFlags::CREATE | OpenFlags::WRITE) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        if let Err(err) = guard.set_permissions(&path, mode) {
            warn!(error = %err, path = %path, "failed to set mode during create");
        }

        let ino = match self.lookup_child(&parent_path, name.to_str().unwrap_or_default()) {
            Ok(Some((ino, _))) => ino,
            _ => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        self.track_path(path.clone(), ino);
        match guard.metadata(&path) {
            Ok(md) => reply.created(
                &TTL,
                &self.attr_from_md(ino, &md),
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            ),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: FuseOpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if ino.0 == RPC_INO {
            self.rpc_handles.remove(&fh.0);
        }
        reply.ok();
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: FuseOpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        if ino.0 == RPC_INO {
            let data = self
                .rpc_handles
                .get(&fh.0)
                .map(|v| v.clone())
                .unwrap_or_default();
            let start = (offset as usize).min(data.len());
            let end = (start + size as usize).min(data.len());
            reply.data(&data[start..end]);
            return;
        }

        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        let mut file = match guard.open(&path, OpenFlags::READ) {
            Ok(file) => file,
            Err(err) => {
                reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                return;
            }
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset)) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        let mut out = vec![0u8; size as usize];
        match file.read(&mut out) {
            Ok(n) => reply.data(&out[..n]),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: FuseOpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        if ino.0 == RPC_INO {
            let resp = self.handle_rpc(data);
            if let Some(mut entry) = self.rpc_handles.get_mut(&fh.0) {
                *entry = resp;
            }
            reply.written(data.len() as u32);
            return;
        }

        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        let mut file = match guard.open(&path, OpenFlags::WRITE) {
            Ok(file) => file,
            Err(err) => {
                reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                return;
            }
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset)) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        if let Err(err) = file.write_all(data) {
            reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
            return;
        }
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        if ino.0 == RPC_INO {
            reply.ok();
            return;
        }
        match self.sync_underlying() {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(Errno::from_i32(errno)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        if ino.0 == RPC_INO {
            reply.ok();
            return;
        }
        match self.sync_underlying() {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(Errno::from_i32(errno)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        if ino.0 == CTL_DIR_INO {
            let entries: [(u64, FileType, &str); 3] = [
                (CTL_DIR_INO, FileType::Directory, "."),
                (ROOT_INO, FileType::Directory, ".."),
                (RPC_INO, FileType::RegularFile, "rpc"),
            ];
            for (i, &(entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
                if reply.add(INodeNo(entry_ino), (i + 1) as u64, kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        let path = match self.path_for_inode(ino.0) {
            Some(path) => path,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };

        let mut entries: Vec<(u64, FileType, String)> = Vec::new();
        let parent_path = Self::parent_path(&path);
        let parent_ino = self.inode_for_path(&parent_path).unwrap_or(ROOT_INO);
        entries.push((ino.0, FileType::Directory, ".".to_string()));
        entries.push((parent_ino, FileType::Directory, "..".to_string()));

        if ino.0 == ROOT_INO {
            entries.push((CTL_DIR_INO, FileType::Directory, ".loophole".to_string()));
        }

        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        let mut dir = match guard.open_dir(&path) {
            Ok(dir) => dir,
            Err(err) => {
                reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                return;
            }
        };
        for entry in &mut dir {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err)));
                    return;
                }
            };
            let name = entry.name().to_string();
            if name == "." || name == ".." {
                continue;
            }
            let full_path = if path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", path, name)
            };
            self.track_path(full_path, entry.inode());
            entries.push((entry.inode(), Self::file_type(entry.file_type()), name));
        }

        for (i, (entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*entry_ino), (i + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let guard = match self.fs.lock() {
            Ok(guard) => guard,
            Err(_) => {
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        };
        match guard.stat() {
            Ok(stats) => reply.statfs(
                stats.total_blocks,
                stats.free_blocks,
                stats.free_blocks,
                stats.total_inodes,
                stats.free_inodes,
                stats.block_size,
                255,
                stats.block_size,
            ),
            Err(err) => reply.error(Errno::from_i32(Self::ext4_err_to_errno(&err))),
        }
    }

    fn access(&self, _req: &Request, ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        match ino.0 {
            ROOT_INO | CTL_DIR_INO | RPC_INO => {
                reply.ok();
                return;
            }
            _ => {}
        }
        if self.path_for_inode(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::from_i32(libc::ENOENT));
        }
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::from_i32(libc::ENODATA));
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::from_i32(libc::ENODATA));
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::from_i32(libc::EOPNOTSUPP));
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::from_i32(libc::EOPNOTSUPP));
    }
}
