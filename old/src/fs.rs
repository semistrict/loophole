use crate::ctl::Ctl;
use crate::inode::{GlobalIno, CTL_DIR_INO};
use crate::metrics::timing;
use crate::store::BlockStorage;
use crate::volume_manager::VolumeManager;
use dashmap::DashMap;
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, ReplyXattr, Request, WriteFlags,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tracing::{debug, warn};

const TTL: Duration = Duration::from_secs(1);

pub struct Loophole {
    vm: Arc<VolumeManager<aws_sdk_s3::Client>>,
    rt: tokio::runtime::Handle,
    ctl: Arc<Ctl<aws_sdk_s3::Client>>,
    /// Tracks which file handles are for virtual ctl files (ino stored per fh).
    ctl_fh: DashMap<u64, GlobalIno>,
    next_fh: AtomicU64,
}

impl Loophole {
    pub fn new(vm: Arc<VolumeManager<aws_sdk_s3::Client>>) -> Self {
        let rt = tokio::runtime::Handle::current();
        let ctl = Arc::new(Ctl::new(Arc::clone(&vm)));
        Self {
            vm,
            rt,
            ctl,
            ctl_fh: DashMap::new(),
            next_fh: AtomicU64::new(1),
        }
    }

    fn uid(&self) -> u32 {
        unsafe { libc::getuid() }
    }

    fn gid(&self) -> u32 {
        unsafe { libc::getgid() }
    }

    fn dir_attr(&self, ino: GlobalIno) -> FileAttr {
        FileAttr {
            ino: INodeNo(ino.raw()),
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.uid(),
            gid: self.gid(),
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn volume_file_attr(&self, ino: GlobalIno, volume_size: u64, block_size: u64) -> FileAttr {
        FileAttr {
            ino: INodeNo(ino.raw()),
            size: volume_size,
            blocks: volume_size / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o600,
            nlink: 1,
            uid: self.uid(),
            gid: self.gid(),
            rdev: 0,
            blksize: block_size.min(u32::MAX as u64) as u32,
            flags: 0,
        }
    }

    fn ctl_file_attr(&self, ino: GlobalIno) -> FileAttr {
        let virt = ino.to_virtual().unwrap();
        let size = self.ctl.file_size(virt).unwrap_or(0);
        FileAttr {
            ino: INodeNo(ino.raw()),
            size,
            blocks: size.div_ceil(512),
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: self.uid(),
            gid: self.gid(),
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

impl Filesystem for Loophole {
    fn destroy(&mut self) {
        if let Err(e) = self.rt.block_on(self.vm.close_all()) {
            warn!(error = %e, "store close failed during FUSE destroy");
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &std::ffi::OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let parent_ino = GlobalIno::from_raw(parent.0);

        // Volume file lookup from root
        if parent_ino.is_root() {
            if let Some(ino) = self.vm.get_ino(name_str) {
                if let Some((volume_size, block_size)) = self.vm.volume_info_by_ino(ino) {
                    reply.entry(
                        &TTL,
                        &self.volume_file_attr(ino, volume_size, block_size),
                        Generation(0),
                    );
                    return;
                }
            }
        }

        // .loophole from root
        if parent_ino.is_root() && name_str == ".loophole" {
            let ctl_global = GlobalIno::from_virtual(CTL_DIR_INO);
            reply.entry(&TTL, &self.dir_attr(ctl_global), Generation(0));
            return;
        }

        // ".." from .loophole → root
        if parent_ino.to_virtual() == Some(CTL_DIR_INO) && name_str == ".." {
            reply.entry(&TTL, &self.dir_attr(GlobalIno::ROOT), Generation(0));
            return;
        }

        // Delegate to ctl
        if let Some(virt_parent) = parent_ino.to_virtual() {
            if let Some(child_ino) = self.ctl.lookup(virt_parent, name_str) {
                if let Some(v) = child_ino.to_virtual() {
                    if self.ctl.is_ctl_dir(v) {
                        reply.entry(&TTL, &self.dir_attr(child_ino), Generation(0));
                    } else {
                        reply.entry(&TTL, &self.ctl_file_attr(child_ino), Generation(0));
                    }
                } else {
                    reply.entry(&TTL, &self.ctl_file_attr(child_ino), Generation(0));
                }
                return;
            }
        }

        reply.error(Errno::from_i32(libc::ENOENT));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let global = GlobalIno::from_raw(ino.0);

        if global.is_root() {
            reply.attr(&TTL, &self.dir_attr(GlobalIno::ROOT));
            return;
        }

        // Volume file
        if let Some((volume_size, block_size)) = self.vm.volume_info_by_ino(global) {
            reply.attr(
                &TTL,
                &self.volume_file_attr(global, volume_size, block_size),
            );
            return;
        }

        // Ctl directories and files
        if let Some(virt) = global.to_virtual() {
            if self.ctl.is_ctl_dir(virt) {
                reply.attr(&TTL, &self.dir_attr(global));
            } else {
                reply.attr(&TTL, &self.ctl_file_attr(global));
            }
            return;
        }

        reply.error(Errno::from_i32(libc::ENOENT));
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let global = GlobalIno::from_raw(ino.0);

        if global.is_root() {
            let mut entries: Vec<(GlobalIno, FileType, String)> = vec![
                (GlobalIno::ROOT, FileType::Directory, ".".into()),
                (GlobalIno::ROOT, FileType::Directory, "..".into()),
            ];

            // Add volume files
            for (name, ino, _volume_size) in self.vm.iter_volumes() {
                entries.push((ino, FileType::RegularFile, name));
            }

            // Add .loophole
            let ctl_global = GlobalIno::from_virtual(CTL_DIR_INO);
            entries.push((ctl_global, FileType::Directory, ".loophole".into()));

            for (i, (entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
                if reply.add(INodeNo(entry_ino.raw()), (i + 1) as u64, *kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        if let Some(virt) = global.to_virtual() {
            if let Some(entries) = self.ctl.readdir(virt) {
                let mut all: Vec<(GlobalIno, FileType, String)> = vec![
                    (global, FileType::Directory, ".".into()),
                    (
                        if virt == CTL_DIR_INO {
                            GlobalIno::ROOT
                        } else {
                            GlobalIno::from_virtual(CTL_DIR_INO)
                        },
                        FileType::Directory,
                        "..".into(),
                    ),
                ];
                for e in entries {
                    let kind = if e.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    all.push((e.ino, kind, e.name));
                }
                for (i, (entry_ino, kind, name)) in all.iter().enumerate().skip(offset as usize) {
                    if reply.add(INodeNo(entry_ino.raw()), (i + 1) as u64, *kind, name) {
                        break;
                    }
                }
                reply.ok();
                return;
            }
        }

        reply.error(Errno::from_i32(libc::ENOTDIR));
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let parent_ino = GlobalIno::from_raw(parent.0);

        // Ctl create: triggers snapshot/clone
        if let Some(virt_parent) = parent_ino.to_virtual() {
            match self.rt.block_on(self.ctl.create(virt_parent, name_str)) {
                Some(Ok((ino, _))) => {
                    reply.created(
                        &TTL,
                        &self.ctl_file_attr(ino),
                        Generation(0),
                        FileHandle(0),
                        FopenFlags::empty(),
                    );
                }
                Some(Err(_)) => reply.error(Errno::from_i32(libc::EEXIST)),
                None => reply.error(Errno::from_i32(libc::EACCES)),
            }
            return;
        }

        reply.error(Errno::from_i32(libc::EACCES));
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let global = GlobalIno::from_raw(ino.0);
        if global.is_virtual() {
            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            self.ctl_fh.insert(fh, global);
            reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
        } else {
            reply.opened(FileHandle(0), FopenFlags::empty());
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.ctl_fh.remove(&fh.0);
        reply.ok();
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let global = GlobalIno::from_raw(ino.0);

        // Virtual ctl file read
        if let Some(virt) = global.to_virtual() {
            if let Some((data, _eof)) = self.ctl.read(virt, offset, size) {
                reply.data(&data);
                return;
            }
        }

        let _timing = timing!("fuse.read");

        // Find volume by inode (lock-free)
        let store = match self.vm.get_store_by_ino(global) {
            Some((_name, store)) => store,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };

        let volume_size = store.state.volume_size;
        let block_size = store.state.block_size;

        if offset >= volume_size || size == 0 {
            reply.data(&[]);
            return;
        }

        let read_end = offset.saturating_add(size as u64).min(volume_size);
        let first_block = offset / block_size;
        let last_block = (read_end - 1) / block_size;

        debug!(offset, size, first_block, last_block, "read");

        let mut buf = Vec::with_capacity((read_end - offset) as usize);

        for block_idx in first_block..=last_block {
            let block_start = block_idx * block_size;
            let slice_start = offset.saturating_sub(block_start);
            let slice_end = (read_end - block_start).min(block_size);
            let len = (slice_end - slice_start) as usize;

            match self
                .rt
                .block_on(store.read_block(block_idx, slice_start, len))
            {
                Ok(bytes) => buf.extend_from_slice(&bytes),
                Err(e) => {
                    warn!(block = block_idx, error = %e, "read_block failed");
                    reply.error(Errno::from_i32(libc::EIO));
                    return;
                }
            }
        }

        reply.data(&buf);
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _timing = timing!("fuse.fsync");
        let global = GlobalIno::from_raw(ino.0);

        let store = match self.vm.get_store_by_ino(global) {
            Some((_name, store)) => store,
            None => {
                reply.ok();
                return;
            }
        };

        match self.rt.block_on(store.flush()) {
            Ok(()) => reply.ok(),
            Err(e) => {
                warn!(error = %e, "flush failed during fsync");
                reply.error(Errno::from_i32(libc::EIO));
            }
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _timing = timing!("fuse.flush");
        let global = GlobalIno::from_raw(ino.0);

        let store = match self.vm.get_store_by_ino(global) {
            Some((_name, store)) => store,
            None => {
                reply.ok();
                return;
            }
        };

        match self.rt.block_on(store.flush()) {
            Ok(()) => reply.ok(),
            Err(e) => {
                warn!(error = %e, "flush failed during flush");
                reply.error(Errno::from_i32(libc::EIO));
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let global = GlobalIno::from_raw(ino.0);

        // Writes to virtual ctl files are no-ops (create triggers the operation).
        if global.is_virtual() {
            reply.written(data.len() as u32);
            return;
        }

        let _timing = timing!("fuse.write");

        // Acquire volume lock (shared) + load store atomically.
        // Blocks if a snapshot/clone is in progress.
        let (store, _guard) = match self.rt.block_on(self.vm.write_lock_by_ino(global)) {
            Some(pair) => pair,
            None => {
                reply.error(Errno::from_i32(libc::ENOENT));
                return;
            }
        };

        if data.is_empty() {
            reply.written(0);
            return;
        }

        let volume_size = store.state.volume_size;
        let block_size = store.state.block_size;

        if offset >= volume_size {
            reply.error(Errno::from_i32(libc::EFBIG));
            return;
        }

        let max_len = (volume_size - offset) as usize;
        let accepted_len = data.len().min(max_len);
        let data = &data[..accepted_len];

        debug!(offset, len = data.len(), volume_size, "write");

        let first_block = offset / block_size;
        let write_end = offset + data.len() as u64;
        let last_block = (write_end - 1) / block_size;
        let mut data_pos = 0usize;

        for block_idx in first_block..=last_block {
            let block_start = block_idx * block_size;
            let write_start = offset.saturating_sub(block_start);
            let block_end = (write_end - block_start).min(block_size);
            let write_len = (block_end - write_start) as usize;

            let chunk = &data[data_pos..data_pos + write_len];
            data_pos += write_len;

            if let Err(e) = self
                .rt
                .block_on(store.write_block(block_idx, write_start, chunk))
            {
                warn!(block = block_idx, error = %e, "write_block failed");
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        }

        reply.written(data.len() as u32);
        // _guard dropped here — volume lock released.
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &std::ffi::OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::from_i32(libc::ENODATA));
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let _timing = timing!("fuse.fallocate");
        let global = GlobalIno::from_raw(ino.0);

        const FALLOC_FL_KEEP_SIZE: i32 = 1;
        const FALLOC_FL_PUNCH_HOLE: i32 = 2;
        if mode != (FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE) {
            reply.error(Errno::from_i32(libc::EOPNOTSUPP));
            return;
        }

        // Acquire volume lock (shared) for fallocate too.
        let (store, _guard) = match self.rt.block_on(self.vm.write_lock_by_ino(global)) {
            Some(pair) => pair,
            None => {
                reply.error(Errno::from_i32(libc::EINVAL));
                return;
            }
        };

        let block_size = store.state.block_size;
        let volume_size = store.state.volume_size;

        if offset >= volume_size || length == 0 {
            reply.ok();
            return;
        }

        let end = (offset + length).min(volume_size);

        debug!(offset, length, end, "fallocate punch hole");

        let first_block = offset / block_size;
        let last_block = (end - 1) / block_size;

        for block_idx in first_block..=last_block {
            let block_start = block_idx * block_size;
            let block_end = ((block_idx + 1) * block_size).min(volume_size);
            let punch_start = offset.max(block_start) - block_start;
            let punch_end = end.min(block_end) - block_start;

            if punch_start == 0 && punch_end == block_size {
                if let Err(e) = self.rt.block_on(store.zero_block(block_idx)) {
                    warn!(block = block_idx, error = %e, "zero_block failed");
                    reply.error(Errno::from_i32(libc::EIO));
                    return;
                }
            } else {
                let len = punch_end - punch_start;
                if let Err(e) = self
                    .rt
                    .block_on(store.zero_range(block_idx, punch_start, len))
                {
                    warn!(block = block_idx, error = %e, "zero_range failed during punch");
                    reply.error(Errno::from_i32(libc::EIO));
                    return;
                }
            }
        }

        reply.ok();
    }
}
