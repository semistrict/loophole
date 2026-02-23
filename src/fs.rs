use crate::ctl::{self, Ctl};
use crate::metrics::timing;
use crate::store::{BlockStorage, Store};
use dashmap::DashMap;
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, ReplyXattr, Request, WriteFlags,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{debug, warn};

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;
const FILE_INO: u64 = 2;

pub struct Fs {
    store: Arc<Store>,
    rt: tokio::runtime::Handle,
    ctl: Arc<Ctl<aws_sdk_s3::Client>>,
    /// Tracks which file handles are for virtual ctl files (ino stored per fh).
    ctl_fh: DashMap<u64, u64>,
    next_fh: AtomicU64,
}

impl Fs {
    pub fn new(store: Arc<Store>) -> Self {
        let rt = tokio::runtime::Handle::current();
        let ctl = Arc::new(Ctl::new(Arc::clone(&store)));
        Self {
            store,
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

    fn dir_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino: INodeNo(ino),
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

    fn file_attr(&self) -> FileAttr {
        FileAttr {
            ino: INodeNo(FILE_INO),
            size: self.store.state.volume_size,
            blocks: self.store.state.volume_size / 512,
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
            blksize: self.store.state.block_size.min(u32::MAX as u64) as u32,
            flags: 0,
        }
    }

    fn ctl_file_attr(&self, ino: u64) -> FileAttr {
        let size = self.ctl.file_size(ino).unwrap_or(0);
        FileAttr {
            ino: INodeNo(ino),
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

impl Filesystem for Fs {
    fn destroy(&mut self) {
        if let Err(e) = self.rt.block_on(self.store.close()) {
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

        // Volume file
        if parent.0 == ROOT_INO && name_str == "volume" {
            reply.entry(&TTL, &self.file_attr(), Generation(0));
            return;
        }

        // .loophole from root
        if parent.0 == ROOT_INO && name_str == ".loophole" {
            reply.entry(&TTL, &self.dir_attr(ctl::CTL_DIR_INO), Generation(0));
            return;
        }

        // ".." from .loophole → root
        if parent.0 == ctl::CTL_DIR_INO && name_str == ".." {
            reply.entry(&TTL, &self.dir_attr(ROOT_INO), Generation(0));
            return;
        }

        // Delegate to ctl
        if let Some(ino) = self.ctl.lookup(parent.0, name_str) {
            if ino == ctl::CTL_DIR_INO
                || ino == ctl::SNAPSHOTS_DIR_INO
                || ino == ctl::CLONES_DIR_INO
            {
                reply.entry(&TTL, &self.dir_attr(ino), Generation(0));
            } else {
                reply.entry(&TTL, &self.ctl_file_attr(ino), Generation(0));
            }
            return;
        }

        reply.error(Errno::from_i32(libc::ENOENT));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match ino.0 {
            ROOT_INO => reply.attr(&TTL, &self.dir_attr(ROOT_INO)),
            FILE_INO => reply.attr(&TTL, &self.file_attr()),
            i if i == ctl::CTL_DIR_INO
                || i == ctl::SNAPSHOTS_DIR_INO
                || i == ctl::CLONES_DIR_INO =>
            {
                reply.attr(&TTL, &self.dir_attr(i))
            }
            i if ctl::is_virtual_ino(i) => reply.attr(&TTL, &self.ctl_file_attr(i)),
            _ => reply.error(Errno::from_i32(libc::ENOENT)),
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
        if ino.0 == ROOT_INO {
            let entries: &[(u64, FileType, &str)] = &[
                (ROOT_INO, FileType::Directory, "."),
                (ROOT_INO, FileType::Directory, ".."),
                (FILE_INO, FileType::RegularFile, "volume"),
                (ctl::CTL_DIR_INO, FileType::Directory, ".loophole"),
            ];
            for (i, &(entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
                if reply.add(INodeNo(entry_ino), (i + 1) as u64, kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        if let Some(entries) = self.ctl.readdir(ino.0) {
            let mut all: Vec<(u64, FileType, String)> = vec![
                (ino.0, FileType::Directory, ".".into()),
                (
                    if ino.0 == ctl::CTL_DIR_INO {
                        ROOT_INO
                    } else {
                        ctl::CTL_DIR_INO
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
                if reply.add(INodeNo(*entry_ino), (i + 1) as u64, *kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
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

        // Flush store before snapshot/clone
        if parent.0 == ctl::SNAPSHOTS_DIR_INO || parent.0 == ctl::CLONES_DIR_INO {
            if let Err(e) = self.rt.block_on(self.store.flush()) {
                warn!(error = %e, "flush failed before ctl create");
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
            match self.rt.block_on(self.ctl.create(parent.0, name_str)) {
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
        if ctl::is_virtual_ino(ino.0) {
            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            self.ctl_fh.insert(fh, ino.0);
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
        // Virtual ctl file read
        if let Some((data, _eof)) = self.ctl.read(ino.0, offset, size) {
            reply.data(&data);
            return;
        }

        let _timing = timing!("fuse.read");

        if ino.0 != FILE_INO {
            reply.error(Errno::from_i32(libc::ENOENT));
            return;
        }

        let volume_size = self.store.state.volume_size;
        let block_size = self.store.state.block_size;

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
                .block_on(self.store.read_block(block_idx, slice_start, len))
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
        if ino.0 == FILE_INO {
            match self.rt.block_on(self.store.flush()) {
                Ok(()) => reply.ok(),
                Err(e) => {
                    warn!(error = %e, "flush failed during fsync");
                    reply.error(Errno::from_i32(libc::EIO));
                }
            }
        } else {
            reply.ok();
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
        if ino.0 == FILE_INO {
            match self.rt.block_on(self.store.flush()) {
                Ok(()) => reply.ok(),
                Err(e) => {
                    warn!(error = %e, "flush failed during flush");
                    reply.error(Errno::from_i32(libc::EIO));
                }
            }
        } else {
            reply.ok();
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
        // Writes to virtual ctl files are no-ops (create triggers the operation).
        if ctl::is_virtual_ino(ino.0) {
            reply.written(data.len() as u32);
            return;
        }

        let _timing = timing!("fuse.write");

        if ino.0 != FILE_INO {
            reply.error(Errno::from_i32(libc::ENOENT));
            return;
        }

        if data.is_empty() {
            reply.written(0);
            return;
        }

        let volume_size = self.store.state.volume_size;
        let block_size = self.store.state.block_size;

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
                .block_on(self.store.write_block(block_idx, write_start, chunk))
            {
                warn!(block = block_idx, error = %e, "write_block failed");
                reply.error(Errno::from_i32(libc::EIO));
                return;
            }
        }

        reply.written(data.len() as u32);
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
        const FALLOC_FL_KEEP_SIZE: i32 = 1;
        const FALLOC_FL_PUNCH_HOLE: i32 = 2;
        if mode != (FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE) {
            reply.error(Errno::from_i32(libc::EOPNOTSUPP));
            return;
        }

        if ino.0 != FILE_INO {
            reply.error(Errno::from_i32(libc::EINVAL));
            return;
        }

        let block_size = self.store.state.block_size;
        let volume_size = self.store.state.volume_size;

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
                if let Err(e) = self.rt.block_on(self.store.zero_block(block_idx)) {
                    warn!(block = block_idx, error = %e, "zero_block failed");
                    reply.error(Errno::from_i32(libc::EIO));
                    return;
                }
            } else {
                let len = punch_end - punch_start;
                if let Err(e) = self
                    .rt
                    .block_on(self.store.zero_range(block_idx, punch_start, len))
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
