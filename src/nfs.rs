use crate::ctl::{self, Ctl};
use crate::store::{S3Access, Store};
use async_trait::async_trait;
use dashmap::DashMap;
use ext4_lwext4::{Error as Ext4Error, Ext4Fs, FileType as Ext4FileType, OpenFlags, SeekFrom};
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, set_gid3, set_mode3,
    set_size3, set_uid3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tracing::warn;

const EXT4_ROOT_INO: u64 = 2;

pub struct Ext4Nfs<S: S3Access> {
    fs: Mutex<Ext4Fs>,
    store: Arc<Store<S>>,
    path_to_ino: DashMap<String, u64>,
    ino_to_paths: DashMap<u64, BTreeSet<String>>,
    ctl: Arc<Ctl<S>>,
}

impl<S: S3Access> Ext4Nfs<S> {
    pub fn new(store: Arc<Store<S>>, fs: Ext4Fs) -> Self {
        let ctl = Arc::new(Ctl::new(Arc::clone(&store)));
        Self {
            store,
            fs: Mutex::new(fs),
            path_to_ino: DashMap::new(),
            ino_to_paths: DashMap::new(),
            ctl,
        }
    }

    fn virtual_file_attr(ino: u64, size: u64) -> fattr3 {
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: ino,
            atime: nfstime3::default(),
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
        }
    }

    fn virtual_dir_attr(ino: u64) -> fattr3 {
        fattr3 {
            ftype: ftype3::NF3DIR,
            mode: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            size: 0,
            used: 0,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: ino,
            atime: nfstime3::default(),
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
        }
    }

    fn ext4_err_to_nfsstat(err: &Ext4Error) -> nfsstat3 {
        match err {
            Ext4Error::NotFound(_) => nfsstat3::NFS3ERR_NOENT,
            Ext4Error::AlreadyExists(_) => nfsstat3::NFS3ERR_EXIST,
            Ext4Error::NotADirectory(_) => nfsstat3::NFS3ERR_NOTDIR,
            Ext4Error::IsADirectory(_) => nfsstat3::NFS3ERR_ISDIR,
            Ext4Error::NotEmpty(_) => nfsstat3::NFS3ERR_NOTEMPTY,
            Ext4Error::PermissionDenied => nfsstat3::NFS3ERR_ACCES,
            Ext4Error::NoSpace => nfsstat3::NFS3ERR_NOSPC,
            Ext4Error::ReadOnly => nfsstat3::NFS3ERR_ROFS,
            Ext4Error::NameTooLong => nfsstat3::NFS3ERR_NAMETOOLONG,
            _ => nfsstat3::NFS3ERR_IO,
        }
    }

    fn file_type(kind: Ext4FileType) -> ftype3 {
        match kind {
            Ext4FileType::RegularFile => ftype3::NF3REG,
            Ext4FileType::Directory => ftype3::NF3DIR,
            Ext4FileType::Symlink => ftype3::NF3LNK,
            Ext4FileType::BlockDevice => ftype3::NF3BLK,
            Ext4FileType::CharDevice => ftype3::NF3CHR,
            Ext4FileType::Fifo => ftype3::NF3FIFO,
            Ext4FileType::Socket => ftype3::NF3SOCK,
            Ext4FileType::Unknown => ftype3::NF3REG,
        }
    }

    fn metadata_to_fattr(ino: u64, md: &ext4_lwext4::Metadata) -> fattr3 {
        let ftype = Self::file_type(md.file_type);
        fattr3 {
            ftype,
            mode: md.mode,
            nlink: md.nlink,
            uid: md.uid,
            gid: md.gid,
            size: md.size,
            used: md.blocks * 512,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: ino,
            atime: nfstime3 {
                seconds: md.atime as u32,
                nseconds: 0,
            },
            mtime: nfstime3 {
                seconds: md.mtime as u32,
                nseconds: 0,
            },
            ctime: nfstime3 {
                seconds: md.ctime as u32,
                nseconds: 0,
            },
        }
    }

    fn track_path(&self, path: String, ino: u64) {
        if path == "/" || ino == EXT4_ROOT_INO {
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

    fn path_for_inode(&self, ino: u64) -> Option<String> {
        if ino == EXT4_ROOT_INO {
            return Some("/".to_string());
        }
        self.ino_to_paths
            .get(&ino)
            .and_then(|set| set.iter().next().cloned())
    }

    fn join_path(parent: &str, name: &str) -> String {
        if parent == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        }
    }

    fn lookup_child_inner(
        &self,
        parent_path: &str,
        name: &str,
    ) -> Result<Option<(u64, Ext4FileType)>, Ext4Error> {
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

    pub fn store(&self) -> &Arc<Store<S>> {
        &self.store
    }
}

#[async_trait]
impl<S: S3Access> NFSFileSystem for Ext4Nfs<S> {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    fn root_dir(&self) -> fileid3 {
        EXT4_ROOT_INO
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;

        if name == "." {
            return Ok(dirid);
        }

        // Virtual directory lookups: .loophole from root
        if dirid == EXT4_ROOT_INO && name == ".loophole" {
            return Ok(ctl::CTL_DIR_INO);
        }

        // ".." from .loophole → root
        if dirid == ctl::CTL_DIR_INO && name == ".." {
            return Ok(EXT4_ROOT_INO);
        }

        // Delegate to ctl for virtual dirs/files
        if let Some(ino) = self.ctl.lookup(dirid, name) {
            return Ok(ino);
        }

        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;

        if name == ".." {
            if parent_path == "/" {
                return Ok(EXT4_ROOT_INO);
            }
            let parent = parent_path
                .rfind('/')
                .map(|i| if i == 0 { "/" } else { &parent_path[..i] })
                .unwrap_or("/");
            return self
                .path_to_ino
                .get(parent)
                .map(|v| *v)
                .or(Some(EXT4_ROOT_INO))
                .ok_or(nfsstat3::NFS3ERR_NOENT);
        }

        let child = self
            .lookup_child_inner(&parent_path, name)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let full_path = Self::join_path(&parent_path, name);
        self.track_path(full_path, child.0);
        Ok(child.0)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        // Virtual directory attrs
        match id {
            ctl::CTL_DIR_INO | ctl::SNAPSHOTS_DIR_INO | ctl::CLONES_DIR_INO => {
                return Ok(Self::virtual_dir_attr(id));
            }
            _ => {}
        }

        // Virtual file attrs
        if let Some(size) = self.ctl.file_size(id) {
            return Ok(Self::virtual_file_attr(id, size));
        }

        let path = self.path_for_inode(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok(Self::metadata_to_fattr(id, &md))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if ctl::is_virtual_ino(id) {
            return self.getattr(id).await;
        }

        let path = self.path_for_inode(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;

        if let set_mode3::mode(mode) = setattr.mode {
            guard
                .set_permissions(&path, mode)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        }

        if matches!(setattr.uid, set_uid3::uid(_)) || matches!(setattr.gid, set_gid3::gid(_)) {
            let existing = guard
                .metadata(&path)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
            let uid = match setattr.uid {
                set_uid3::uid(u) => u,
                _ => existing.uid,
            };
            let gid = match setattr.gid {
                set_gid3::gid(g) => g,
                _ => existing.gid,
            };
            guard
                .set_owner(&path, uid, gid)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        }

        if let set_size3::size(size) = setattr.size {
            let mut file = guard
                .open(&path, OpenFlags::WRITE)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
            file.truncate(size)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        }

        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok(Self::metadata_to_fattr(id, &md))
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        // Virtual file reads via ctl
        if let Some(result) = self.ctl.read(id, offset, count) {
            return Ok(result);
        }

        let path = self.path_for_inode(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let mut file = guard
            .open(&path, OpenFlags::READ)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        let mut buf = vec![0u8; count as usize];
        let n = file
            .read(&mut buf)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        buf.truncate(n);
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        let eof = offset + n as u64 >= md.size;
        Ok((buf, eof))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        // Writes to virtual files are ignored (the create triggers the operation)
        if ctl::is_virtual_ino(id) {
            return self.getattr(id).await;
        }

        let path = self.path_for_inode(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let mut file = guard
            .open(&path, OpenFlags::WRITE)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        file.write_all(data)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        drop(file);
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok(Self::metadata_to_fattr(id, &md))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;

        // Creating a file under .loophole/snapshots/ or .loophole/clones/ triggers an operation
        if dirid == ctl::SNAPSHOTS_DIR_INO || dirid == ctl::CLONES_DIR_INO {
            // Sync ext4 and flush store before snapshot/clone
            {
                let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
                guard.sync().map_err(|_| nfsstat3::NFS3ERR_IO)?;
            }
            self.store.flush().await.map_err(|_| nfsstat3::NFS3ERR_IO)?;

            match self.ctl.create(dirid, name).await {
                Some(Ok((ino, size))) => {
                    return Ok((ino, Self::virtual_file_attr(ino, size)));
                }
                Some(Err(_)) => return Err(nfsstat3::NFS3ERR_EXIST),
                None => return Err(nfsstat3::NFS3ERR_ACCES),
            }
        }

        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let path = Self::join_path(&parent_path, name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        guard
            .open(&path, OpenFlags::CREATE | OpenFlags::WRITE)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        if let set_mode3::mode(mode) = attr.mode
            && let Err(e) = guard.set_permissions(&path, mode)
        {
            warn!(error = %e, path = %path, "failed to set mode during create");
        }

        let (ino, _) = self
            .lookup_child_inner(&parent_path, name)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?
            .ok_or(nfsstat3::NFS3ERR_IO)?;
        self.track_path(path.clone(), ino);
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok((ino, Self::metadata_to_fattr(ino, &md)))
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;

        // Exclusive create in snapshot/clone dirs — same as regular create
        if dirid == ctl::SNAPSHOTS_DIR_INO || dirid == ctl::CLONES_DIR_INO {
            let (ino, _) = self.create(dirid, filename, sattr3::default()).await?;
            return Ok(ino);
        }

        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let path = Self::join_path(&parent_path, name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;

        if guard.metadata(&path).is_ok() {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        guard
            .open(&path, OpenFlags::CREATE | OpenFlags::WRITE)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        let (ino, _) = self
            .lookup_child_inner(&parent_path, name)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?
            .ok_or(nfsstat3::NFS3ERR_IO)?;
        self.track_path(path, ino);
        Ok(ino)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if ctl::is_virtual_ino(dirid) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }

        let name = std::str::from_utf8(&dirname.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let path = Self::join_path(&parent_path, name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        guard
            .mkdir(&path, 0o755)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        let (ino, _) = self
            .lookup_child_inner(&parent_path, name)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?
            .ok_or(nfsstat3::NFS3ERR_IO)?;
        self.track_path(path.clone(), ino);
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok((ino, Self::metadata_to_fattr(ino, &md)))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        if ctl::is_virtual_ino(dirid) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }

        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let path = Self::join_path(&parent_path, name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;

        match guard.remove(&path) {
            Ok(()) => {
                self.untrack_path(&path);
                Ok(())
            }
            Err(Ext4Error::IsADirectory(_)) => {
                guard
                    .rmdir(&path)
                    .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
                self.untrack_path(&path);
                Ok(())
            }
            Err(e) => Err(Self::ext4_err_to_nfsstat(&e)),
        }
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        if ctl::is_virtual_ino(from_dirid) || ctl::is_virtual_ino(to_dirid) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }

        let from_name =
            std::str::from_utf8(&from_filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let to_name = std::str::from_utf8(&to_filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let from_parent = self
            .path_for_inode(from_dirid)
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        let to_parent = self
            .path_for_inode(to_dirid)
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        let from = Self::join_path(&from_parent, from_name);
        let to = Self::join_path(&to_parent, to_name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        guard
            .rename(&from, &to)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        if let Some((_, ino)) = self.path_to_ino.remove(&from) {
            self.untrack_single(&from, ino);
            self.track_path(to, ino);
        }

        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        // Virtual directory listings
        if dirid == ctl::CTL_DIR_INO {
            let entries = vec![
                DirEntry {
                    fileid: ctl::SNAPSHOTS_DIR_INO,
                    name: b"snapshots"[..].into(),
                    attr: Self::virtual_dir_attr(ctl::SNAPSHOTS_DIR_INO),
                },
                DirEntry {
                    fileid: ctl::CLONES_DIR_INO,
                    name: b"clones"[..].into(),
                    attr: Self::virtual_dir_attr(ctl::CLONES_DIR_INO),
                },
            ];
            return Ok(paginate_entries(entries, start_after, max_entries));
        }

        if let Some(ctl_entries) = self.ctl.readdir(dirid) {
            let entries: Vec<DirEntry> = ctl_entries
                .into_iter()
                .map(|e| {
                    let attr = if e.is_dir {
                        Self::virtual_dir_attr(e.ino)
                    } else {
                        let size = self.ctl.file_size(e.ino).unwrap_or(0);
                        Self::virtual_file_attr(e.ino, size)
                    };
                    DirEntry {
                        fileid: e.ino,
                        name: e.name.as_bytes().into(),
                        attr,
                    }
                })
                .collect();
            return Ok(paginate_entries(entries, start_after, max_entries));
        }

        let path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let mut dir = guard
            .open_dir(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        let mut all_entries = Vec::new();

        // Inject .loophole into root listing
        if dirid == EXT4_ROOT_INO {
            all_entries.push(DirEntry {
                fileid: ctl::CTL_DIR_INO,
                name: b".loophole"[..].into(),
                attr: Self::virtual_dir_attr(ctl::CTL_DIR_INO),
            });
        }

        for entry in &mut dir {
            let entry = entry.map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
            let name = entry.name().to_string();
            if name == "." || name == ".." {
                continue;
            }
            let full_path = Self::join_path(&path, &name);
            let ino = entry.inode();
            self.track_path(full_path.clone(), ino);
            let md = guard
                .metadata(&full_path)
                .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
            all_entries.push(DirEntry {
                fileid: ino,
                name: name.as_bytes().into(),
                attr: Self::metadata_to_fattr(ino, &md),
            });
        }

        Ok(paginate_entries(all_entries, start_after, max_entries))
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if ctl::is_virtual_ino(dirid) {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }

        let name = std::str::from_utf8(&linkname.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let target = std::str::from_utf8(&symlink.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let parent_path = self.path_for_inode(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let path = Self::join_path(&parent_path, name);

        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        guard
            .symlink(target, &path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;

        let (ino, _) = self
            .lookup_child_inner(&parent_path, name)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?
            .ok_or(nfsstat3::NFS3ERR_IO)?;
        self.track_path(path.clone(), ino);
        let md = guard
            .metadata(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok((ino, Self::metadata_to_fattr(ino, &md)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let path = self.path_for_inode(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let guard = self.fs.lock().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let target = guard
            .readlink(&path)
            .map_err(|e| Self::ext4_err_to_nfsstat(&e))?;
        Ok(target.as_bytes().into())
    }
}

fn paginate_entries(
    entries: Vec<DirEntry>,
    start_after: fileid3,
    max_entries: usize,
) -> ReadDirResult {
    let skip = if start_after > 0 {
        entries
            .iter()
            .position(|e| e.fileid == start_after)
            .map(|p| p + 1)
            .unwrap_or(0)
    } else {
        0
    };

    let remaining: Vec<DirEntry> = entries.into_iter().skip(skip).collect();
    let end = remaining.len() <= max_entries;
    let entries: Vec<DirEntry> = remaining.into_iter().take(max_entries).collect();

    ReadDirResult { entries, end }
}
