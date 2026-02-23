//! Ext4 filesystem operations.

use crate::blockdev::{BlockDevice, BlockDeviceWrapper};
use crate::dir::Dir;
use crate::error::{check_errno, check_errno_with_path, Error, Result};
use crate::file::File;
use crate::types::{FileType, FsStats, Metadata, OpenFlags};
use ext4_lwext4_sys::{
    ext4_atime_get, ext4_cache_flush, ext4_cache_write_back, ext4_ctime_get, ext4_device_register,
    ext4_device_unregister, ext4_dir_mk, ext4_dir_rm, ext4_flink, ext4_fremove, ext4_frename,
    ext4_fsymlink, ext4_inode_exist, ext4_journal_start, ext4_journal_stop, ext4_mode_get,
    ext4_mode_set, ext4_mount, ext4_mount_point_stats, ext4_mount_stats, ext4_mtime_get,
    ext4_owner_get, ext4_owner_set, ext4_readlink, ext4_recover, ext4_umount,
};
use std::ffi::{c_char, CStr, CString};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

// Counter for generating unique device names
static DEVICE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// An ext4 filesystem instance.
///
/// This is the main entry point for filesystem operations. Create an instance
/// by mounting a block device, then use the various methods to manipulate files
/// and directories.
///
/// # Example
/// ```no_run
/// use ext4_lwext4::{Ext4Fs, FileBlockDevice, OpenFlags};
///
/// // Open a disk image and mount
/// let device = FileBlockDevice::open("disk.img").unwrap();
/// let fs = Ext4Fs::mount(device, false).unwrap();
///
/// // Create a directory
/// fs.mkdir("/data", 0o755).unwrap();
///
/// // Write a file (use a block to ensure file is dropped before umount)
/// {
///     let mut file = fs.open("/data/hello.txt", OpenFlags::CREATE | OpenFlags::WRITE).unwrap();
///     // ... write operations ...
/// }
///
/// // Unmount when done
/// fs.umount().unwrap();
/// ```
pub struct Ext4Fs {
    /// Wrapper holding the block device and C structures (kept alive for the C library)
    #[allow(dead_code)]
    wrapper: Pin<Box<BlockDeviceWrapper>>,
    /// Device name for lwext4
    device_name: CString,
    /// Mount point path
    mount_point: CString,
    /// Whether mounted read-only
    read_only: bool,
    /// Whether journal is active
    journal_active: bool,
}

impl Ext4Fs {
    /// Mount an ext4 filesystem from a block device.
    ///
    /// # Arguments
    /// * `device` - The block device containing the filesystem
    /// * `read_only` - Whether to mount read-only
    ///
    /// # Returns
    /// A mounted `Ext4Fs` instance
    pub fn mount<B: BlockDevice + 'static>(device: B, read_only: bool) -> Result<Self> {
        // Generate unique device and mount point names
        let id = DEVICE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let device_name = CString::new(format!("ext4dev{}", id)).unwrap();
        let mount_point = CString::new(format!("/mp{}/", id)).unwrap();

        // Create the wrapper
        let wrapper = BlockDeviceWrapper::new(device);

        // Register the device with lwext4
        let ret = unsafe { ext4_device_register(wrapper.as_bdev_ptr(), device_name.as_ptr()) };
        check_errno(ret)?;

        // Mount the filesystem
        let ret = unsafe { ext4_mount(device_name.as_ptr(), mount_point.as_ptr(), read_only) };
        if ret != 0 {
            // Unregister device on mount failure
            unsafe { ext4_device_unregister(device_name.as_ptr()) };
            return Err(Error::from(ret));
        }

        // Recover journal if needed
        let ret = unsafe { ext4_recover(mount_point.as_ptr()) };
        if ret != 0 {
            // Continue even if recovery fails - might not have journal
        }

        // Start journaling if not read-only
        let journal_active = if !read_only {
            let ret = unsafe { ext4_journal_start(mount_point.as_ptr()) };
            ret == 0
        } else {
            false
        };

        // Enable write-back cache mode for better performance.
        // Batches block writes instead of flushing each one immediately.
        if !read_only {
            unsafe { ext4_cache_write_back(mount_point.as_ptr(), true) };
        }

        Ok(Self {
            wrapper,
            device_name,
            mount_point,
            read_only,
            journal_active,
        })
    }

    /// Unmount the filesystem.
    ///
    /// This flushes all pending writes and releases the block device.
    pub fn umount(self) -> Result<()> {
        // Disable write-back cache before unmounting
        if !self.read_only {
            unsafe { ext4_cache_write_back(self.mount_point.as_ptr(), false) };
        }

        // Stop journaling if active
        if self.journal_active {
            unsafe { ext4_journal_stop(self.mount_point.as_ptr()) };
        }

        // Flush cache
        unsafe { ext4_cache_flush(self.mount_point.as_ptr()) };

        // Unmount
        let ret = unsafe { ext4_umount(self.mount_point.as_ptr()) };
        check_errno(ret)?;

        // Unregister device
        let ret = unsafe { ext4_device_unregister(self.device_name.as_ptr()) };
        check_errno(ret)?;

        Ok(())
    }

    /// Get the mount point path used internally.
    #[allow(dead_code)]
    pub(crate) fn mount_point(&self) -> &CStr {
        &self.mount_point
    }

    /// Create a full path by prepending the mount point.
    pub(crate) fn make_path(&self, path: &str) -> Result<CString> {
        // Remove leading slash from path if present
        let path = path.strip_prefix('/').unwrap_or(path);
        let mount_point = self.mount_point.to_str().map_err(|_| {
            Error::InvalidArgument("invalid mount point".to_string())
        })?;
        let full_path = format!("{}{}", mount_point, path);
        CString::new(full_path).map_err(Error::from)
    }

    /// Check if filesystem is mounted read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Get filesystem statistics.
    pub fn stat(&self) -> Result<FsStats> {
        let mut stats = ext4_mount_stats::default();
        let ret = unsafe { ext4_mount_point_stats(self.mount_point.as_ptr(), &mut stats) };
        check_errno(ret)?;

        // Extract volume name, handling null termination
        let volume_name = unsafe {
            let name_bytes = &stats.volume_name;
            let len = name_bytes.iter().position(|&c| c == 0).unwrap_or(16);
            let slice = std::slice::from_raw_parts(name_bytes.as_ptr() as *const u8, len);
            String::from_utf8_lossy(slice).into_owned()
        };

        Ok(FsStats {
            block_size: stats.block_size,
            total_blocks: stats.blocks_count,
            free_blocks: stats.free_blocks_count,
            total_inodes: stats.inodes_count as u64,
            free_inodes: stats.free_inodes_count as u64,
            block_group_count: stats.block_group_count,
            blocks_per_group: stats.blocks_per_group,
            inodes_per_group: stats.inodes_per_group,
            volume_name,
        })
    }

    /// Open a file.
    ///
    /// # Arguments
    /// * `path` - Path to the file
    /// * `flags` - Open flags (READ, WRITE, CREATE, etc.)
    pub fn open(&self, path: &str, flags: OpenFlags) -> Result<File<'_>> {
        File::open(self, path, flags)
    }

    /// Open a directory for iteration.
    pub fn open_dir(&self, path: &str) -> Result<Dir<'_>> {
        Dir::open(self, path)
    }

    /// Create a directory.
    ///
    /// # Arguments
    /// * `path` - Path for the new directory
    /// * `mode` - Permissions (e.g., 0o755)
    pub fn mkdir(&self, path: &str, mode: u32) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let full_path = self.make_path(path)?;
        let ret = unsafe { ext4_dir_mk(full_path.as_ptr()) };
        check_errno_with_path(ret, path)?;

        // Set permissions
        if mode != 0 {
            self.set_permissions(path, mode)?;
        }

        Ok(())
    }

    /// Remove a file.
    pub fn remove(&self, path: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let full_path = self.make_path(path)?;
        let ret = unsafe { ext4_fremove(full_path.as_ptr()) };
        check_errno_with_path(ret, path)
    }

    /// Remove a directory (recursively).
    pub fn rmdir(&self, path: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let full_path = self.make_path(path)?;
        let ret = unsafe { ext4_dir_rm(full_path.as_ptr()) };
        check_errno_with_path(ret, path)
    }

    /// Rename a file or directory.
    pub fn rename(&self, from: &str, to: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let from_path = self.make_path(from)?;
        let to_path = self.make_path(to)?;
        let ret = unsafe { ext4_frename(from_path.as_ptr(), to_path.as_ptr()) };
        check_errno_with_path(ret, from)
    }

    /// Create a hard link.
    pub fn link(&self, src: &str, dst: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let src_path = self.make_path(src)?;
        let dst_path = self.make_path(dst)?;
        let ret = unsafe { ext4_flink(src_path.as_ptr(), dst_path.as_ptr()) };
        check_errno_with_path(ret, src)
    }

    /// Create a symbolic link.
    ///
    /// # Arguments
    /// * `target` - The path the symlink points to
    /// * `path` - Path for the new symlink
    pub fn symlink(&self, target: &str, path: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let target_cstr = CString::new(target)?;
        let path_full = self.make_path(path)?;
        let ret = unsafe { ext4_fsymlink(target_cstr.as_ptr(), path_full.as_ptr()) };
        check_errno_with_path(ret, path)
    }

    /// Read the target of a symbolic link.
    pub fn readlink(&self, path: &str) -> Result<String> {
        let full_path = self.make_path(path)?;
        let mut buf = vec![0u8; 4096];
        let mut rcnt: usize = 0;

        let ret = unsafe {
            ext4_readlink(
                full_path.as_ptr(),
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                &mut rcnt,
            )
        };
        check_errno_with_path(ret, path)?;

        buf.truncate(rcnt);
        String::from_utf8(buf).map_err(|_| Error::InvalidArgument("invalid UTF-8 in symlink".to_string()))
    }

    /// Check if a path exists.
    pub fn exists(&self, path: &str) -> bool {
        self.metadata(path).is_ok()
    }

    /// Check if a path exists and is a file.
    pub fn is_file(&self, path: &str) -> bool {
        let full_path = match self.make_path(path) {
            Ok(p) => p,
            Err(_) => return false,
        };
        unsafe { ext4_inode_exist(full_path.as_ptr(), FileType::RegularFile.to_raw() as i32) == 0 }
    }

    /// Check if a path exists and is a directory.
    pub fn is_dir(&self, path: &str) -> bool {
        let full_path = match self.make_path(path) {
            Ok(p) => p,
            Err(_) => return false,
        };
        unsafe { ext4_inode_exist(full_path.as_ptr(), FileType::Directory.to_raw() as i32) == 0 }
    }

    /// Get file metadata.
    pub fn metadata(&self, path: &str) -> Result<Metadata> {
        let full_path = self.make_path(path)?;

        // Get mode to check existence
        let mut mode: u32 = 0;
        let ret = unsafe { ext4_mode_get(full_path.as_ptr(), &mut mode) };
        check_errno_with_path(ret, path)?;

        // Determine file type from mode
        let file_type = match mode & 0o170000 {
            0o100000 => FileType::RegularFile,
            0o040000 => FileType::Directory,
            0o120000 => FileType::Symlink,
            0o060000 => FileType::BlockDevice,
            0o020000 => FileType::CharDevice,
            0o010000 => FileType::Fifo,
            0o140000 => FileType::Socket,
            _ => FileType::Unknown,
        };

        // Get owner
        let mut uid: u32 = 0;
        let mut gid: u32 = 0;
        unsafe { ext4_owner_get(full_path.as_ptr(), &mut uid, &mut gid) };

        // Get timestamps
        let mut atime: u32 = 0;
        let mut mtime: u32 = 0;
        let mut ctime: u32 = 0;
        unsafe {
            ext4_atime_get(full_path.as_ptr(), &mut atime);
            ext4_mtime_get(full_path.as_ptr(), &mut mtime);
            ext4_ctime_get(full_path.as_ptr(), &mut ctime);
        }

        // For file size, we need to open the file temporarily
        let size = if file_type == FileType::RegularFile {
            if let Ok(file) = File::open(self, path, OpenFlags::READ) {
                file.size()
            } else {
                0
            }
        } else {
            0
        };

        Ok(Metadata {
            file_type,
            size,
            blocks: 0, // Not easily available without reading inode directly
            mode: mode & 0o7777, // Mask out file type bits
            uid,
            gid,
            atime: atime as u64,
            mtime: mtime as u64,
            ctime: ctime as u64,
            nlink: 1, // Not easily available
        })
    }

    /// Set file permissions.
    pub fn set_permissions(&self, path: &str, mode: u32) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let full_path = self.make_path(path)?;
        let ret = unsafe { ext4_mode_set(full_path.as_ptr(), mode) };
        check_errno_with_path(ret, path)
    }

    /// Set file owner.
    pub fn set_owner(&self, path: &str, uid: u32, gid: u32) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }

        let full_path = self.make_path(path)?;
        let ret = unsafe { ext4_owner_set(full_path.as_ptr(), uid, gid) };
        check_errno_with_path(ret, path)
    }

    /// Flush all pending writes to disk.
    pub fn sync(&self) -> Result<()> {
        let ret = unsafe { ext4_cache_flush(self.mount_point.as_ptr()) };
        check_errno(ret)
    }
}

impl Drop for Ext4Fs {
    fn drop(&mut self) {
        // Note: We can't return errors from drop, so we just try our best
        if !self.read_only {
            unsafe { ext4_cache_write_back(self.mount_point.as_ptr(), false) };
        }
        if self.journal_active {
            unsafe { ext4_journal_stop(self.mount_point.as_ptr()) };
        }
        unsafe {
            ext4_cache_flush(self.mount_point.as_ptr());
            ext4_umount(self.mount_point.as_ptr());
            ext4_device_unregister(self.device_name.as_ptr());
        }
    }
}
