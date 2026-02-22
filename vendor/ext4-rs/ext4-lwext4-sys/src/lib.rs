//! FFI bindings to lwext4 - a lightweight ext2/3/4 filesystem implementation.
//!
//! This crate provides low-level bindings to the lwext4 C library.
//! For a safe, high-level API, use the `ext4-rs` crate instead.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use std::os::raw::{c_char, c_int, c_void};

// ============================================================================
// File open flags (matching ext4_oflags.h with CONFIG_HAVE_OWN_OFLAGS=1)
// ============================================================================

pub const O_RDONLY: c_int = 0o0;
pub const O_WRONLY: c_int = 0o1;
pub const O_RDWR: c_int = 0o2;
pub const O_CREAT: c_int = 0o100;
pub const O_EXCL: c_int = 0o200;
pub const O_TRUNC: c_int = 0o1000;
pub const O_APPEND: c_int = 0o2000;

// Seek flags
pub const SEEK_SET: u32 = 0;
pub const SEEK_CUR: u32 = 1;
pub const SEEK_END: u32 = 2;

// ============================================================================
// Directory entry types (from ext4_types.h)
// ============================================================================

pub const EXT4_DE_UNKNOWN: u8 = 0;
pub const EXT4_DE_REG_FILE: u8 = 1;
pub const EXT4_DE_DIR: u8 = 2;
pub const EXT4_DE_CHRDEV: u8 = 3;
pub const EXT4_DE_BLKDEV: u8 = 4;
pub const EXT4_DE_FIFO: u8 = 5;
pub const EXT4_DE_SOCK: u8 = 6;
pub const EXT4_DE_SYMLINK: u8 = 7;

// ============================================================================
// Filesystem types for mkfs
// ============================================================================

pub const F_SET_EXT2: c_int = 2;
pub const F_SET_EXT3: c_int = 3;
pub const F_SET_EXT4: c_int = 4;

// ============================================================================
// UUID size
// ============================================================================

pub const UUID_SIZE: usize = 16;

// ============================================================================
// Block device interface (from ext4_blockdev.h)
// ============================================================================

/// Block device interface - defines callbacks for block I/O operations.
#[repr(C)]
pub struct ext4_blockdev_iface {
    /// Open device function
    pub open: Option<unsafe extern "C" fn(bdev: *mut ext4_blockdev) -> c_int>,

    /// Block read function
    pub bread: Option<
        unsafe extern "C" fn(
            bdev: *mut ext4_blockdev,
            buf: *mut c_void,
            blk_id: u64,
            blk_cnt: u32,
        ) -> c_int,
    >,

    /// Block write function
    pub bwrite: Option<
        unsafe extern "C" fn(
            bdev: *mut ext4_blockdev,
            buf: *const c_void,
            blk_id: u64,
            blk_cnt: u32,
        ) -> c_int,
    >,

    /// Close device function
    pub close: Option<unsafe extern "C" fn(bdev: *mut ext4_blockdev) -> c_int>,

    /// Lock block device (optional, required for multi-partition mode)
    pub lock: Option<unsafe extern "C" fn(bdev: *mut ext4_blockdev) -> c_int>,

    /// Unlock block device (optional, required for multi-partition mode)
    pub unlock: Option<unsafe extern "C" fn(bdev: *mut ext4_blockdev) -> c_int>,

    /// Physical block size in bytes
    pub ph_bsize: u32,

    /// Total physical block count
    pub ph_bcnt: u64,

    /// Physical block buffer
    pub ph_bbuf: *mut u8,

    /// Reference counter to block device interface
    pub ph_refctr: u32,

    /// Physical read counter
    pub bread_ctr: u32,

    /// Physical write counter
    pub bwrite_ctr: u32,

    /// User data pointer for driver context
    pub p_user: *mut c_void,
}

/// Block device structure
#[repr(C)]
pub struct ext4_blockdev {
    /// Block device interface
    pub bdif: *mut ext4_blockdev_iface,

    /// Partition offset in bdif (for multi-partition mode)
    pub part_offset: u64,

    /// Partition size in bdif (for multi-partition mode)
    pub part_size: u64,

    /// Block cache (opaque)
    pub bc: *mut c_void,

    /// Logical block size in bytes
    pub lg_bsize: u32,

    /// Logical block count
    pub lg_bcnt: u64,

    /// Cache write-back mode reference counter
    pub cache_write_back: u32,

    /// Associated filesystem (opaque)
    pub fs: *mut c_void,

    /// Journal reference (opaque)
    pub journal: *mut c_void,
}

// ============================================================================
// File descriptor (from ext4.h)
// ============================================================================

/// File descriptor structure
#[repr(C)]
pub struct ext4_file {
    /// Mount point handle (opaque)
    pub mp: *mut c_void,

    /// File inode ID
    pub inode: u32,

    /// Open flags
    pub flags: u32,

    /// File size
    pub fsize: u64,

    /// Current file position
    pub fpos: u64,
}

// ============================================================================
// Directory structures (from ext4.h)
// ============================================================================

/// Directory entry descriptor
#[repr(C)]
pub struct ext4_direntry {
    /// Inode number
    pub inode: u32,

    /// Distance to next entry
    pub entry_length: u16,

    /// Name length
    pub name_length: u8,

    /// Inode type (EXT4_DE_* constants)
    pub inode_type: u8,

    /// Entry name (not null-terminated, use name_length)
    pub name: [u8; 255],
}

/// Directory descriptor
#[repr(C)]
pub struct ext4_dir {
    /// File descriptor
    pub f: ext4_file,

    /// Current directory entry
    pub de: ext4_direntry,

    /// Next entry offset
    pub next_off: u64,
}

// ============================================================================
// Mount statistics (from ext4.h)
// ============================================================================

/// Filesystem mount statistics
#[repr(C)]
pub struct ext4_mount_stats {
    pub inodes_count: u32,
    pub free_inodes_count: u32,
    pub blocks_count: u64,
    pub free_blocks_count: u64,
    pub block_size: u32,
    pub block_group_count: u32,
    pub blocks_per_group: u32,
    pub inodes_per_group: u32,
    pub volume_name: [c_char; 16],
}

// ============================================================================
// mkfs info (from ext4_mkfs.h)
// ============================================================================

/// Filesystem creation options
#[repr(C)]
pub struct ext4_mkfs_info {
    /// Total filesystem length in bytes
    pub len: u64,

    /// Block size (1024, 2048, or 4096)
    pub block_size: u32,

    /// Blocks per group
    pub blocks_per_group: u32,

    /// Inodes per group
    pub inodes_per_group: u32,

    /// Inode size (128 or 256)
    pub inode_size: u32,

    /// Total number of inodes
    pub inodes: u32,

    /// Number of journal blocks
    pub journal_blocks: u32,

    /// Read-only compatible features
    pub feat_ro_compat: u32,

    /// Compatible features
    pub feat_compat: u32,

    /// Incompatible features
    pub feat_incompat: u32,

    /// Reserved blocks for group descriptors
    pub bg_desc_reserve_blocks: u32,

    /// Group descriptor size
    pub dsc_size: u16,

    /// UUID (16 bytes)
    pub uuid: [u8; UUID_SIZE],

    /// Enable journaling
    pub journal: bool,

    /// Filesystem label
    pub label: *const c_char,
}

// ============================================================================
// Filesystem structure (opaque, for mkfs)
// ============================================================================

/// Filesystem structure (opaque)
#[repr(C)]
pub struct ext4_fs {
    _private: [u8; 0],
}

// ============================================================================
// OS lock interface (from ext4.h)
// ============================================================================

/// OS-dependent lock interface
#[repr(C)]
pub struct ext4_lock {
    /// Lock access to mount point
    pub lock: Option<unsafe extern "C" fn()>,

    /// Unlock access to mount point
    pub unlock: Option<unsafe extern "C" fn()>,
}

// ============================================================================
// External C functions
// ============================================================================

unsafe extern "C" {
    // ========================================================================
    // Helper functions
    // ========================================================================

    /// Returns `sizeof(struct ext4_fs)` so Rust can allocate the opaque struct.
    pub fn lwext4_sizeof_ext4_fs() -> usize;

    // ========================================================================
    // Device registration
    // ========================================================================

    /// Register a block device with a name
    pub fn ext4_device_register(bd: *mut ext4_blockdev, dev_name: *const c_char) -> c_int;

    /// Unregister a block device by name
    pub fn ext4_device_unregister(dev_name: *const c_char) -> c_int;

    /// Unregister all block devices
    pub fn ext4_device_unregister_all() -> c_int;

    // ========================================================================
    // Mount operations
    // ========================================================================

    /// Mount a filesystem
    pub fn ext4_mount(
        dev_name: *const c_char,
        mount_point: *const c_char,
        read_only: bool,
    ) -> c_int;

    /// Unmount a filesystem
    pub fn ext4_umount(mount_point: *const c_char) -> c_int;

    /// Get mount point statistics
    pub fn ext4_mount_point_stats(
        mount_point: *const c_char,
        stats: *mut ext4_mount_stats,
    ) -> c_int;

    /// Setup OS lock routines for a mount point
    pub fn ext4_mount_setup_locks(mount_point: *const c_char, locks: *const ext4_lock) -> c_int;

    // ========================================================================
    // Journal operations
    // ========================================================================

    /// Start journaling
    pub fn ext4_journal_start(mount_point: *const c_char) -> c_int;

    /// Stop journaling
    pub fn ext4_journal_stop(mount_point: *const c_char) -> c_int;

    /// Recover journal (must be called after mount)
    pub fn ext4_recover(mount_point: *const c_char) -> c_int;

    // ========================================================================
    // Cache operations
    // ========================================================================

    /// Enable/disable write-back cache mode
    pub fn ext4_cache_write_back(path: *const c_char, on: bool) -> c_int;

    /// Flush cache
    pub fn ext4_cache_flush(path: *const c_char) -> c_int;

    // ========================================================================
    // File operations
    // ========================================================================

    /// Open a file (string flags like "r", "w", "a", etc.)
    pub fn ext4_fopen(file: *mut ext4_file, path: *const c_char, flags: *const c_char) -> c_int;

    /// Open a file (integer flags)
    pub fn ext4_fopen2(file: *mut ext4_file, path: *const c_char, flags: c_int) -> c_int;

    /// Close a file
    pub fn ext4_fclose(file: *mut ext4_file) -> c_int;

    /// Truncate a file
    pub fn ext4_ftruncate(file: *mut ext4_file, size: u64) -> c_int;

    /// Read from a file
    pub fn ext4_fread(
        file: *mut ext4_file,
        buf: *mut c_void,
        size: usize,
        rcnt: *mut usize,
    ) -> c_int;

    /// Write to a file
    pub fn ext4_fwrite(
        file: *mut ext4_file,
        buf: *const c_void,
        size: usize,
        wcnt: *mut usize,
    ) -> c_int;

    /// Seek in a file
    pub fn ext4_fseek(file: *mut ext4_file, offset: i64, origin: u32) -> c_int;

    /// Get current file position
    pub fn ext4_ftell(file: *mut ext4_file) -> u64;

    /// Get file size
    pub fn ext4_fsize(file: *mut ext4_file) -> u64;

    // ========================================================================
    // File management
    // ========================================================================

    /// Remove a file
    pub fn ext4_fremove(path: *const c_char) -> c_int;

    /// Rename a file
    pub fn ext4_frename(path: *const c_char, new_path: *const c_char) -> c_int;

    /// Create a hard link
    pub fn ext4_flink(path: *const c_char, hardlink_path: *const c_char) -> c_int;

    // ========================================================================
    // Symbolic links
    // ========================================================================

    /// Create a symbolic link
    pub fn ext4_fsymlink(target: *const c_char, path: *const c_char) -> c_int;

    /// Read a symbolic link
    pub fn ext4_readlink(
        path: *const c_char,
        buf: *mut c_char,
        bufsize: usize,
        rcnt: *mut usize,
    ) -> c_int;

    // ========================================================================
    // Directory operations
    // ========================================================================

    /// Create a directory
    pub fn ext4_dir_mk(path: *const c_char) -> c_int;

    /// Remove a directory (recursive)
    pub fn ext4_dir_rm(path: *const c_char) -> c_int;

    /// Rename/move a directory
    pub fn ext4_dir_mv(path: *const c_char, new_path: *const c_char) -> c_int;

    /// Open a directory
    pub fn ext4_dir_open(dir: *mut ext4_dir, path: *const c_char) -> c_int;

    /// Close a directory
    pub fn ext4_dir_close(dir: *mut ext4_dir) -> c_int;

    /// Get next directory entry (returns NULL if no more entries)
    pub fn ext4_dir_entry_next(dir: *mut ext4_dir) -> *const ext4_direntry;

    /// Rewind directory entry offset
    pub fn ext4_dir_entry_rewind(dir: *mut ext4_dir);

    // ========================================================================
    // Metadata operations
    // ========================================================================

    /// Get file mode (permissions)
    pub fn ext4_mode_get(path: *const c_char, mode: *mut u32) -> c_int;

    /// Set file mode (permissions)
    pub fn ext4_mode_set(path: *const c_char, mode: u32) -> c_int;

    /// Get file owner
    pub fn ext4_owner_get(path: *const c_char, uid: *mut u32, gid: *mut u32) -> c_int;

    /// Set file owner
    pub fn ext4_owner_set(path: *const c_char, uid: u32, gid: u32) -> c_int;

    /// Get access time
    pub fn ext4_atime_get(path: *const c_char, atime: *mut u32) -> c_int;

    /// Set access time
    pub fn ext4_atime_set(path: *const c_char, atime: u32) -> c_int;

    /// Get modification time
    pub fn ext4_mtime_get(path: *const c_char, mtime: *mut u32) -> c_int;

    /// Set modification time
    pub fn ext4_mtime_set(path: *const c_char, mtime: u32) -> c_int;

    /// Get change time
    pub fn ext4_ctime_get(path: *const c_char, ctime: *mut u32) -> c_int;

    /// Set change time
    pub fn ext4_ctime_set(path: *const c_char, ctime: u32) -> c_int;

    // ========================================================================
    // Inode operations
    // ========================================================================

    /// Check if inode exists
    pub fn ext4_inode_exist(path: *const c_char, inode_type: c_int) -> c_int;

    /// Create a special file (device node, fifo, socket)
    pub fn ext4_mknod(path: *const c_char, filetype: c_int, dev: u32) -> c_int;

    // ========================================================================
    // Extended attributes
    // ========================================================================

    /// Set extended attribute
    pub fn ext4_setxattr(
        path: *const c_char,
        name: *const c_char,
        name_len: usize,
        data: *const c_void,
        data_size: usize,
    ) -> c_int;

    /// Get extended attribute
    pub fn ext4_getxattr(
        path: *const c_char,
        name: *const c_char,
        name_len: usize,
        buf: *mut c_void,
        buf_size: usize,
        data_size: *mut usize,
    ) -> c_int;

    /// List extended attributes
    pub fn ext4_listxattr(
        path: *const c_char,
        list: *mut c_char,
        size: usize,
        ret_size: *mut usize,
    ) -> c_int;

    /// Remove extended attribute
    pub fn ext4_removexattr(path: *const c_char, name: *const c_char, name_len: usize) -> c_int;

    // ========================================================================
    // mkfs
    // ========================================================================

    /// Create a filesystem
    pub fn ext4_mkfs(
        fs: *mut ext4_fs,
        bd: *mut ext4_blockdev,
        info: *mut ext4_mkfs_info,
        fs_type: c_int,
    ) -> c_int;

    /// Read filesystem info from existing filesystem
    pub fn ext4_mkfs_read_info(bd: *mut ext4_blockdev, info: *mut ext4_mkfs_info) -> c_int;

    // ========================================================================
    // Block device operations (low-level)
    // ========================================================================

    /// Initialize block device
    pub fn ext4_block_init(bdev: *mut ext4_blockdev) -> c_int;

    /// Finalize block device
    pub fn ext4_block_fini(bdev: *mut ext4_blockdev) -> c_int;

    /// Flush block cache
    pub fn ext4_block_cache_flush(bdev: *mut ext4_blockdev) -> c_int;

    /// Set logical block size
    pub fn ext4_block_set_lb_size(bdev: *mut ext4_blockdev, lb_size: u32);
}

// ============================================================================
// Helper implementations
// ============================================================================

impl Default for ext4_blockdev_iface {
    fn default() -> Self {
        Self {
            open: None,
            bread: None,
            bwrite: None,
            close: None,
            lock: None,
            unlock: None,
            ph_bsize: 0,
            ph_bcnt: 0,
            ph_bbuf: std::ptr::null_mut(),
            ph_refctr: 0,
            bread_ctr: 0,
            bwrite_ctr: 0,
            p_user: std::ptr::null_mut(),
        }
    }
}

impl Default for ext4_blockdev {
    fn default() -> Self {
        Self {
            bdif: std::ptr::null_mut(),
            part_offset: 0,
            part_size: 0,
            bc: std::ptr::null_mut(),
            lg_bsize: 0,
            lg_bcnt: 0,
            cache_write_back: 0,
            fs: std::ptr::null_mut(),
            journal: std::ptr::null_mut(),
        }
    }
}

impl Default for ext4_file {
    fn default() -> Self {
        Self {
            mp: std::ptr::null_mut(),
            inode: 0,
            flags: 0,
            fsize: 0,
            fpos: 0,
        }
    }
}

impl Default for ext4_direntry {
    fn default() -> Self {
        Self {
            inode: 0,
            entry_length: 0,
            name_length: 0,
            inode_type: 0,
            name: [0; 255],
        }
    }
}

impl Default for ext4_dir {
    fn default() -> Self {
        Self {
            f: ext4_file::default(),
            de: ext4_direntry::default(),
            next_off: 0,
        }
    }
}

impl Default for ext4_mount_stats {
    fn default() -> Self {
        Self {
            inodes_count: 0,
            free_inodes_count: 0,
            blocks_count: 0,
            free_blocks_count: 0,
            block_size: 0,
            block_group_count: 0,
            blocks_per_group: 0,
            inodes_per_group: 0,
            volume_name: [0; 16],
        }
    }
}

impl Default for ext4_mkfs_info {
    fn default() -> Self {
        Self {
            len: 0,
            block_size: 4096,
            blocks_per_group: 0,
            inodes_per_group: 0,
            inode_size: 256,
            inodes: 0,
            journal_blocks: 0,
            feat_ro_compat: 0,
            feat_compat: 0,
            feat_incompat: 0,
            bg_desc_reserve_blocks: 0,
            dsc_size: 0,
            uuid: [0; UUID_SIZE],
            journal: true,
            label: std::ptr::null(),
        }
    }
}

// Safety: These types are used for FFI and are Send/Sync when properly synchronized
unsafe impl Send for ext4_blockdev_iface {}
unsafe impl Sync for ext4_blockdev_iface {}
unsafe impl Send for ext4_blockdev {}
unsafe impl Sync for ext4_blockdev {}
unsafe impl Send for ext4_file {}
unsafe impl Send for ext4_dir {}
