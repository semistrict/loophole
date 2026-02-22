//! Common types used throughout ext4-rs.

use bitflags::bitflags;
use ext4_lwext4_sys;

/// Filesystem type for creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FsType {
    /// ext2 filesystem (no journaling)
    Ext2,
    /// ext3 filesystem (with journaling)
    Ext3,
    /// ext4 filesystem (with extents and journaling)
    #[default]
    Ext4,
}

impl FsType {
    /// Convert to lwext4 filesystem type constant
    pub(crate) fn to_raw(self) -> i32 {
        match self {
            FsType::Ext2 => ext4_lwext4_sys::F_SET_EXT2,
            FsType::Ext3 => ext4_lwext4_sys::F_SET_EXT3,
            FsType::Ext4 => ext4_lwext4_sys::F_SET_EXT4,
        }
    }
}

/// File type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Regular file
    RegularFile,
    /// Directory
    Directory,
    /// Symbolic link
    Symlink,
    /// Block device
    BlockDevice,
    /// Character device
    CharDevice,
    /// FIFO (named pipe)
    Fifo,
    /// Socket
    Socket,
    /// Unknown file type
    Unknown,
}

impl FileType {
    /// Convert from lwext4 directory entry type
    pub(crate) fn from_raw(raw: u8) -> Self {
        match raw {
            ext4_lwext4_sys::EXT4_DE_REG_FILE => FileType::RegularFile,
            ext4_lwext4_sys::EXT4_DE_DIR => FileType::Directory,
            ext4_lwext4_sys::EXT4_DE_SYMLINK => FileType::Symlink,
            ext4_lwext4_sys::EXT4_DE_BLKDEV => FileType::BlockDevice,
            ext4_lwext4_sys::EXT4_DE_CHRDEV => FileType::CharDevice,
            ext4_lwext4_sys::EXT4_DE_FIFO => FileType::Fifo,
            ext4_lwext4_sys::EXT4_DE_SOCK => FileType::Socket,
            _ => FileType::Unknown,
        }
    }

    /// Convert to lwext4 directory entry type
    pub(crate) fn to_raw(self) -> u8 {
        match self {
            FileType::RegularFile => ext4_lwext4_sys::EXT4_DE_REG_FILE,
            FileType::Directory => ext4_lwext4_sys::EXT4_DE_DIR,
            FileType::Symlink => ext4_lwext4_sys::EXT4_DE_SYMLINK,
            FileType::BlockDevice => ext4_lwext4_sys::EXT4_DE_BLKDEV,
            FileType::CharDevice => ext4_lwext4_sys::EXT4_DE_CHRDEV,
            FileType::Fifo => ext4_lwext4_sys::EXT4_DE_FIFO,
            FileType::Socket => ext4_lwext4_sys::EXT4_DE_SOCK,
            FileType::Unknown => ext4_lwext4_sys::EXT4_DE_UNKNOWN,
        }
    }

    /// Check if this is a regular file
    pub fn is_file(&self) -> bool {
        matches!(self, FileType::RegularFile)
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        matches!(self, FileType::Directory)
    }

    /// Check if this is a symbolic link
    pub fn is_symlink(&self) -> bool {
        matches!(self, FileType::Symlink)
    }
}

bitflags! {
    /// Flags for opening files.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct OpenFlags: u32 {
        /// Open for reading only
        const READ = 0b0000_0001;
        /// Open for writing only
        const WRITE = 0b0000_0010;
        /// Create file if it doesn't exist
        const CREATE = 0b0000_0100;
        /// Truncate file to zero length
        const TRUNCATE = 0b0000_1000;
        /// Append to end of file
        const APPEND = 0b0001_0000;
        /// Fail if file already exists (requires CREATE)
        const EXCLUSIVE = 0b0010_0000;
    }
}

impl OpenFlags {
    /// Convert to lwext4 open flags
    pub(crate) fn to_raw(self) -> i32 {
        let mut flags = 0i32;

        if self.contains(OpenFlags::READ) && self.contains(OpenFlags::WRITE) {
            flags |= ext4_lwext4_sys::O_RDWR;
        } else if self.contains(OpenFlags::WRITE) {
            flags |= ext4_lwext4_sys::O_WRONLY;
        } else {
            flags |= ext4_lwext4_sys::O_RDONLY;
        }

        if self.contains(OpenFlags::CREATE) {
            flags |= ext4_lwext4_sys::O_CREAT;
        }
        if self.contains(OpenFlags::TRUNCATE) {
            flags |= ext4_lwext4_sys::O_TRUNC;
        }
        if self.contains(OpenFlags::APPEND) {
            flags |= ext4_lwext4_sys::O_APPEND;
        }
        if self.contains(OpenFlags::EXCLUSIVE) {
            flags |= ext4_lwext4_sys::O_EXCL;
        }

        flags
    }
}

/// Seek position for file operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekFrom {
    /// Seek from the beginning of the file
    Start(u64),
    /// Seek from the end of the file
    End(i64),
    /// Seek from the current position
    Current(i64),
}

impl SeekFrom {
    /// Convert to lwext4 seek origin and offset
    pub(crate) fn to_raw(self) -> (i64, u32) {
        match self {
            SeekFrom::Start(pos) => (pos as i64, ext4_lwext4_sys::SEEK_SET),
            SeekFrom::End(pos) => (pos, ext4_lwext4_sys::SEEK_END),
            SeekFrom::Current(pos) => (pos, ext4_lwext4_sys::SEEK_CUR),
        }
    }
}

/// File metadata.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// File type
    pub file_type: FileType,
    /// File size in bytes
    pub size: u64,
    /// Number of blocks allocated
    pub blocks: u64,
    /// File mode (permissions)
    pub mode: u32,
    /// Owner user ID
    pub uid: u32,
    /// Owner group ID
    pub gid: u32,
    /// Access time (Unix timestamp)
    pub atime: u64,
    /// Modification time (Unix timestamp)
    pub mtime: u64,
    /// Change time (Unix timestamp)
    pub ctime: u64,
    /// Number of hard links
    pub nlink: u32,
}

impl Metadata {
    /// Check if this is a regular file
    pub fn is_file(&self) -> bool {
        self.file_type.is_file()
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    /// Check if this is a symbolic link
    pub fn is_symlink(&self) -> bool {
        self.file_type.is_symlink()
    }

    /// Get the file size
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Check if the file is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

/// Filesystem statistics.
#[derive(Debug, Clone)]
pub struct FsStats {
    /// Block size in bytes
    pub block_size: u32,
    /// Total number of blocks
    pub total_blocks: u64,
    /// Number of free blocks
    pub free_blocks: u64,
    /// Total number of inodes
    pub total_inodes: u64,
    /// Number of free inodes
    pub free_inodes: u64,
    /// Number of block groups
    pub block_group_count: u32,
    /// Blocks per group
    pub blocks_per_group: u32,
    /// Inodes per group
    pub inodes_per_group: u32,
    /// Volume name
    pub volume_name: String,
}

impl FsStats {
    /// Calculate total filesystem size in bytes
    pub fn total_size(&self) -> u64 {
        self.total_blocks * self.block_size as u64
    }

    /// Calculate free space in bytes
    pub fn free_size(&self) -> u64 {
        self.free_blocks * self.block_size as u64
    }

    /// Calculate used space in bytes
    pub fn used_size(&self) -> u64 {
        (self.total_blocks - self.free_blocks) * self.block_size as u64
    }
}
