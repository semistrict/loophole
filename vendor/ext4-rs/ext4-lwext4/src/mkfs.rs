//! Filesystem creation (mkfs) functionality.

use crate::blockdev::{BlockDevice, BlockDeviceWrapper};
use crate::error::{check_errno, Result};
use crate::types::FsType;
use ext4_lwext4_sys::{ext4_fs, ext4_mkfs, ext4_mkfs_info, UUID_SIZE};
use std::ffi::CString;
use std::ptr;

/// Options for creating a new ext2/3/4 filesystem.
#[derive(Debug, Clone)]
pub struct MkfsOptions {
    /// Filesystem type (ext2, ext3, or ext4)
    pub fs_type: FsType,
    /// Block size in bytes (1024, 2048, or 4096)
    pub block_size: u32,
    /// Inode size in bytes (128 or 256)
    pub inode_size: u32,
    /// Enable journaling (ext3/ext4 only)
    pub journal: bool,
    /// Filesystem label (max 16 characters)
    pub label: Option<String>,
    /// UUID for the filesystem
    pub uuid: Option<[u8; 16]>,
}

impl Default for MkfsOptions {
    fn default() -> Self {
        Self {
            fs_type: FsType::Ext4,
            block_size: 4096,
            inode_size: 256,
            journal: true,
            label: None,
            uuid: None,
        }
    }
}

impl MkfsOptions {
    /// Create options for ext2 filesystem (no journal).
    pub fn ext2() -> Self {
        Self {
            fs_type: FsType::Ext2,
            journal: false,
            ..Default::default()
        }
    }

    /// Create options for ext3 filesystem.
    pub fn ext3() -> Self {
        Self {
            fs_type: FsType::Ext3,
            ..Default::default()
        }
    }

    /// Create options for ext4 filesystem.
    pub fn ext4() -> Self {
        Self::default()
    }

    /// Set the filesystem label.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the UUID.
    pub fn with_uuid(mut self, uuid: [u8; 16]) -> Self {
        self.uuid = Some(uuid);
        self
    }

    /// Set the block size.
    pub fn with_block_size(mut self, size: u32) -> Self {
        self.block_size = size;
        self
    }

    /// Enable or disable journaling.
    pub fn with_journal(mut self, enabled: bool) -> Self {
        self.journal = enabled;
        self
    }
}

/// Create a new ext2/3/4 filesystem on a block device.
///
/// # Arguments
/// * `device` - The block device to format
/// * `options` - Filesystem creation options
///
/// # Example
/// ```no_run
/// use ext4_lwext4::{mkfs, MkfsOptions, MemoryBlockDevice};
///
/// let device = MemoryBlockDevice::new(100 * 1024 * 1024, 512);
/// mkfs(device, &MkfsOptions::default()).unwrap();
/// ```
pub fn mkfs<B: BlockDevice + 'static>(device: B, options: &MkfsOptions) -> Result<()> {
    // Create the wrapper for the device
    let wrapper = BlockDeviceWrapper::new(device);

    // Get the raw pointer to the block device
    let bdev_ptr = wrapper.as_bdev_ptr();

    // Create the label CString if provided
    let label_cstring = options
        .label
        .as_ref()
        .map(|l| CString::new(l.as_str()).unwrap());

    // Prepare the mkfs info structure
    let mut info = ext4_mkfs_info {
        len: unsafe { (*bdev_ptr).part_size },
        block_size: options.block_size,
        blocks_per_group: 0, // Let lwext4 calculate
        inodes_per_group: 0, // Let lwext4 calculate
        inode_size: options.inode_size,
        inodes: 0, // Let lwext4 calculate
        journal_blocks: 0, // Let lwext4 calculate
        feat_ro_compat: 0,
        feat_compat: 0,
        feat_incompat: 0,
        bg_desc_reserve_blocks: 0,
        dsc_size: 0,
        uuid: options.uuid.unwrap_or([0u8; UUID_SIZE]),
        journal: options.journal && options.fs_type != FsType::Ext2,
        label: label_cstring
            .as_ref()
            .map(|c| c.as_ptr())
            .unwrap_or(ptr::null()),
    };

    // Allocate a zeroed ext4_fs struct (opaque, size known only to C)
    let fs_size = unsafe { ext4_lwext4_sys::lwext4_sizeof_ext4_fs() };
    let fs_buf = vec![0u8; fs_size];
    let fs_ptr = fs_buf.as_ptr() as *mut ext4_fs;

    // Call lwext4 mkfs
    let ret = unsafe {
        ext4_mkfs(
            fs_ptr,
            bdev_ptr,
            &mut info,
            options.fs_type.to_raw(),
        )
    };

    check_errno(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mkfs_options_builder() {
        let opts = MkfsOptions::ext4()
            .with_label("test_disk")
            .with_block_size(4096)
            .with_journal(true);

        assert_eq!(opts.fs_type, FsType::Ext4);
        assert_eq!(opts.label, Some("test_disk".to_string()));
        assert_eq!(opts.block_size, 4096);
        assert!(opts.journal);
    }
}
