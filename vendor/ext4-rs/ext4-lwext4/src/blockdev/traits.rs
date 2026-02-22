//! Block device trait definition.

use crate::error::Result;

/// Trait for block devices that can be used with ext4 filesystems.
///
/// Implementors must provide block-level read/write operations.
/// All operations are performed on aligned blocks.
pub trait BlockDevice: Send {
    /// Read blocks from the device.
    ///
    /// # Arguments
    /// * `block_id` - Starting block number
    /// * `buf` - Buffer to read into (must be block_size * block_count bytes)
    ///
    /// # Returns
    /// Number of blocks read, or error
    fn read_blocks(&self, block_id: u64, buf: &mut [u8]) -> Result<u32>;

    /// Write blocks to the device.
    ///
    /// # Arguments
    /// * `block_id` - Starting block number
    /// * `buf` - Buffer to write from (must be block_size * block_count bytes)
    ///
    /// # Returns
    /// Number of blocks written, or error
    fn write_blocks(&mut self, block_id: u64, buf: &[u8]) -> Result<u32>;

    /// Flush any pending writes to the device.
    fn flush(&mut self) -> Result<()>;

    /// Get the physical block size in bytes.
    fn block_size(&self) -> u32;

    /// Get the total number of blocks.
    fn block_count(&self) -> u64;

    /// Open the device (called before first I/O operation).
    ///
    /// Default implementation does nothing.
    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Close the device (called when unmounting).
    ///
    /// Default implementation does nothing.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Extension trait for BlockDevice with helper methods.
pub trait BlockDeviceExt: BlockDevice {
    /// Get the total size of the device in bytes.
    fn total_size(&self) -> u64 {
        self.block_count() * self.block_size() as u64
    }
}

impl<T: BlockDevice> BlockDeviceExt for T {}
