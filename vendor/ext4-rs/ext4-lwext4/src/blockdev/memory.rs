//! In-memory block device implementation.

use crate::error::Result;
use crate::blockdev::traits::BlockDevice;

/// A block device backed by memory.
///
/// This is useful for testing and embedded scenarios where
/// no persistent storage is available.
pub struct MemoryBlockDevice {
    data: Vec<u8>,
    block_size: u32,
    block_count: u64,
}

impl MemoryBlockDevice {
    /// Create a new in-memory block device.
    ///
    /// # Arguments
    /// * `size` - Total size in bytes
    /// * `block_size` - Block size in bytes (typically 512 or 4096)
    ///
    /// # Returns
    /// A new MemoryBlockDevice
    pub fn new(size: u64, block_size: u32) -> Self {
        let block_count = size / block_size as u64;
        let actual_size = block_count * block_size as u64;

        Self {
            data: vec![0u8; actual_size as usize],
            block_size,
            block_count,
        }
    }

    /// Create a new in-memory block device with default block size (512 bytes).
    pub fn with_size(size: u64) -> Self {
        Self::new(size, 512)
    }

    /// Create a block device from existing data.
    ///
    /// # Arguments
    /// * `data` - The data to wrap
    /// * `block_size` - Block size in bytes
    ///
    /// # Panics
    /// Panics if data length is not aligned to block_size
    pub fn from_vec(data: Vec<u8>, block_size: u32) -> Self {
        assert!(
            data.len() % block_size as usize == 0,
            "data length must be aligned to block size"
        );

        let block_count = data.len() as u64 / block_size as u64;

        Self {
            data,
            block_size,
            block_count,
        }
    }

    /// Get the underlying data as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get the underlying data as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Consume the device and return the underlying data.
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

impl BlockDevice for MemoryBlockDevice {
    fn read_blocks(&self, block_id: u64, buf: &mut [u8]) -> Result<u32> {
        let offset = (block_id * self.block_size as u64) as usize;
        let len = buf.len();

        if offset + len > self.data.len() {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of device",
            )));
        }

        buf.copy_from_slice(&self.data[offset..offset + len]);
        Ok((len / self.block_size as usize) as u32)
    }

    fn write_blocks(&mut self, block_id: u64, buf: &[u8]) -> Result<u32> {
        let offset = (block_id * self.block_size as u64) as usize;
        let len = buf.len();

        if offset + len > self.data.len() {
            return Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "write past end of device",
            )));
        }

        self.data[offset..offset + len].copy_from_slice(buf);
        Ok((len / self.block_size as usize) as u32)
    }

    fn flush(&mut self) -> Result<()> {
        // No-op for memory device
        Ok(())
    }

    fn block_size(&self) -> u32 {
        self.block_size
    }

    fn block_count(&self) -> u64 {
        self.block_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_device_read_write() {
        let mut dev = MemoryBlockDevice::new(4096, 512);

        let write_buf = vec![0xAA; 512];
        dev.write_blocks(0, &write_buf).unwrap();

        let mut read_buf = vec![0u8; 512];
        dev.read_blocks(0, &mut read_buf).unwrap();

        assert_eq!(write_buf, read_buf);
    }

    #[test]
    fn test_memory_device_block_count() {
        let dev = MemoryBlockDevice::new(8192, 512);
        assert_eq!(dev.block_count(), 16);
        assert_eq!(dev.block_size(), 512);
    }
}
