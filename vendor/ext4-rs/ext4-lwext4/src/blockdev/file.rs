//! File-backed block device implementation.

use crate::error::{Error, Result};
use crate::blockdev::traits::BlockDevice;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

/// A block device backed by a file on the host filesystem.
///
/// This is useful for working with disk images or loopback devices.
pub struct FileBlockDevice {
    file: Mutex<File>,
    block_size: u32,
    block_count: u64,
}

impl FileBlockDevice {
    /// Create a new disk image file with the specified size.
    ///
    /// # Arguments
    /// * `path` - Path to create the file at
    /// * `size` - Total size in bytes
    /// * `block_size` - Block size (default 512)
    ///
    /// # Returns
    /// A new FileBlockDevice wrapping the created file
    pub fn create<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
        Self::create_with_block_size(path, size, 512)
    }

    /// Create a new disk image file with custom block size.
    pub fn create_with_block_size<P: AsRef<Path>>(
        path: P,
        size: u64,
        block_size: u32,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;

        // Set file size
        file.set_len(size)?;

        let block_count = size / block_size as u64;

        Ok(Self {
            file: Mutex::new(file),
            block_size,
            block_count,
        })
    }

    /// Open an existing disk image file.
    ///
    /// # Arguments
    /// * `path` - Path to the file
    ///
    /// # Returns
    /// A FileBlockDevice wrapping the file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_block_size(path, 512)
    }

    /// Open an existing disk image file with custom block size.
    pub fn open_with_block_size<P: AsRef<Path>>(path: P, block_size: u32) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let size = file.metadata()?.len();
        let block_count = size / block_size as u64;

        Ok(Self {
            file: Mutex::new(file),
            block_size,
            block_count,
        })
    }

    /// Open an existing disk image file in read-only mode.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_read_only_with_block_size(path, 512)
    }

    /// Open an existing disk image file in read-only mode with custom block size.
    pub fn open_read_only_with_block_size<P: AsRef<Path>>(
        path: P,
        block_size: u32,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path.as_ref())?;

        let size = file.metadata()?.len();
        let block_count = size / block_size as u64;

        Ok(Self {
            file: Mutex::new(file),
            block_size,
            block_count,
        })
    }
}

impl BlockDevice for FileBlockDevice {
    fn read_blocks(&self, block_id: u64, buf: &mut [u8]) -> Result<u32> {
        let offset = block_id * self.block_size as u64;
        let block_count = buf.len() as u32 / self.block_size;

        let mut file = self.file.lock().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "lock poisoned",
            ))
        })?;

        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;

        Ok(block_count)
    }

    fn write_blocks(&mut self, block_id: u64, buf: &[u8]) -> Result<u32> {
        let offset = block_id * self.block_size as u64;
        let block_count = buf.len() as u32 / self.block_size;

        let mut file = self.file.lock().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "lock poisoned",
            ))
        })?;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;

        Ok(block_count)
    }

    fn flush(&mut self) -> Result<()> {
        let file = self.file.lock().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "lock poisoned",
            ))
        })?;
        file.sync_all()?;
        Ok(())
    }

    fn block_size(&self) -> u32 {
        self.block_size
    }

    fn block_count(&self) -> u64 {
        self.block_count
    }
}
