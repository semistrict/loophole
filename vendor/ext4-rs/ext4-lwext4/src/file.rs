//! File operations for ext4 filesystems.

use crate::error::{check_errno, check_errno_with_path, Error, Result};
use crate::fs::Ext4Fs;
use crate::types::{Metadata, OpenFlags, SeekFrom};
use ext4_lwext4_sys::{
    ext4_fclose, ext4_file, ext4_fopen2, ext4_fread, ext4_fseek, ext4_fsize, ext4_ftell,
    ext4_ftruncate, ext4_fwrite,
};
use std::io::{self, Read, Seek, Write};
use std::os::raw::c_void;

/// A file handle for reading and writing.
///
/// Files are automatically closed when dropped.
pub struct File<'a> {
    fs: &'a Ext4Fs,
    inner: ext4_file,
    path: String,
}

impl<'a> File<'a> {
    /// Open a file with the specified flags.
    pub(crate) fn open(fs: &'a Ext4Fs, path: &str, flags: OpenFlags) -> Result<Self> {
        let full_path = fs.make_path(path)?;
        let mut inner = ext4_file::default();

        let ret = unsafe { ext4_fopen2(&mut inner, full_path.as_ptr(), flags.to_raw()) };
        check_errno_with_path(ret, path)?;

        Ok(Self {
            fs,
            inner,
            path: path.to_string(),
        })
    }

    /// Read data from the file.
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut rcnt: usize = 0;
        let ret = unsafe {
            ext4_fread(
                &mut self.inner,
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                &mut rcnt,
            )
        };
        check_errno(ret)?;
        Ok(rcnt)
    }

    /// Write data to the file.
    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut wcnt: usize = 0;
        let ret = unsafe {
            ext4_fwrite(
                &mut self.inner,
                buf.as_ptr() as *const c_void,
                buf.len(),
                &mut wcnt,
            )
        };
        check_errno(ret)?;
        Ok(wcnt)
    }

    /// Seek to a position in the file.
    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (offset, origin) = pos.to_raw();
        let ret = unsafe { ext4_fseek(&mut self.inner, offset, origin) };
        check_errno(ret)?;
        Ok(self.position())
    }

    /// Get the current position in the file.
    pub fn position(&self) -> u64 {
        unsafe { ext4_ftell(&self.inner as *const ext4_file as *mut ext4_file) }
    }

    /// Get the file size.
    pub fn size(&self) -> u64 {
        unsafe { ext4_fsize(&self.inner as *const ext4_file as *mut ext4_file) }
    }

    /// Truncate or extend the file to the specified size.
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        let ret = unsafe { ext4_ftruncate(&mut self.inner, size) };
        check_errno(ret)
    }

    /// Flush pending writes (no-op for ext4, writes go through cache).
    pub fn flush(&mut self) -> Result<()> {
        // lwext4 doesn't have per-file flush, it's all through the cache
        Ok(())
    }

    /// Sync the file to disk by flushing the filesystem cache.
    pub fn sync(&mut self) -> Result<()> {
        self.fs.sync()
    }

    /// Get file metadata.
    pub fn metadata(&self) -> Result<Metadata> {
        self.fs.metadata(&self.path)
    }

    /// Read the entire file contents into a vector.
    pub fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let size = self.size() as usize;
        buf.reserve(size);

        // Seek to beginning
        self.seek(SeekFrom::Start(0))?;

        // Read in chunks
        let mut total_read = 0;
        let mut chunk = vec![0u8; 8192];

        loop {
            let n = self.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            total_read += n;
        }

        Ok(total_read)
    }

    /// Read the entire file contents into a string.
    pub fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        let mut bytes = Vec::new();
        let n = self.read_to_end(&mut bytes)?;
        *buf = String::from_utf8(bytes)
            .map_err(|_| Error::InvalidArgument("file contains invalid UTF-8".to_string()))?;
        Ok(n)
    }

    /// Write all bytes to the file.
    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.write(&buf[written..])?;
            if n == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )));
            }
            written += n;
        }
        Ok(())
    }
}

impl Drop for File<'_> {
    fn drop(&mut self) {
        unsafe {
            ext4_fclose(&mut self.inner);
        }
    }
}

// Implement std::io traits for compatibility

impl Read for File<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        File::read(self, buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl Write for File<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        File::write(self, buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn flush(&mut self) -> io::Result<()> {
        File::flush(self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl Seek for File<'_> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            io::SeekFrom::Start(n) => SeekFrom::Start(n),
            io::SeekFrom::End(n) => SeekFrom::End(n),
            io::SeekFrom::Current(n) => SeekFrom::Current(n),
        };
        File::seek(self, pos).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
