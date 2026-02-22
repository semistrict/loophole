//! Directory operations for ext4 filesystems.

use crate::error::{check_errno_with_path, Result};
use crate::fs::Ext4Fs;
use crate::types::FileType;
use ext4_lwext4_sys::{ext4_dir, ext4_dir_close, ext4_dir_entry_next, ext4_dir_entry_rewind, ext4_dir_open};

/// A directory handle for iterating over entries.
///
/// Directories are automatically closed when dropped.
pub struct Dir<'a> {
    #[allow(dead_code)]
    fs: &'a Ext4Fs,
    inner: ext4_dir,
}

impl<'a> Dir<'a> {
    /// Open a directory for iteration.
    pub(crate) fn open(fs: &'a Ext4Fs, path: &str) -> Result<Self> {
        let full_path = fs.make_path(path)?;
        let mut inner = ext4_dir::default();

        let ret = unsafe { ext4_dir_open(&mut inner, full_path.as_ptr()) };
        check_errno_with_path(ret, path)?;

        Ok(Self { fs, inner })
    }

    /// Get the next directory entry.
    pub fn next_entry(&mut self) -> Option<Result<DirEntry>> {
        let entry_ptr = unsafe { ext4_dir_entry_next(&mut self.inner) };

        if entry_ptr.is_null() {
            return None;
        }

        let entry = unsafe { &*entry_ptr };

        // Extract name (not null-terminated!)
        let name_len = entry.name_length as usize;
        let name_bytes = &entry.name[..name_len];
        let name = match String::from_utf8(name_bytes.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                // Try lossy conversion for non-UTF8 names
                String::from_utf8_lossy(name_bytes).into_owned()
            }
        };

        Some(Ok(DirEntry {
            name,
            inode: entry.inode as u64,
            file_type: FileType::from_raw(entry.inode_type),
        }))
    }

    /// Rewind to the beginning of the directory.
    pub fn rewind(&mut self) {
        unsafe {
            ext4_dir_entry_rewind(&mut self.inner);
        }
    }
}

impl Drop for Dir<'_> {
    fn drop(&mut self) {
        unsafe {
            ext4_dir_close(&mut self.inner);
        }
    }
}

impl<'a> Iterator for Dir<'a> {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

/// A directory entry.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Entry name
    pub name: String,
    /// Inode number
    pub inode: u64,
    /// File type
    pub file_type: FileType,
}

impl DirEntry {
    /// Get the entry name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the inode number.
    pub fn inode(&self) -> u64 {
        self.inode
    }

    /// Get the file type.
    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    /// Check if this is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type.is_file()
    }

    /// Check if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    /// Check if this is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.file_type.is_symlink()
    }
}
