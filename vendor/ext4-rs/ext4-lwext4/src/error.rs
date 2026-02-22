//! Error types for ext4-rs operations.

use std::ffi::NulError;
use thiserror::Error;

/// Error type for ext4 filesystem operations.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// File or directory not found
    #[error("not found: {0}")]
    NotFound(String),

    /// File or directory already exists
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Path is not a directory
    #[error("not a directory: {0}")]
    NotADirectory(String),

    /// Path is a directory (when file expected)
    #[error("is a directory: {0}")]
    IsADirectory(String),

    /// Directory is not empty
    #[error("directory not empty: {0}")]
    NotEmpty(String),

    /// Permission denied
    #[error("permission denied")]
    PermissionDenied,

    /// No space left on device
    #[error("no space left on device")]
    NoSpace,

    /// Filesystem is read-only
    #[error("read-only filesystem")]
    ReadOnly,

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Path contains null byte
    #[error("path contains null byte")]
    NulError(#[from] NulError),

    /// Filesystem-level error with errno
    #[error("filesystem error: {0}")]
    Filesystem(i32),

    /// Device not found or not registered
    #[error("device not found")]
    DeviceNotFound,

    /// Mount point not found
    #[error("mount point not found")]
    MountPointNotFound,

    /// Name too long
    #[error("name too long")]
    NameTooLong,

    /// Too many open files
    #[error("too many open files")]
    TooManyOpenFiles,

    /// Invalid filesystem
    #[error("invalid filesystem")]
    InvalidFilesystem,
}

/// Result type for ext4 filesystem operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Convert lwext4 error code to Error
impl From<i32> for Error {
    fn from(errno: i32) -> Self {
        match errno {
            0 => panic!("attempted to convert success (0) to error"),
            libc::ENOENT => Error::NotFound(String::new()),
            libc::EEXIST => Error::AlreadyExists(String::new()),
            libc::ENOTDIR => Error::NotADirectory(String::new()),
            libc::EISDIR => Error::IsADirectory(String::new()),
            libc::ENOTEMPTY => Error::NotEmpty(String::new()),
            libc::EACCES | libc::EPERM => Error::PermissionDenied,
            libc::ENOSPC => Error::NoSpace,
            libc::EROFS => Error::ReadOnly,
            libc::EINVAL => Error::InvalidArgument(String::new()),
            libc::ENXIO | libc::ENODEV => Error::DeviceNotFound,
            libc::ENAMETOOLONG => Error::NameTooLong,
            libc::EMFILE | libc::ENFILE => Error::TooManyOpenFiles,
            _ => Error::Filesystem(errno),
        }
    }
}

/// Check lwext4 return code and convert to Result
pub(crate) fn check_errno(errno: i32) -> Result<()> {
    if errno == 0 {
        Ok(())
    } else {
        Err(Error::from(errno))
    }
}

/// Check lwext4 return code with context for the error
pub(crate) fn check_errno_with_path(errno: i32, path: &str) -> Result<()> {
    if errno == 0 {
        Ok(())
    } else {
        let err = match errno {
            libc::ENOENT => Error::NotFound(path.to_string()),
            libc::EEXIST => Error::AlreadyExists(path.to_string()),
            libc::ENOTDIR => Error::NotADirectory(path.to_string()),
            libc::EISDIR => Error::IsADirectory(path.to_string()),
            libc::ENOTEMPTY => Error::NotEmpty(path.to_string()),
            _ => Error::from(errno),
        };
        Err(err)
    }
}
