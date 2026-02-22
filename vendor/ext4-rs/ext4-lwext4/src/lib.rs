//! A safe Rust wrapper for ext2/3/4 filesystem operations based on lwext4.
//!
//! This crate provides a high-level, idiomatic Rust API for working with ext2, ext3,
//! and ext4 filesystems. It wraps the lwext4 C library, providing memory safety and
//! ergonomic interfaces.
//!
//! # Features
//!
//! - Create ext2/3/4 filesystems with `mkfs`
//! - Mount and unmount filesystems
//! - File operations: create, read, write, truncate, seek
//! - Directory operations: create, remove, iterate
//! - Symbolic and hard links
//! - File metadata and permissions
//! - Block device abstraction for various backing stores
//!
//! # Example
//!
//! ```no_run
//! use ext4_lwext4::{mkfs, Ext4Fs, MkfsOptions, OpenFlags, FileBlockDevice};
//! use std::io::{Read, Write};
//!
//! // Create a disk image
//! let mut device = FileBlockDevice::create("disk.img", 100 * 1024 * 1024).unwrap();
//!
//! // Format as ext4
//! mkfs(device, &MkfsOptions::default()).unwrap();
//!
//! // Reopen and mount
//! let device = FileBlockDevice::open("disk.img").unwrap();
//! let fs = Ext4Fs::mount(device, false).unwrap();
//!
//! // Create a directory
//! fs.mkdir("/data", 0o755).unwrap();
//!
//! // Write a file
//! {
//!     let mut file = fs.open("/data/hello.txt", OpenFlags::CREATE | OpenFlags::WRITE).unwrap();
//!     file.write_all(b"Hello, ext4!").unwrap();
//! }
//!
//! // Read it back
//! {
//!     let mut file = fs.open("/data/hello.txt", OpenFlags::READ).unwrap();
//!     let mut content = String::new();
//!     file.read_to_string(&mut content).unwrap();
//!     println!("Content: {}", content);
//! }
//!
//! // List directory
//! for entry in fs.open_dir("/data").unwrap() {
//!     let entry = entry.unwrap();
//!     println!("{}: {:?}", entry.name(), entry.file_type());
//! }
//!
//! // Unmount
//! fs.umount().unwrap();
//! ```
//!
//! # Block Devices
//!
//! The crate provides several block device implementations:
//!
//! - [`FileBlockDevice`]: Backed by a file (disk image)
//! - [`MemoryBlockDevice`]: In-memory storage (for testing/embedded)
//!
//! You can implement the [`BlockDevice`] trait for custom storage backends.

pub mod blockdev;
pub mod dir;
pub mod error;
pub mod file;
pub mod fs;
pub mod mkfs;
pub mod types;

// Re-export main types at crate root
pub use blockdev::{BlockDevice, BlockDeviceExt, FileBlockDevice, MemoryBlockDevice};
pub use dir::{Dir, DirEntry};
pub use error::{Error, Result};
pub use file::File;
pub use fs::Ext4Fs;
pub use mkfs::{mkfs, MkfsOptions};
pub use types::{FileType, FsStats, FsType, Metadata, OpenFlags, SeekFrom};
