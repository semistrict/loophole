#[macro_use]
pub mod assert;

pub mod cache;
pub mod cache_repo;
pub mod ctl;
#[cfg(feature = "block-fuse")]
pub mod fs;
#[cfg(feature = "ext4-fuse")]
pub mod fs_ext4;
pub mod metrics;
pub mod s3;
pub mod store;
pub mod uploader;

#[cfg(feature = "lwext4")]
pub mod blockdev_adapter;
#[cfg(feature = "lwext4")]
pub mod lwext4_api;
#[cfg(feature = "nfs")]
pub mod nfs;

#[cfg(all(test, feature = "lwext4"))]
mod lwext4_no_fuse_tests;
#[cfg(all(test, feature = "lwext4"))]
mod lwext4_py_port_tests;
// NFS integration tests disabled — see src/nfs_tests.rs header comment for status.
// #[cfg(all(test, feature = "nfs"))]
// mod nfs_tests;
