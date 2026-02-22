//! Block device abstraction layer.
//!
//! This module provides the [`BlockDevice`] trait and implementations for
//! various backing stores, as well as the internal bridge to lwext4's C API.

mod file;
mod memory;
mod traits;

pub use file::FileBlockDevice;
pub use memory::MemoryBlockDevice;
pub use traits::{BlockDevice, BlockDeviceExt};

use crate::error::Error;
use ext4_lwext4_sys::{ext4_blockdev, ext4_blockdev_iface};
use std::ffi::c_void;
use std::os::raw::c_int;
use std::pin::Pin;
use std::ptr;
use std::cell::UnsafeCell;

/// Internal wrapper that bridges a Rust BlockDevice to lwext4's C callbacks.
///
/// This structure holds the Rust device and provides the C-compatible
/// ext4_blockdev and ext4_blockdev_iface structures that lwext4 expects.
pub(crate) struct BlockDeviceWrapper {
    /// The underlying Rust block device (type-erased)
    device: UnsafeCell<Box<dyn BlockDevice>>,
    /// Physical block buffer for lwext4
    buffer: Pin<Box<[u8]>>,
    /// Block device interface with callbacks
    bdif: Pin<Box<ext4_blockdev_iface>>,
    /// Block device structure for lwext4
    bdev: Pin<Box<ext4_blockdev>>,
}

// Safety: We ensure thread safety through careful management of the wrapper
unsafe impl Send for BlockDeviceWrapper {}
unsafe impl Sync for BlockDeviceWrapper {}

impl BlockDeviceWrapper {
    /// Create a new wrapper for a BlockDevice.
    pub fn new<B: BlockDevice + 'static>(device: B) -> Pin<Box<Self>> {
        let block_size = device.block_size();
        let block_count = device.block_count();

        // Allocate buffer for physical block operations
        let buffer = vec![0u8; block_size as usize].into_boxed_slice();
        let buffer = Pin::new(buffer);

        // Create the block device interface with callbacks
        let bdif = Box::new(ext4_blockdev_iface {
            open: Some(blockdev_open),
            bread: Some(blockdev_bread),
            bwrite: Some(blockdev_bwrite),
            close: Some(blockdev_close),
            lock: Some(blockdev_lock),
            unlock: Some(blockdev_unlock),
            ph_bsize: block_size,
            ph_bcnt: block_count,
            ph_bbuf: ptr::null_mut(), // Will be set after Pin
            ph_refctr: 0,
            bread_ctr: 0,
            bwrite_ctr: 0,
            p_user: ptr::null_mut(), // Will be set after Pin
        });
        let bdif = Pin::new(bdif);

        // Create the block device structure
        let bdev = Box::new(ext4_blockdev {
            bdif: ptr::null_mut(), // Will be set after Pin
            part_offset: 0,
            part_size: block_count * block_size as u64,
            bc: ptr::null_mut(),
            lg_bsize: 0,
            lg_bcnt: 0,
            cache_write_back: 0,
            fs: ptr::null_mut(),
            journal: ptr::null_mut(),
        });
        let bdev = Pin::new(bdev);

        let wrapper = Box::new(Self {
            device: UnsafeCell::new(Box::new(device)),
            buffer,
            bdif,
            bdev,
        });

        let wrapper = Pin::new(wrapper);

        // Now fix up the pointers
        unsafe {
            let wrapper_ptr = &*wrapper as *const Self as *mut Self;

            // Set buffer pointer in bdif
            let bdif_ptr = &*wrapper.bdif as *const ext4_blockdev_iface as *mut ext4_blockdev_iface;
            (*bdif_ptr).ph_bbuf = wrapper.buffer.as_ptr() as *mut u8;
            (*bdif_ptr).p_user = wrapper_ptr as *mut c_void;

            // Set bdif pointer in bdev
            let bdev_ptr = &*wrapper.bdev as *const ext4_blockdev as *mut ext4_blockdev;
            (*bdev_ptr).bdif = bdif_ptr;
        }

        wrapper
    }

    /// Get a pointer to the ext4_blockdev structure for use with lwext4.
    pub fn as_bdev_ptr(&self) -> *mut ext4_blockdev {
        &*self.bdev as *const ext4_blockdev as *mut ext4_blockdev
    }

    /// Get a mutable reference to the underlying device.
    ///
    /// # Safety
    /// Caller must ensure no concurrent access to the device.
    #[allow(dead_code)]
    pub(crate) unsafe fn device_mut(&self) -> &mut dyn BlockDevice {
        unsafe { (*self.device.get()).as_mut() }
    }
}

// ============================================================================
// C callback trampolines
// ============================================================================

/// Get the BlockDeviceWrapper from a blockdev pointer
unsafe fn get_wrapper(bdev: *mut ext4_blockdev) -> &'static BlockDeviceWrapper {
    unsafe {
        let bdif = (*bdev).bdif;
        let wrapper_ptr = (*bdif).p_user as *const BlockDeviceWrapper;
        &*wrapper_ptr
    }
}

/// Open callback
unsafe extern "C" fn blockdev_open(bdev: *mut ext4_blockdev) -> c_int {
    unsafe {
        let wrapper = get_wrapper(bdev);
        let device = (*wrapper.device.get()).as_mut();

        match device.open() {
            Ok(()) => 0,
            Err(e) => match e {
                Error::Io(io) => io.raw_os_error().unwrap_or(libc::EIO),
                _ => libc::EIO,
            },
        }
    }
}

/// Block read callback
unsafe extern "C" fn blockdev_bread(
    bdev: *mut ext4_blockdev,
    buf: *mut c_void,
    blk_id: u64,
    blk_cnt: u32,
) -> c_int {
    unsafe {
        let wrapper = get_wrapper(bdev);
        let device = (*wrapper.device.get()).as_ref();
        let block_size = device.block_size() as usize;
        let total_size = block_size * blk_cnt as usize;

        let slice = std::slice::from_raw_parts_mut(buf as *mut u8, total_size);

        match device.read_blocks(blk_id, slice) {
            Ok(_) => 0,
            Err(e) => match e {
                Error::Io(io) => io.raw_os_error().unwrap_or(libc::EIO),
                _ => libc::EIO,
            },
        }
    }
}

/// Block write callback
unsafe extern "C" fn blockdev_bwrite(
    bdev: *mut ext4_blockdev,
    buf: *const c_void,
    blk_id: u64,
    blk_cnt: u32,
) -> c_int {
    unsafe {
        let wrapper = get_wrapper(bdev);
        let device = (*wrapper.device.get()).as_mut();
        let block_size = device.block_size() as usize;
        let total_size = block_size * blk_cnt as usize;

        let slice = std::slice::from_raw_parts(buf as *const u8, total_size);

        match device.write_blocks(blk_id, slice) {
            Ok(_) => 0,
            Err(e) => match e {
                Error::Io(io) => io.raw_os_error().unwrap_or(libc::EIO),
                _ => libc::EIO,
            },
        }
    }
}

/// Close callback
unsafe extern "C" fn blockdev_close(bdev: *mut ext4_blockdev) -> c_int {
    unsafe {
        let wrapper = get_wrapper(bdev);
        let device = (*wrapper.device.get()).as_mut();

        match device.close() {
            Ok(()) => 0,
            Err(e) => match e {
                Error::Io(io) => io.raw_os_error().unwrap_or(libc::EIO),
                _ => libc::EIO,
            },
        }
    }
}

/// Lock callback (no-op for single-threaded use)
unsafe extern "C" fn blockdev_lock(_bdev: *mut ext4_blockdev) -> c_int {
    0
}

/// Unlock callback (no-op for single-threaded use)
unsafe extern "C" fn blockdev_unlock(_bdev: *mut ext4_blockdev) -> c_int {
    0
}
