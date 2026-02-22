# ext4-lwext4

A safe Rust wrapper for ext2/3/4 filesystem operations based on [lwext4](https://github.com/gkostka/lwext4).

[![Crates.io](https://img.shields.io/crates/v/ext4-lwext4.svg)](https://crates.io/crates/ext4-lwext4)
[![Documentation](https://docs.rs/ext4-lwext4/badge.svg)](https://docs.rs/ext4-lwext4)
[![License](https://img.shields.io/crates/l/ext4-lwext4.svg)](LICENSE)

## Features

- Create ext2/3/4 filesystems with `mkfs`
- Mount and unmount filesystems
- File operations: create, read, write, truncate, seek
- Directory operations: create, remove, iterate
- Symbolic and hard links
- File metadata and permissions
- Block device abstraction for various backing stores
- Zero runtime dependencies (lwext4 is statically compiled)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ext4-lwext4 = "0.1"
```

## Quick Start

```rust
use ext4_lwext4::{mkfs, Ext4Fs, MkfsOptions, OpenFlags, FileBlockDevice};
use std::io::{Read, Write};

// Create a 100MB disk image
let device = FileBlockDevice::create("disk.img", 100 * 1024 * 1024)?;

// Format as ext4
mkfs(device, &MkfsOptions::default())?;

// Reopen and mount
let device = FileBlockDevice::open("disk.img")?;
let fs = Ext4Fs::mount(device, false)?;

// Create a directory
fs.mkdir("/data", 0o755)?;

// Write a file
{
    let mut file = fs.open("/data/hello.txt", OpenFlags::CREATE | OpenFlags::WRITE)?;
    file.write_all(b"Hello, ext4!")?;
}

// Read it back
{
    let mut file = fs.open("/data/hello.txt", OpenFlags::READ)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    println!("Content: {}", content);
}

// List directory
for entry in fs.open_dir("/data")? {
    let entry = entry?;
    println!("{}: {:?}", entry.name(), entry.file_type());
}

// Unmount
fs.umount()?;
```

## Block Devices

The crate provides several block device implementations:

| Type | Description |
|------|-------------|
| `FileBlockDevice` | Backed by a file (disk image) |
| `MemoryBlockDevice` | In-memory storage (for testing/embedded) |

You can implement the `BlockDevice` trait for custom storage backends:

```rust
use ext4_lwext4::BlockDevice;

pub trait BlockDevice: Send {
    fn block_size(&self) -> u32;
    fn block_count(&self) -> u64;
    fn read_block(&mut self, block: u64, buffer: &mut [u8]) -> std::io::Result<()>;
    fn write_block(&mut self, block: u64, buffer: &[u8]) -> std::io::Result<()>;
}
```

## Filesystem Types

```rust
use ext4_lwext4::MkfsOptions;

// ext2 (no journaling)
let opts = MkfsOptions::ext2();

// ext3 (with journaling)
let opts = MkfsOptions::ext3();

// ext4 (with extents and journaling) - default
let opts = MkfsOptions::ext4();

// Custom options
let opts = MkfsOptions {
    block_size: 4096,
    inode_size: 256,
    label: Some("my_volume".into()),
    ..MkfsOptions::ext4()
};
```

## API Overview

### Filesystem Operations

```rust
let fs = Ext4Fs::mount(device, read_only)?;

// Directory operations
fs.mkdir("/path", mode)?;
fs.rmdir("/path")?;
fs.rename("/old", "/new")?;

// File operations
let file = fs.open("/path", OpenFlags::READ | OpenFlags::WRITE)?;
fs.remove("/path")?;

// Links
fs.symlink("/target", "/link")?;
fs.link("/existing", "/new_link")?;
fs.readlink("/link")?;

// Metadata
let meta = fs.metadata("/path")?;
fs.chmod("/path", 0o644)?;
fs.chown("/path", uid, gid)?;

// Filesystem info
let stats = fs.stats()?;
println!("Free space: {} bytes", stats.free_size());

fs.umount()?;
```

### File Operations

```rust
use std::io::{Read, Write, Seek};

let mut file = fs.open("/file.txt", OpenFlags::CREATE | OpenFlags::READ | OpenFlags::WRITE)?;

// Write
file.write_all(b"Hello")?;

// Seek
file.seek(std::io::SeekFrom::Start(0))?;

// Read
let mut buf = [0u8; 5];
file.read_exact(&mut buf)?;

// Truncate
file.truncate(0)?;

// Get size
let size = file.size()?;
```

### Directory Iteration

```rust
let dir = fs.open_dir("/path")?;

for entry in dir {
    let entry = entry?;
    println!("Name: {}", entry.name());
    println!("Type: {:?}", entry.file_type());
    println!("Inode: {}", entry.inode());
}
```

## License

This project uses a dual licensing scheme:

### Default: MIT OR Apache-2.0

By default (without GPL features), this project is licensed under either:
- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

### With GPL Features: GPL-2.0

When compiled with `gpl-extents` or `gpl-xattr` features, the resulting binary is licensed under [GPL-2.0](LICENSE-GPL) due to the inclusion of GPL-licensed source files from lwext4.

| Feature | Description | License Impact |
|---------|-------------|----------------|
| (default) | Base ext4 support | MIT OR Apache-2.0 |
| `gpl-extents` | Advanced extents implementation | GPL-2.0 |
| `gpl-xattr` | Extended attributes support | GPL-2.0 |
| `gpl` | All GPL features | GPL-2.0 |

### lwext4 Licensing

The underlying [lwext4](https://github.com/gkostka/lwext4) library has mixed licensing:
- Most source files: **BSD-3-Clause**
- `ext4_extent.c`, `ext4_xattr.c`: **GPL-2.0**

This crate excludes GPL-licensed files by default to maintain permissive licensing compatibility.

## Building from Source

```bash
# Clone with submodules
git clone --recursive https://github.com/arcbox-labs/ext4-lwext4
cd ext4-lwext4

# Build (default, no GPL)
cargo build

# Build with GPL features
cargo build --features gpl

# Run tests
cargo test
```

## Requirements

- Rust 1.90+ (2024 edition)
- C compiler (for building lwext4)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Acknowledgments

- [lwext4](https://github.com/gkostka/lwext4) - The lightweight ext2/3/4 implementation this crate wraps
- [HelenOS](http://helenos.org/) - Original source of much of lwext4's code
