use std::env;
use std::path::PathBuf;

fn main() {
    let lwext4_src = PathBuf::from("vendor/lwext4/src");
    let lwext4_inc = PathBuf::from("vendor/lwext4/include");

    // Core source files (BSD-3-Clause / MIT OR Apache-2.0 compatible)
    let mut sources = vec![
        "ext4.c",
        "ext4_balloc.c",
        "ext4_bcache.c",
        "ext4_bitmap.c",
        "ext4_block_group.c",
        "ext4_blockdev.c",
        "ext4_crc32.c",
        "ext4_debug.c",
        "ext4_dir.c",
        "ext4_dir_idx.c",
        "ext4_fs.c",
        "ext4_hash.c",
        "ext4_ialloc.c",
        "ext4_inode.c",
        "ext4_journal.c",
        "ext4_mkfs.c",
        "ext4_super.c",
        "ext4_trans.c",
    ];

    // GPL-2.0 licensed files - only included with explicit feature flags
    if env::var("CARGO_FEATURE_GPL_EXTENTS").is_ok() {
        sources.push("ext4_extent.c");
    }
    if env::var("CARGO_FEATURE_GPL_XATTR").is_ok() {
        sources.push("ext4_xattr.c");
    }

    let mut build = cc::Build::new();

    for src in &sources {
        build.file(lwext4_src.join(src));
    }

    // Helper functions (sizeof, etc.)
    build.file("vendor/helpers.c");

    build
        .include(&lwext4_inc)
        .include(lwext4_inc.join("misc"))
        .define("CONFIG_USE_DEFAULT_CFG", "1")
        .define("CONFIG_HAVE_OWN_OFLAGS", "1");

    // Disable extent support at the C preprocessor level when the GPL feature
    // is not enabled. Without this, ext4_fs.c compiles calls to extent functions
    // (guarded by CONFIG_EXTENTS_ENABLE) but ext4_extent.c is not compiled,
    // causing undefined symbol errors at link time.
    if env::var("CARGO_FEATURE_GPL_EXTENTS").is_err() {
        build.define("CONFIG_EXTENTS_ENABLE", "0");
    }

    build
        .flag_if_supported("-std=c99")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-sign-compare")
        .warnings(false)
        .compile("lwext4");

    println!("cargo:rerun-if-changed=vendor/lwext4/src");
    println!("cargo:rerun-if-changed=vendor/lwext4/include");
}
