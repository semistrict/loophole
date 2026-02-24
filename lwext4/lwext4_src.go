package lwext4

// Compile all lwext4 C sources into this Go package.
// Using #include of .c files is a standard CGO pattern to compile
// third-party C code without a separate build system.

// #cgo CFLAGS: -DCONFIG_USE_DEFAULT_CFG=1
// #cgo CFLAGS: -DCONFIG_EXT4_BLOCKDEVS_COUNT=16
// #cgo CFLAGS: -DCONFIG_EXT4_MOUNTPOINTS_COUNT=16
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_OFLAGS=1
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ERRNO=0
// #cgo CFLAGS: -DCONFIG_HAVE_OWN_ASSERT=1
// #cgo CFLAGS: -DCONFIG_DEBUG_PRINTF=0
// #cgo CFLAGS: -DCONFIG_DEBUG_ASSERT=0
// #cgo CFLAGS: -I${SRCDIR}/../third_party/lwext4/include
// #include "../third_party/lwext4/src/ext4.c"
// #include "../third_party/lwext4/src/ext4_balloc.c"
// #include "../third_party/lwext4/src/ext4_bcache.c"
// #include "../third_party/lwext4/src/ext4_bitmap.c"
// #include "../third_party/lwext4/src/ext4_block_group.c"
// #include "../third_party/lwext4/src/ext4_blockdev.c"
// #include "../third_party/lwext4/src/ext4_crc32.c"
// #include "../third_party/lwext4/src/ext4_debug.c"
// #include "../third_party/lwext4/src/ext4_dir.c"
// #include "../third_party/lwext4/src/ext4_dir_idx.c"
// #include "../third_party/lwext4/src/ext4_extent.c"
// #include "../third_party/lwext4/src/ext4_fs.c"
// #include "../third_party/lwext4/src/ext4_hash.c"
// #include "../third_party/lwext4/src/ext4_ialloc.c"
// #include "../third_party/lwext4/src/ext4_inode.c"
// #include "../third_party/lwext4/src/ext4_journal.c"
// #include "../third_party/lwext4/src/ext4_mbr.c"
// #include "../third_party/lwext4/src/ext4_mkfs.c"
// #include "../third_party/lwext4/src/ext4_super.c"
// #include "../third_party/lwext4/src/ext4_trans.c"
// #include "../third_party/lwext4/src/ext4_xattr.c"
import "C"
