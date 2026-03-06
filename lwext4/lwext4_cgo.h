// lwext4_cgo.h — Opaque C wrapper for TinyGo CGO compatibility.
// TinyGo's CGO parser cannot handle forward-declared structs, so we
// hide all lwext4 types behind void* handles and wrapper functions.
#ifndef LWEXT4_CGO_H_
#define LWEXT4_CGO_H_

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include "inode_ops.h"

// --- Opaque handle types (all void* to avoid exposing lwext4 structs) ---

// lh_bdev is an opaque ext4_blockdev*.
typedef void *lh_bdev;

// lh_mp is an opaque ext4_mountpoint*.
typedef void *lh_mp;

// lh_file is an opaque heap-allocated ext4_file*.
typedef void *lh_file;

// --- Block device ---

lh_bdev lh_create_blockdev(int handle, uint32_t ph_bsize, uint64_t ph_bcnt);
void    lh_destroy_blockdev(lh_bdev bdev);

// --- Device / mount lifecycle ---

int  lh_device_register(lh_bdev bdev, const char *dev_name);
void lh_device_unregister(const char *dev_name);
int  lh_mkfs(lh_bdev bdev, int64_t size, uint32_t block_size, int journal, const char *label);
int  lh_mount(const char *dev_name, const char *mount_point, int read_only);
int  lh_umount(const char *mount_point);
void lh_cache_write_back(const char *mount_point, int on);
int  lh_cache_flush(const char *mount_point);

// --- Mount point lookup ---

lh_mp lh_get_mp(const char *mount_point);

// --- Inode operations (thin wrappers that cast lh_mp → ext4_mountpoint*) ---

int lh_lookup(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len, uint32_t *child);
int lh_getattr(lh_mp mp, uint32_t ino, struct inode_attr *out);
int lh_setattr(lh_mp mp, uint32_t ino, const struct inode_attr *attr, uint32_t mask);
int lh_mknod(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
             uint32_t mode, int filetype, uint32_t *child);
int lh_mkdir(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
             uint32_t mode, uint32_t *child);
int lh_unlink(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len);
int lh_unlink_orphan(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len, uint32_t *child);
int lh_free_orphan(lh_mp mp, uint32_t ino);
int lh_orphan_recover(lh_mp mp);
int lh_rmdir(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len);
int lh_rename(lh_mp mp,
              uint32_t src_parent, const char *src_name, uint32_t src_name_len,
              uint32_t dst_parent, const char *dst_name, uint32_t dst_name_len);
int lh_link(lh_mp mp, uint32_t ino, uint32_t new_parent, const char *name, uint32_t name_len);
int lh_symlink(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
               const char *target, uint32_t target_len, uint32_t *child);
int lh_readlink(lh_mp mp, uint32_t ino, char *buf, size_t buf_size, size_t *read_cnt);

// --- Readdir (uses callback bridge defined in readdir_bridge.h) ---

int lh_readdir_bridge(lh_mp mp, uint32_t dir_ino, uintptr_t ctx);

// --- File I/O ---

lh_file lh_file_open(lh_mp mp, uint32_t ino, int flags);  // returns NULL on error
int     lh_file_close(lh_file f);
int     lh_file_read(lh_file f, void *buf, size_t size, size_t *rcnt);
int     lh_file_write(lh_file f, const void *buf, size_t size, size_t *wcnt);
int     lh_file_seek(lh_file f, int64_t offset, uint32_t whence);
int64_t lh_file_tell(lh_file f);
int     lh_file_truncate(lh_file f, uint64_t size);
uint64_t lh_file_size(lh_file f);

#endif // LWEXT4_CGO_H_
