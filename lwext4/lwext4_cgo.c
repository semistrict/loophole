// lwext4_cgo.c — Implementation of the opaque wrapper.
// This file includes the real lwext4 headers (clang handles forward
// declarations fine) and implements thin wrappers.

#include <stdlib.h>
#include <string.h>
#include <ext4.h>
#include <ext4_mkfs.h>
#include "blockdev.h"
#include "inode_ops.h"
#include "readdir_bridge.h"
#include "lwext4_cgo.h"

// --- Block device ---

lh_bdev lh_create_blockdev(int handle, uint32_t ph_bsize, uint64_t ph_bcnt) {
    return (lh_bdev)create_blockdev(handle, ph_bsize, ph_bcnt);
}

void lh_destroy_blockdev(lh_bdev bdev) {
    destroy_blockdev((struct ext4_blockdev *)bdev);
}

// --- Device / mount lifecycle ---

int lh_device_register(lh_bdev bdev, const char *dev_name) {
    return ext4_device_register((struct ext4_blockdev *)bdev, dev_name);
}

void lh_device_unregister(const char *dev_name) {
    ext4_device_unregister(dev_name);
}

int lh_mkfs(lh_bdev bdev, int64_t size, uint32_t block_size, int journal, const char *label) {
    struct ext4_mkfs_info info;
    memset(&info, 0, sizeof(info));
    info.len = (uint64_t)size;
    info.block_size = block_size;
    info.journal = journal ? 1 : 0;
    if (label) {
        info.label = (char *)label;
    }
    struct ext4_fs fs;
    memset(&fs, 0, sizeof(fs));
    return ext4_mkfs(&fs, (struct ext4_blockdev *)bdev, &info, F_SET_EXT4);
}

int lh_mount(const char *dev_name, const char *mount_point, int read_only) {
    return ext4_mount(dev_name, mount_point, read_only ? 1 : 0);
}

int lh_umount(const char *mount_point) {
    return ext4_umount(mount_point);
}

void lh_cache_write_back(const char *mount_point, int on) {
    ext4_cache_write_back(mount_point, on ? 1 : 0);
}

int lh_cache_flush(const char *mount_point) {
    return ext4_cache_flush(mount_point);
}

// --- Mount point lookup ---

lh_mp lh_get_mp(const char *mount_point) {
    return (lh_mp)inode_get_mp(mount_point);
}

// --- Inode operations ---

int lh_lookup(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len, uint32_t *child) {
    return inode_lookup((struct ext4_mountpoint *)mp, parent, name, name_len, child);
}

int lh_getattr(lh_mp mp, uint32_t ino, struct inode_attr *out) {
    return inode_getattr((struct ext4_mountpoint *)mp, ino, out);
}

int lh_setattr(lh_mp mp, uint32_t ino, const struct inode_attr *attr, uint32_t mask) {
    return inode_setattr((struct ext4_mountpoint *)mp, ino, attr, mask);
}

int lh_mknod(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
             uint32_t mode, int filetype, uint32_t *child) {
    return inode_mknod((struct ext4_mountpoint *)mp, parent, name, name_len, mode, filetype, child);
}

int lh_mkdir(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
             uint32_t mode, uint32_t *child) {
    return inode_mkdir((struct ext4_mountpoint *)mp, parent, name, name_len, mode, child);
}

int lh_unlink(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len) {
    return inode_unlink((struct ext4_mountpoint *)mp, parent, name, name_len);
}

int lh_unlink_orphan(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len, uint32_t *child) {
    return inode_unlink_orphan((struct ext4_mountpoint *)mp, parent, name, name_len, child);
}

int lh_free_orphan(lh_mp mp, uint32_t ino) {
    return inode_free_orphan((struct ext4_mountpoint *)mp, ino);
}

int lh_orphan_recover(lh_mp mp) {
    return inode_orphan_recover((struct ext4_mountpoint *)mp);
}

int lh_rmdir(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len) {
    return inode_rmdir((struct ext4_mountpoint *)mp, parent, name, name_len);
}

int lh_rename(lh_mp mp,
              uint32_t src_parent, const char *src_name, uint32_t src_name_len,
              uint32_t dst_parent, const char *dst_name, uint32_t dst_name_len) {
    return inode_rename((struct ext4_mountpoint *)mp, src_parent, src_name, src_name_len,
                        dst_parent, dst_name, dst_name_len);
}

int lh_link(lh_mp mp, uint32_t ino, uint32_t new_parent, const char *name, uint32_t name_len) {
    return inode_link((struct ext4_mountpoint *)mp, ino, new_parent, name, name_len);
}

int lh_symlink(lh_mp mp, uint32_t parent, const char *name, uint32_t name_len,
               const char *target, uint32_t target_len, uint32_t *child) {
    return inode_symlink((struct ext4_mountpoint *)mp, parent, name, name_len, target, target_len, child);
}

int lh_readlink(lh_mp mp, uint32_t ino, char *buf, size_t buf_size, size_t *read_cnt) {
    return inode_readlink((struct ext4_mountpoint *)mp, ino, buf, buf_size, read_cnt);
}

// --- Readdir ---

int lh_readdir_bridge(lh_mp mp, uint32_t dir_ino, uintptr_t ctx) {
    return readdir_bridge((struct ext4_mountpoint *)mp, dir_ino, ctx);
}

// --- File I/O ---

lh_file lh_file_open(lh_mp mp, uint32_t ino, int flags) {
    ext4_file *f = (ext4_file *)malloc(sizeof(ext4_file));
    if (!f) return NULL;
    int rc = inode_file_open((struct ext4_mountpoint *)mp, ino, flags, f);
    if (rc != 0) {
        free(f);
        return NULL;
    }
    return (lh_file)f;
}

int lh_file_close(lh_file f) {
    ext4_file *fp = (ext4_file *)f;
    int rc = ext4_fclose(fp);
    free(fp);
    return rc;
}

int lh_file_read(lh_file f, void *buf, size_t size, size_t *rcnt) {
    return ext4_fread((ext4_file *)f, buf, size, rcnt);
}

int lh_file_write(lh_file f, const void *buf, size_t size, size_t *wcnt) {
    return ext4_fwrite((ext4_file *)f, buf, size, wcnt);
}

int lh_file_seek(lh_file f, int64_t offset, uint32_t whence) {
    return ext4_fseek((ext4_file *)f, offset, whence);
}

int64_t lh_file_tell(lh_file f) {
    return (int64_t)ext4_ftell((ext4_file *)f);
}

int lh_file_truncate(lh_file f, uint64_t size) {
    return ext4_ftruncate((ext4_file *)f, size);
}

uint64_t lh_file_size(lh_file f) {
    return ext4_fsize((ext4_file *)f);
}
