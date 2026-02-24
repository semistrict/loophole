// inode_ops.c — inode-level operations using lwext4 internals.
//
// This file is #include'd from a CGO compilation unit that already
// compiles ext4.c (via lwext4_src.go). We must NOT re-include ext4.c
// here. Instead we forward-declare/access the private statics we need.
//
// Because lwext4_src.go does `#include "../third_party/lwext4/src/ext4.c"`,
// all static functions and the s_mp[] array are available within the same
// translation unit. However, this file is a *separate* .c file compiled
// by CGO. So we need to re-declare the struct and access the global array.

#include <ext4_config.h>
#include <ext4_types.h>
#include <ext4_errno.h>
#include <ext4_oflags.h>
#include <ext4_blockdev.h>
#include <ext4_fs.h>
#include <ext4_dir.h>
#include <ext4_dir_idx.h>
#include <ext4_inode.h>
#include <ext4_super.h>
#include <ext4_bcache.h>
#include <ext4_journal.h>
#include <ext4.h>
#include <ext4_trans.h>

#include <string.h>
#include <stdlib.h>

#include "inode_ops.h"

// --- Access to lwext4 private structures ---
// These are defined as static in ext4.c, but since ext4.c is compiled
// via #include in lwext4_src.go (same translation unit), we can't access
// them from this separate .c file. We re-declare the struct and array
// as extern, relying on the fact that ext4.c's static is actually a
// global symbol in the CGO compilation model (all .c files in a package
// are compiled into one object).
//
// Actually, statics in ext4.c are NOT visible here. We need a different
// approach: use the public ext4_get_sblock() to get the superblock, and
// from that derive the mountpoint via container_of. Or, we use the public
// ext4_mount_point_stats / ext4_get_sblock APIs.
//
// Best approach: we know ext4_file.mp is a struct ext4_mountpoint*.
// We can get it by opening a dummy path. But that's circular.
//
// Simplest correct approach: We store the mp pointer in the Go FS struct
// by calling a helper right after mount that retrieves it. We use
// ext4_get_sblock which takes a mount_point string and returns the sblock.
// From sblock, we can use container_of to get back to ext4_mountpoint.

// Re-declare the mountpoint struct so we can use container_of.
// This must match the definition in ext4.c exactly.
struct ext4_mountpoint {
    bool mounted;
    char name[CONFIG_EXT4_MAX_MP_NAME + 1];
    const struct ext4_lock *os_locks;
    struct ext4_fs fs;
    struct jbd_fs jbd_fs;
    struct jbd_journal jbd_journal;
    struct ext4_bcache bc;
};

// Use ext4_get_sblock to find the sblock, then container_of to get mp.
struct ext4_mountpoint *inode_get_mp(const char *mount_point) {
    struct ext4_sblock *sb = NULL;
    int r = ext4_get_sblock(mount_point, &sb);
    if (r != EOK || !sb)
        return NULL;
    // sb is at offset of .fs.sb within ext4_mountpoint.
    // mp->fs.sb is the sblock. So mp = container_of(sb, struct ext4_mountpoint, fs.sb)
    char *p = (char *)sb;
    p -= offsetof(struct ext4_mountpoint, fs) + offsetof(struct ext4_fs, sb);
    return (struct ext4_mountpoint *)p;
}

int inode_lookup(struct ext4_mountpoint *mp, uint32_t parent_ino,
                 const char *name, uint32_t name_len, uint32_t *child_ino) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    struct ext4_dir_search_result result;
    r = ext4_dir_find_entry(&result, &parent_ref, name, name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    *child_ino = ext4_dir_en_get_inode(result.dentry);
    ext4_dir_destroy_result(&parent_ref, &result);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_getattr(struct ext4_mountpoint *mp, uint32_t ino,
                  struct inode_attr *out) {
    struct ext4_inode_ref ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, ino, &ref);
    if (r != EOK)
        return r;

    out->ino = ino;
    out->mode = ext4_inode_get_mode(&mp->fs.sb, ref.inode);
    out->uid = ext4_inode_get_uid(ref.inode);
    out->gid = ext4_inode_get_gid(ref.inode);
    out->size = ext4_inode_get_size(&mp->fs.sb, ref.inode);
    out->atime = ext4_inode_get_access_time(ref.inode);
    out->mtime = ext4_inode_get_modif_time(ref.inode);
    out->ctime = ext4_inode_get_change_inode_time(ref.inode);
    out->links = ext4_inode_get_links_cnt(ref.inode);

    ext4_fs_put_inode_ref(&ref);
    return EOK;
}

int inode_setattr(struct ext4_mountpoint *mp, uint32_t ino,
                  const struct inode_attr *attr, uint32_t mask) {
    struct ext4_inode_ref ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, ino, &ref);
    if (r != EOK)
        return r;

    if (mask & INODE_ATTR_MODE)
        ext4_inode_set_mode(&mp->fs.sb, ref.inode, attr->mode);
    if (mask & INODE_ATTR_UID)
        ext4_inode_set_uid(ref.inode, attr->uid);
    if (mask & INODE_ATTR_GID)
        ext4_inode_set_gid(ref.inode, attr->gid);
    if (mask & INODE_ATTR_ATIME)
        ext4_inode_set_access_time(ref.inode, attr->atime);
    if (mask & INODE_ATTR_MTIME)
        ext4_inode_set_modif_time(ref.inode, attr->mtime);
    if (mask & INODE_ATTR_CTIME)
        ext4_inode_set_change_inode_time(ref.inode, attr->ctime);

    ref.dirty = true;
    r = ext4_fs_put_inode_ref(&ref);
    return r;
}

int inode_mknod(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len,
                uint32_t mode, int filetype, uint32_t *child_ino) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    struct ext4_inode_ref child_ref;
    r = ext4_fs_alloc_inode(&mp->fs, &child_ref, filetype);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Set requested permissions while preserving file type bits from alloc.
    uint32_t alloc_mode = ext4_inode_get_mode(&mp->fs.sb, child_ref.inode);
    ext4_inode_set_mode(&mp->fs.sb, child_ref.inode,
                        (alloc_mode & 0xF000) | (mode & 07777));
    child_ref.dirty = true;

    // Add entry to parent directory.
    r = ext4_dir_add_entry(&parent_ref, name, name_len, &child_ref);
    if (r != EOK) {
        ext4_fs_free_inode(&child_ref);
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Set link count to 1.
    ext4_inode_set_links_cnt(child_ref.inode, 1);
    child_ref.dirty = true;

    *child_ino = child_ref.index;

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_mkdir(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len,
                uint32_t mode, uint32_t *child_ino) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    struct ext4_inode_ref child_ref;
    r = ext4_fs_alloc_inode(&mp->fs, &child_ref, EXT4_DE_DIR);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Set mode with directory type bits.
    ext4_inode_set_mode(&mp->fs.sb, child_ref.inode,
                        EXT4_INODE_MODE_DIRECTORY | (mode & 07777));
    child_ref.dirty = true;

    // Add entry in parent.
    r = ext4_dir_add_entry(&parent_ref, name, name_len, &child_ref);
    if (r != EOK) {
        ext4_fs_free_inode(&child_ref);
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Initialize directory: add "." and ".." entries.
#if CONFIG_DIR_INDEX_ENABLE
    if (ext4_sb_feature_com(&mp->fs.sb, EXT4_FCOM_DIR_INDEX)) {
        r = ext4_dir_dx_init(&child_ref, &parent_ref);
        if (r != EOK) {
            ext4_dir_remove_entry(&parent_ref, name, name_len);
            ext4_fs_free_inode(&child_ref);
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&parent_ref);
            return r;
        }
        ext4_inode_set_flag(child_ref.inode, EXT4_INODE_FLAG_INDEX);
        child_ref.dirty = true;
    } else
#endif
    {
        r = ext4_dir_add_entry(&child_ref, ".", 1, &child_ref);
        if (r != EOK) {
            ext4_dir_remove_entry(&parent_ref, name, name_len);
            ext4_fs_free_inode(&child_ref);
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&parent_ref);
            return r;
        }

        r = ext4_dir_add_entry(&child_ref, "..", 2, &parent_ref);
        if (r != EOK) {
            ext4_dir_remove_entry(&parent_ref, name, name_len);
            ext4_dir_remove_entry(&child_ref, ".", 1);
            ext4_fs_free_inode(&child_ref);
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&parent_ref);
            return r;
        }
    }

    // Directory starts with 2 links (. and ..).
    ext4_inode_set_links_cnt(child_ref.inode, 2);
    child_ref.dirty = true;

    // Parent gains a link (for ..).
    ext4_fs_inode_links_count_inc(&parent_ref);
    parent_ref.dirty = true;

    *child_ino = child_ref.index;

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

// Helper: check if directory has children (non . / ..).
static int has_children(struct ext4_mountpoint *mp, struct ext4_inode_ref *dir_ref, bool *result) {
    struct ext4_sblock *sb = &mp->fs.sb;
    if (!ext4_inode_is_type(sb, dir_ref->inode, EXT4_INODE_MODE_DIRECTORY)) {
        *result = false;
        return EOK;
    }

    struct ext4_dir_iter it;
    int r = ext4_dir_iterator_init(&it, dir_ref, 0);
    if (r != EOK)
        return r;

    *result = false;
    while (it.curr != NULL) {
        if (ext4_dir_en_get_inode(it.curr) != 0) {
            uint16_t nlen = ext4_dir_en_get_name_len(sb, it.curr);
            if (nlen == 1 && it.curr->name[0] == '.') {
                // skip "."
            } else if (nlen == 2 && it.curr->name[0] == '.' && it.curr->name[1] == '.') {
                // skip ".."
            } else {
                *result = true;
                break;
            }
        }
        r = ext4_dir_iterator_next(&it);
        if (r != EOK) {
            ext4_dir_iterator_fini(&it);
            return r;
        }
    }
    ext4_dir_iterator_fini(&it);
    return EOK;
}

int inode_unlink(struct ext4_mountpoint *mp, uint32_t parent_ino,
                 const char *name, uint32_t name_len) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    // Find the entry to get the child inode.
    struct ext4_dir_search_result result;
    r = ext4_dir_find_entry(&result, &parent_ref, name, name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    uint32_t child_ino = ext4_dir_en_get_inode(result.dentry);
    ext4_dir_destroy_result(&parent_ref, &result);

    struct ext4_inode_ref child_ref;
    r = ext4_fs_get_inode_ref(&mp->fs, child_ino, &child_ref);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Don't unlink directories with this function.
    if (ext4_inode_is_type(&mp->fs.sb, child_ref.inode, EXT4_INODE_MODE_DIRECTORY)) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return EISDIR;
    }

    // Remove directory entry.
    r = ext4_dir_remove_entry(&parent_ref, name, name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Decrement link count.
    uint16_t links = ext4_inode_get_links_cnt(child_ref.inode);
    if (links > 0) {
        ext4_fs_inode_links_count_dec(&child_ref);
        child_ref.dirty = true;
    }

    // If link count reaches 0, free the inode.
    links = ext4_inode_get_links_cnt(child_ref.inode);
    if (links == 0) {
        // Truncate all data.
        ext4_fs_truncate_inode(&child_ref, 0);
        ext4_fs_free_inode(&child_ref);
    }

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_rmdir(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    // Find entry.
    struct ext4_dir_search_result result;
    r = ext4_dir_find_entry(&result, &parent_ref, name, name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    uint32_t child_ino = ext4_dir_en_get_inode(result.dentry);
    ext4_dir_destroy_result(&parent_ref, &result);

    struct ext4_inode_ref child_ref;
    r = ext4_fs_get_inode_ref(&mp->fs, child_ino, &child_ref);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Must be a directory.
    if (!ext4_inode_is_type(&mp->fs.sb, child_ref.inode, EXT4_INODE_MODE_DIRECTORY)) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return ENOTDIR;
    }

    // Must be empty.
    bool children = false;
    r = has_children(mp, &child_ref, &children);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }
    if (children) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return ENOTEMPTY;
    }

    // Remove from parent.
    r = ext4_dir_remove_entry(&parent_ref, name, name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Decrement parent link count (child's ".." pointed to parent).
    ext4_fs_inode_links_count_dec(&parent_ref);
    parent_ref.dirty = true;

    // Set child link count to 0 and free.
    ext4_inode_set_links_cnt(child_ref.inode, 0);
    child_ref.dirty = true;
    ext4_fs_truncate_inode(&child_ref, 0);
    ext4_fs_free_inode(&child_ref);

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_rename(struct ext4_mountpoint *mp,
                 uint32_t src_parent_ino, const char *src_name, uint32_t src_name_len,
                 uint32_t dst_parent_ino, const char *dst_name, uint32_t dst_name_len) {
    // Look up source entry.
    struct ext4_inode_ref src_parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, src_parent_ino, &src_parent_ref);
    if (r != EOK)
        return r;

    struct ext4_dir_search_result result;
    r = ext4_dir_find_entry(&result, &src_parent_ref, src_name, src_name_len);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&src_parent_ref);
        return r;
    }

    uint32_t child_ino = ext4_dir_en_get_inode(result.dentry);
    ext4_dir_destroy_result(&src_parent_ref, &result);

    struct ext4_inode_ref child_ref;
    r = ext4_fs_get_inode_ref(&mp->fs, child_ino, &child_ref);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&src_parent_ref);
        return r;
    }

    // Get dst parent (may be same as src).
    struct ext4_inode_ref dst_parent_ref;
    if (dst_parent_ino == src_parent_ino) {
        dst_parent_ref = src_parent_ref;
    } else {
        r = ext4_fs_get_inode_ref(&mp->fs, dst_parent_ino, &dst_parent_ref);
        if (r != EOK) {
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&src_parent_ref);
            return r;
        }
    }

    // Try to remove destination if it already exists.
    struct ext4_dir_search_result dst_result;
    r = ext4_dir_find_entry(&dst_result, &dst_parent_ref, dst_name, dst_name_len);
    if (r == EOK) {
        // Destination exists — remove it.
        uint32_t dst_child_ino = ext4_dir_en_get_inode(dst_result.dentry);
        ext4_dir_destroy_result(&dst_parent_ref, &dst_result);

        struct ext4_inode_ref dst_child_ref;
        r = ext4_fs_get_inode_ref(&mp->fs, dst_child_ino, &dst_child_ref);
        if (r == EOK) {
            ext4_dir_remove_entry(&dst_parent_ref, dst_name, dst_name_len);
            if (ext4_inode_is_type(&mp->fs.sb, dst_child_ref.inode, EXT4_INODE_MODE_DIRECTORY)) {
                ext4_fs_inode_links_count_dec(&dst_parent_ref);
                dst_parent_ref.dirty = true;
            }
            ext4_fs_inode_links_count_dec(&dst_child_ref);
            dst_child_ref.dirty = true;
            if (ext4_inode_get_links_cnt(dst_child_ref.inode) == 0) {
                ext4_fs_truncate_inode(&dst_child_ref, 0);
                ext4_fs_free_inode(&dst_child_ref);
            }
            ext4_fs_put_inode_ref(&dst_child_ref);
        }
    }

    // Add entry in destination.
    r = ext4_dir_add_entry(&dst_parent_ref, dst_name, dst_name_len, &child_ref);
    if (r != EOK)
        goto cleanup;

    // Remove entry from source.
    r = ext4_dir_remove_entry(&src_parent_ref, src_name, src_name_len);
    if (r != EOK)
        goto cleanup;

    // If it's a directory and parents differ, update ".." and link counts.
    bool is_dir = ext4_inode_is_type(&mp->fs.sb, child_ref.inode, EXT4_INODE_MODE_DIRECTORY);
    if (is_dir && dst_parent_ino != src_parent_ino) {
        // Update ".." to point to new parent.
        bool idx = ext4_inode_has_flag(child_ref.inode, EXT4_INODE_FLAG_INDEX);
        if (!idx) {
            struct ext4_dir_search_result dotdot;
            r = ext4_dir_find_entry(&dotdot, &child_ref, "..", 2);
            if (r == EOK) {
                ext4_dir_en_set_inode(dotdot.dentry, dst_parent_ino);
                ext4_trans_set_block_dirty(dotdot.block.buf);
                ext4_dir_destroy_result(&child_ref, &dotdot);
            }
        } else {
#if CONFIG_DIR_INDEX_ENABLE
            ext4_dir_dx_reset_parent_inode(&child_ref, dst_parent_ino);
#endif
        }

        ext4_fs_inode_links_count_inc(&dst_parent_ref);
        dst_parent_ref.dirty = true;
        ext4_fs_inode_links_count_dec(&src_parent_ref);
        src_parent_ref.dirty = true;
    }

cleanup:
    ext4_fs_put_inode_ref(&child_ref);
    if (dst_parent_ino != src_parent_ino)
        ext4_fs_put_inode_ref(&dst_parent_ref);
    ext4_fs_put_inode_ref(&src_parent_ref);
    return r;
}

int inode_link(struct ext4_mountpoint *mp, uint32_t ino,
               uint32_t new_parent_ino, const char *name, uint32_t name_len) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, new_parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    struct ext4_inode_ref child_ref;
    r = ext4_fs_get_inode_ref(&mp->fs, ino, &child_ref);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Cannot hardlink directories.
    if (ext4_inode_is_type(&mp->fs.sb, child_ref.inode, EXT4_INODE_MODE_DIRECTORY)) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return EPERM;
    }

    r = ext4_dir_add_entry(&parent_ref, name, name_len, &child_ref);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    ext4_fs_inode_links_count_inc(&child_ref);
    child_ref.dirty = true;

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_symlink(struct ext4_mountpoint *mp, uint32_t parent_ino,
                  const char *name, uint32_t name_len,
                  const char *target, uint32_t target_len,
                  uint32_t *child_ino) {
    struct ext4_inode_ref parent_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, parent_ino, &parent_ref);
    if (r != EOK)
        return r;

    struct ext4_inode_ref child_ref;
    r = ext4_fs_alloc_inode(&mp->fs, &child_ref, EXT4_DE_SYMLINK);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    // Set symlink mode.
    ext4_inode_set_mode(&mp->fs.sb, child_ref.inode,
                        EXT4_INODE_MODE_SOFTLINK | 0777);
    child_ref.dirty = true;

    // Add to parent directory.
    r = ext4_dir_add_entry(&parent_ref, name, name_len, &child_ref);
    if (r != EOK) {
        ext4_fs_free_inode(&child_ref);
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return r;
    }

    ext4_inode_set_links_cnt(child_ref.inode, 1);
    child_ref.dirty = true;

    // Write symlink target. Short symlinks go in inode->blocks.
    uint32_t block_size = ext4_sb_get_block_size(&mp->fs.sb);
    if (target_len < sizeof(child_ref.inode->blocks)) {
        memset(child_ref.inode->blocks, 0, sizeof(child_ref.inode->blocks));
        memcpy(child_ref.inode->blocks, target, target_len);
        ext4_inode_clear_flag(child_ref.inode, EXT4_INODE_FLAG_EXTENTS);
    } else if (target_len <= block_size) {
        ext4_fs_inode_blocks_init(&mp->fs, &child_ref);
        ext4_fsblk_t fblock;
        ext4_lblk_t sblock;
        r = ext4_fs_append_inode_dblk(&child_ref, &fblock, &sblock);
        if (r != EOK) {
            ext4_fs_free_inode(&child_ref);
            ext4_dir_remove_entry(&parent_ref, name, name_len);
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&parent_ref);
            return r;
        }
        uint64_t off = fblock * block_size;
        r = ext4_block_writebytes(mp->fs.bdev, off, target, target_len);
        if (r != EOK) {
            ext4_fs_free_inode(&child_ref);
            ext4_dir_remove_entry(&parent_ref, name, name_len);
            ext4_fs_put_inode_ref(&child_ref);
            ext4_fs_put_inode_ref(&parent_ref);
            return r;
        }
    } else {
        // Target too long.
        ext4_fs_free_inode(&child_ref);
        ext4_dir_remove_entry(&parent_ref, name, name_len);
        ext4_fs_put_inode_ref(&child_ref);
        ext4_fs_put_inode_ref(&parent_ref);
        return ENAMETOOLONG;
    }

    ext4_inode_set_size(child_ref.inode, target_len);
    child_ref.dirty = true;

    *child_ino = child_ref.index;

    ext4_fs_put_inode_ref(&child_ref);
    ext4_fs_put_inode_ref(&parent_ref);
    return EOK;
}

int inode_readlink(struct ext4_mountpoint *mp, uint32_t ino,
                   char *buf, size_t buf_size, size_t *read_cnt) {
    struct ext4_inode_ref ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, ino, &ref);
    if (r != EOK)
        return r;

    uint64_t size = ext4_inode_get_size(&mp->fs.sb, ref.inode);
    if (size > buf_size)
        size = buf_size;

    if (size < sizeof(ref.inode->blocks)) {
        // Short symlink: stored inline in inode->blocks.
        memcpy(buf, ref.inode->blocks, size);
        *read_cnt = size;
    } else {
        // Long symlink: use ext4_fread via a manually populated ext4_file.
        ext4_file f;
        memset(&f, 0, sizeof(f));
        f.mp = mp;
        f.inode = ino;
        f.flags = O_RDONLY;
        f.fsize = ext4_inode_get_size(&mp->fs.sb, ref.inode);
        f.fpos = 0;
        ext4_fs_put_inode_ref(&ref);
        r = ext4_fread(&f, buf, buf_size, read_cnt);
        return r;
    }

    ext4_fs_put_inode_ref(&ref);
    return EOK;
}

int inode_readdir(struct ext4_mountpoint *mp, uint32_t dir_ino,
                  inode_readdir_cb cb, void *ctx) {
    struct ext4_inode_ref dir_ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, dir_ino, &dir_ref);
    if (r != EOK)
        return r;

    struct ext4_dir_iter it;
    r = ext4_dir_iterator_init(&it, &dir_ref, 0);
    if (r != EOK) {
        ext4_fs_put_inode_ref(&dir_ref);
        return r;
    }

    while (it.curr != NULL) {
        uint32_t entry_ino = ext4_dir_en_get_inode(it.curr);
        if (entry_ino != 0) {
            uint16_t nlen = ext4_dir_en_get_name_len(&mp->fs.sb, it.curr);
            // Skip "." and ".."
            bool skip = false;
            if (nlen == 1 && it.curr->name[0] == '.')
                skip = true;
            if (nlen == 2 && it.curr->name[0] == '.' && it.curr->name[1] == '.')
                skip = true;

            if (!skip) {
                struct inode_dirent de;
                de.inode = entry_ino;
                de.type = ext4_dir_en_get_inode_type(&mp->fs.sb, it.curr);
                de.name_len = (uint8_t)(nlen > 255 ? 255 : nlen);
                memcpy(de.name, it.curr->name, de.name_len);
                de.name[de.name_len] = 0;

                int stop = cb(&de, ctx);
                if (stop) break;
            }
        }

        r = ext4_dir_iterator_next(&it);
        if (r != EOK) {
            ext4_dir_iterator_fini(&it);
            ext4_fs_put_inode_ref(&dir_ref);
            return r;
        }
    }

    ext4_dir_iterator_fini(&it);
    ext4_fs_put_inode_ref(&dir_ref);
    return EOK;
}

int inode_file_open(struct ext4_mountpoint *mp, uint32_t ino,
                    int flags, ext4_file *out) {
    struct ext4_inode_ref ref;
    int r = ext4_fs_get_inode_ref(&mp->fs, ino, &ref);
    if (r != EOK)
        return r;

    memset(out, 0, sizeof(*out));
    out->mp = mp;
    out->inode = ino;
    out->flags = (uint32_t)flags;
    out->fsize = ext4_inode_get_size(&mp->fs.sb, ref.inode);
    out->fpos = 0;

    ext4_fs_put_inode_ref(&ref);
    return EOK;
}
