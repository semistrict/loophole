#ifndef LOOPHOLE_INODE_OPS_H
#define LOOPHOLE_INODE_OPS_H

#include <stdint.h>
#include <stddef.h>

// Attribute struct returned by inode_getattr.
struct inode_attr {
    uint32_t ino;
    uint64_t size;
    uint32_t mode;   // includes file type bits (S_IFDIR, S_IFREG, etc.)
    uint32_t uid;
    uint32_t gid;
    uint32_t atime;
    uint32_t mtime;
    uint32_t ctime;
    uint16_t links;
};

// Bitmask for inode_setattr: which fields to set.
#define INODE_ATTR_MODE  (1 << 0)
#define INODE_ATTR_UID   (1 << 1)
#define INODE_ATTR_GID   (1 << 2)
#define INODE_ATTR_ATIME (1 << 3)
#define INODE_ATTR_MTIME (1 << 4)
#define INODE_ATTR_CTIME (1 << 5)
#define INODE_ATTR_SIZE  (1 << 6)

// Directory entry passed to readdir callback.
struct inode_dirent {
    uint32_t inode;
    uint8_t  type;   // EXT4_DE_REG_FILE, EXT4_DE_DIR, etc.
    uint8_t  name_len;
    char     name[256];
};

// Callback for inode_readdir. Return 0 to continue, non-zero to stop.
typedef int (*inode_readdir_cb)(const struct inode_dirent *de, void *ctx);

// Get the ext4_mountpoint* for a given mount point name.
// Returns NULL if not found.
struct ext4_mountpoint *inode_get_mp(const char *mount_point);

// Lookup a child entry in parent directory by name.
int inode_lookup(struct ext4_mountpoint *mp, uint32_t parent_ino,
                 const char *name, uint32_t name_len, uint32_t *child_ino);

// Get attributes of an inode.
int inode_getattr(struct ext4_mountpoint *mp, uint32_t ino,
                  struct inode_attr *out);

// Set attributes of an inode. Only fields indicated by mask are written.
int inode_setattr(struct ext4_mountpoint *mp, uint32_t ino,
                  const struct inode_attr *attr, uint32_t mask);

// Create a regular file (or other non-dir, non-symlink type) in parent dir.
// filetype is EXT4_DE_REG_FILE, EXT4_DE_CHRDEV, etc.
int inode_mknod(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len,
                uint32_t mode, int filetype, uint32_t *child_ino);

// Create a directory in parent dir.
int inode_mkdir(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len,
                uint32_t mode, uint32_t *child_ino);

// Unlink a name from parent directory. Decrements link count, frees inode if 0.
int inode_unlink(struct ext4_mountpoint *mp, uint32_t parent_ino,
                 const char *name, uint32_t name_len);

// Remove a directory (must be empty except for . and ..).
int inode_rmdir(struct ext4_mountpoint *mp, uint32_t parent_ino,
                const char *name, uint32_t name_len);

// Rename: move entry from src_parent/src_name to dst_parent/dst_name.
int inode_rename(struct ext4_mountpoint *mp,
                 uint32_t src_parent_ino, const char *src_name, uint32_t src_name_len,
                 uint32_t dst_parent_ino, const char *dst_name, uint32_t dst_name_len);

// Create a hard link: add name in new_parent pointing to ino.
int inode_link(struct ext4_mountpoint *mp, uint32_t ino,
               uint32_t new_parent_ino, const char *name, uint32_t name_len);

// Create a symbolic link in parent dir. Returns new inode number.
int inode_symlink(struct ext4_mountpoint *mp, uint32_t parent_ino,
                  const char *name, uint32_t name_len,
                  const char *target, uint32_t target_len,
                  uint32_t *child_ino);

// Read symlink target.
int inode_readlink(struct ext4_mountpoint *mp, uint32_t ino,
                   char *buf, size_t buf_size, size_t *read_cnt);

// Iterate directory entries, calling cb for each (skips . and ..).
int inode_readdir(struct ext4_mountpoint *mp, uint32_t dir_ino,
                  inode_readdir_cb cb, void *ctx);

// Open a file by inode number. Populates the ext4_file struct for I/O.
// flags: O_RDONLY, O_WRONLY, O_RDWR (POSIX-style).
struct ext4_file;
int inode_file_open(struct ext4_mountpoint *mp, uint32_t ino,
                    int flags, struct ext4_file *out);

// Unlink a name from parent directory. Decrements link count.
// If link count reaches 0, adds the inode to the on-disk orphan list
// instead of freeing it immediately. Returns the child inode number
// in *child_ino so the caller can track open handles.
int inode_unlink_orphan(struct ext4_mountpoint *mp, uint32_t parent_ino,
                        const char *name, uint32_t name_len,
                        uint32_t *child_ino);

// Free an orphaned inode: truncate data, free inode, remove from orphan list.
// Call this when the last open file handle for an orphaned inode is closed.
int inode_free_orphan(struct ext4_mountpoint *mp, uint32_t ino);

// Recover all orphaned inodes: walk the on-disk orphan list and free each
// inode. Call this once at mount time to clean up after crashes.
int inode_orphan_recover(struct ext4_mountpoint *mp);

// Read the head of the on-disk orphan list (for testing/debugging).
uint32_t inode_orphan_head(struct ext4_mountpoint *mp);

// Add an inode to the on-disk orphan list (prepend to superblock's
// last_orphan linked list using inode deletion_time as next pointer).
int inode_orphan_add(struct ext4_mountpoint *mp, uint32_t ino);

// Remove an inode from the on-disk orphan list.
int inode_orphan_remove(struct ext4_mountpoint *mp, uint32_t ino);

#endif // LOOPHOLE_INODE_OPS_H
