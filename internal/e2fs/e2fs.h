// e2fs.h — C API for creating and populating ext4 filesystem images.
#ifndef E2FS_H
#define E2FS_H

#include <stdint.h>

typedef long errcode_t;

// Opaque handle to an ext2_filsys.
typedef void *e2fs_t;

// Create a new ext4 filesystem image at path with the given size in bytes.
// The file must already exist and be truncated to the desired size.
// On success, *out is set to the filesystem handle.
errcode_t e2fs_create(const char *path, uint64_t size_bytes, e2fs_t *out);

// Flush and close the filesystem.
errcode_t e2fs_close(e2fs_t fs);

// Create a directory at path with the given metadata.
errcode_t e2fs_mkdir(e2fs_t fs, const char *path, uint32_t mode,
		     uint32_t uid, uint32_t gid, int64_t mtime);

// Write a regular file at path with the given data and metadata.
errcode_t e2fs_write_file(e2fs_t fs, const char *path, uint32_t mode,
			  uint32_t uid, uint32_t gid, int64_t mtime,
			  const void *data, uint64_t size);

// Create a symbolic link at path pointing to target.
errcode_t e2fs_symlink(e2fs_t fs, const char *path, const char *target,
		       uint32_t uid, uint32_t gid, int64_t mtime);

// Create a device node (char, block, fifo, socket) at path.
errcode_t e2fs_mknod(e2fs_t fs, const char *path, uint32_t mode,
		     uint32_t uid, uint32_t gid, int64_t mtime,
		     uint32_t major, uint32_t minor);

// Create a hard link at path pointing to target.
errcode_t e2fs_hardlink(e2fs_t fs, const char *path, const char *target);

#endif // E2FS_H
