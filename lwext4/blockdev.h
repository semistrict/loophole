#ifndef LOOPHOLE_BLOCKDEV_H
#define LOOPHOLE_BLOCKDEV_H

#include <ext4_blockdev.h>

// create_blockdev allocates an ext4_blockdev + ext4_blockdev_iface + buffer
// that routes I/O callbacks through the Go handle map. The handle is stored
// in bdif->p_user as a uintptr_t.
struct ext4_blockdev *create_blockdev(int handle, uint32_t ph_bsize, uint64_t ph_bcnt);

// destroy_blockdev frees everything allocated by create_blockdev.
void destroy_blockdev(struct ext4_blockdev *bdev);

#endif // LOOPHOLE_BLOCKDEV_H
