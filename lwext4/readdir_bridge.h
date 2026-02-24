#ifndef LOOPHOLE_READDIR_BRIDGE_H
#define LOOPHOLE_READDIR_BRIDGE_H

#include <stdint.h>
#include "inode_ops.h"

// Implemented in readdir_bridge.c (calls into Go).
int readdir_bridge(struct ext4_mountpoint *mp, uint32_t dir_ino, uintptr_t ctx);

#endif
