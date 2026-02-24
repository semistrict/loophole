#include "readdir_bridge.h"
#include "_cgo_export.h"

typedef struct {
    uintptr_t ctx;
} bridge_state;

static int readdir_cb(const struct inode_dirent *de, void *arg) {
    bridge_state *s = (bridge_state *)arg;
    return goReaddirCallback((struct inode_dirent *)de, s->ctx);
}

int readdir_bridge(struct ext4_mountpoint *mp, uint32_t dir_ino, uintptr_t ctx) {
    bridge_state s = { .ctx = ctx };
    return inode_readdir(mp, dir_ino, readdir_cb, &s);
}
