#include "blockdev.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

// Go-exported callbacks (defined in cgo.go).
extern int goBlockdevOpen(int handle);
extern int goBlockdevRead(int handle, void *buf, uint64_t blk_id, uint32_t blk_cnt, uint32_t blk_size);
extern int goBlockdevWrite(int handle, const void *buf, uint64_t blk_id, uint32_t blk_cnt, uint32_t blk_size);
extern int goBlockdevClose(int handle);

static int bdev_open(struct ext4_blockdev *bdev) {
    int handle = (int)(uintptr_t)bdev->bdif->p_user;
    return goBlockdevOpen(handle);
}

static int bdev_bread(struct ext4_blockdev *bdev, void *buf,
                      uint64_t blk_id, uint32_t blk_cnt) {
    int handle = (int)(uintptr_t)bdev->bdif->p_user; // XXX: why is this casted to an int? why do the goBlockdevOpen etc functions take an int?
    return goBlockdevRead(handle, buf, blk_id, blk_cnt, bdev->bdif->ph_bsize);
}

static int bdev_bwrite(struct ext4_blockdev *bdev, const void *buf,
                       uint64_t blk_id, uint32_t blk_cnt) {
    int handle = (int)(uintptr_t)bdev->bdif->p_user;
    return goBlockdevWrite(handle, buf, blk_id, blk_cnt, bdev->bdif->ph_bsize);
}

static int bdev_close(struct ext4_blockdev *bdev) {
    int handle = (int)(uintptr_t)bdev->bdif->p_user;
    return goBlockdevClose(handle);
}

struct ext4_blockdev *create_blockdev(int handle, uint32_t ph_bsize, uint64_t ph_bcnt) {
    struct ext4_blockdev *bdev = calloc(1, sizeof(struct ext4_blockdev));
    if (!bdev) return NULL;

    struct ext4_blockdev_iface *bdif = calloc(1, sizeof(struct ext4_blockdev_iface));
    if (!bdif) { free(bdev); return NULL; }

    uint8_t *buf = calloc(1, ph_bsize);
    if (!buf) { free(bdif); free(bdev); return NULL; }

    bdif->open = bdev_open;
    bdif->bread = bdev_bread;
    bdif->bwrite = bdev_bwrite;
    bdif->close = bdev_close;
    bdif->lock = NULL; // XXX: what is this?
    bdif->unlock = NULL;
    bdif->ph_bsize = ph_bsize; // XXX: validate the values we pass here make sense
    bdif->ph_bcnt = ph_bcnt;
    bdif->ph_bbuf = buf;
    bdif->p_user = (void *)(uintptr_t)handle;

    bdev->bdif = bdif;
    bdev->part_offset = 0;
    bdev->part_size = (uint64_t)ph_bcnt * (uint64_t)ph_bsize;

    return bdev;
}

void destroy_blockdev(struct ext4_blockdev *bdev) {
    if (!bdev) return;
    if (bdev->bdif) {
        free(bdev->bdif->ph_bbuf);
        free(bdev->bdif);
    }
    free(bdev);
}
