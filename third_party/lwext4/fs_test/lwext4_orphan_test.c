/*
 * Orphan inode list tests.
 *
 * Tests the on-disk orphan inode list used for crash-safe
 * unlink-while-open semantics. Exercises:
 *   - orphan_add / orphan_remove
 *   - unlink_orphan (unlink that defers inode freeing)
 *   - free_orphan (deferred inode free on last close)
 *   - orphan_recover (crash recovery)
 *   - data integrity through the orphan lifecycle
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <inttypes.h>
#include <sys/time.h>

#include <ext4.h>
#include <ext4_errno.h>

#include "../blockdev/linux/file_dev.h"
#include "common/test_lwext4.h"

/* Timing stubs required by test_lwext4.c common code. */
uint32_t tim_get_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint32_t)(tv.tv_sec * 1000 + tv.tv_usec / 1000);
}
uint64_t tim_get_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}
void tim_wait_ms(uint32_t v) { (void)v; }

static struct ext4_io_stats dummy_stats;
void io_timings_clear(void) { memset(&dummy_stats, 0, sizeof(dummy_stats)); }
const struct ext4_io_stats *io_timings_get(uint32_t time_sum_ms) {
    (void)time_sum_ms;
    return &dummy_stats;
}

#include "inode_ops.h"

#define MP "/mp/"
#define ROOT_INO 2

static int failures = 0;
static int passes = 0;

#define ASSERT_EQ(a, b, msg) do { \
    if ((a) != (b)) { \
        printf("  FAIL: %s: expected %d, got %d\n", msg, (int)(b), (int)(a)); \
        failures++; \
        return; \
    } else { \
        passes++; \
    } \
} while(0)

#define ASSERT_NEQ(a, b, msg) do { \
    if ((a) == (b)) { \
        printf("  FAIL: %s: got unexpected %d\n", msg, (int)(a)); \
        failures++; \
        return; \
    } else { \
        passes++; \
    } \
} while(0)

#define ASSERT_TRUE(cond, msg) do { \
    if (!(cond)) { \
        printf("  FAIL: %s\n", msg); \
        failures++; \
        return; \
    } else { \
        passes++; \
    } \
} while(0)

/* Shorthand for inode_unlink_orphan with strlen. */
#define UNLINK_ORPHAN(name, child_out) \
    inode_unlink_orphan(mp, ROOT_INO, name, strlen(name), child_out)

/* Shorthand for inode_lookup with strlen. */
#define LOOKUP(name, ino_out) \
    inode_lookup(mp, ROOT_INO, name, strlen(name), ino_out)

/* Shorthand for inode_link with strlen. */
#define LINK(ino, name) \
    inode_link(mp, ino, ROOT_INO, name, strlen(name))

static struct ext4_mountpoint *mp;

static uint32_t get_orphan_head(void) {
    return inode_orphan_head(mp);
}

/* Helper: create a file with known data, return inode number. */
static int create_file(const char *name, uint8_t fill, uint32_t size,
                       uint32_t *ino_out) {
    char path[256];
    snprintf(path, sizeof(path), MP "%s", name);

    ext4_file f;
    int r = ext4_fopen(&f, path, "wb");
    if (r != EOK) return r;

    uint8_t buf[4096];
    memset(buf, fill, sizeof(buf));

    uint32_t written = 0;
    while (written < size) {
        size_t chunk = size - written;
        if (chunk > sizeof(buf)) chunk = sizeof(buf);
        size_t wb;
        r = ext4_fwrite(&f, buf, chunk, &wb);
        if (r != EOK) { ext4_fclose(&f); return r; }
        written += wb;
    }

    ext4_fclose(&f);
    return LOOKUP(name, ino_out);
}

/* ---------- Test: basic orphan_add / orphan_remove ---------- */

static void test_orphan_add_remove(void) {
    printf("test_orphan_add_remove... ");

    uint32_t ino;
    ASSERT_EQ(create_file("orphan_ar.bin", 0xAA, 4096, &ino), EOK, "create file");
    ASSERT_EQ(get_orphan_head(), 0, "orphan list initially empty");

    ASSERT_EQ(inode_orphan_add(mp, ino), EOK, "orphan_add");
    ASSERT_EQ(get_orphan_head(), ino, "inode is head of orphan list");

    ASSERT_EQ(inode_orphan_remove(mp, ino), EOK, "orphan_remove");
    ASSERT_EQ(get_orphan_head(), 0, "orphan list empty after remove");

    ext4_fremove(MP "orphan_ar.bin");
    printf("OK\n");
}

/* ---------- Test: multiple orphans form a linked list ---------- */

static void test_orphan_multiple(void) {
    printf("test_orphan_multiple... ");

    uint32_t ino1, ino2, ino3;
    ASSERT_EQ(create_file("orph1.bin", 0x11, 4096, &ino1), EOK, "create 1");
    ASSERT_EQ(create_file("orph2.bin", 0x22, 4096, &ino2), EOK, "create 2");
    ASSERT_EQ(create_file("orph3.bin", 0x33, 4096, &ino3), EOK, "create 3");

    /* Add all three: list should be ino3 -> ino2 -> ino1 -> 0. */
    ASSERT_EQ(inode_orphan_add(mp, ino1), EOK, "add 1");
    ASSERT_EQ(inode_orphan_add(mp, ino2), EOK, "add 2");
    ASSERT_EQ(inode_orphan_add(mp, ino3), EOK, "add 3");
    ASSERT_EQ(get_orphan_head(), ino3, "head is ino3");

    /* Remove middle element (ino2). */
    ASSERT_EQ(inode_orphan_remove(mp, ino2), EOK, "remove middle");
    ASSERT_EQ(get_orphan_head(), ino3, "head still ino3");

    /* Remove head (ino3). */
    ASSERT_EQ(inode_orphan_remove(mp, ino3), EOK, "remove head");
    ASSERT_EQ(get_orphan_head(), ino1, "head now ino1");

    /* Remove last (ino1). */
    ASSERT_EQ(inode_orphan_remove(mp, ino1), EOK, "remove last");
    ASSERT_EQ(get_orphan_head(), 0, "list empty");

    ext4_fremove(MP "orph1.bin");
    ext4_fremove(MP "orph2.bin");
    ext4_fremove(MP "orph3.bin");
    printf("OK\n");
}

/* ---------- Test: remove nonexistent returns ENOENT ---------- */

static void test_orphan_remove_nonexistent(void) {
    printf("test_orphan_remove_nonexistent... ");
    ASSERT_EQ(inode_orphan_remove(mp, 99999), ENOENT, "remove nonexistent returns ENOENT");
    printf("OK\n");
}

/* ---------- Test: unlink_orphan keeps data accessible ---------- */

static void test_unlink_orphan_data_intact(void) {
    printf("test_unlink_orphan_data_intact... ");

    uint32_t ino;
    ASSERT_EQ(create_file("doomed.bin", 0xDE, 8192, &ino), EOK, "create");

    /* Open file for reading (simulate having an open fd). */
    ext4_file f;
    ASSERT_EQ(ext4_fopen(&f, MP "doomed.bin", "rb"), EOK, "open for read");

    /* Unlink via orphan path. */
    uint32_t unlinked_ino;
    ASSERT_EQ(UNLINK_ORPHAN("doomed.bin", &unlinked_ino), EOK, "unlink_orphan");
    ASSERT_EQ(unlinked_ino, ino, "returned correct ino");
    ASSERT_EQ(get_orphan_head(), ino, "inode on orphan list");

    /* File should not be findable by name anymore. */
    uint32_t lookup_ino;
    ASSERT_NEQ(LOOKUP("doomed.bin", &lookup_ino), EOK, "lookup fails after unlink");

    /* But we can still read data through the open handle. */
    uint8_t buf[8192];
    size_t rb;
    ASSERT_EQ(ext4_fread(&f, buf, sizeof(buf), &rb), EOK, "read after unlink");
    ASSERT_EQ((int)rb, 8192, "read full data");

    bool data_ok = true;
    for (int i = 0; i < 8192; i++) {
        if (buf[i] != 0xDE) { data_ok = false; break; }
    }
    ASSERT_TRUE(data_ok, "data intact after unlink");

    ext4_fclose(&f);

    ASSERT_EQ(inode_free_orphan(mp, ino), EOK, "free_orphan");
    ASSERT_EQ(get_orphan_head(), 0, "orphan list empty after free");
    printf("OK\n");
}

/* ---------- Test: unlink with hardlink doesn't orphan ---------- */

static void test_unlink_orphan_with_hardlink(void) {
    printf("test_unlink_orphan_with_hardlink... ");

    uint32_t ino;
    ASSERT_EQ(create_file("orig.bin", 0xBB, 4096, &ino), EOK, "create");
    ASSERT_EQ(LINK(ino, "link.bin"), EOK, "hardlink");

    /* Unlink original — links_count goes from 2 to 1, should NOT orphan. */
    uint32_t child_ino;
    ASSERT_EQ(UNLINK_ORPHAN("orig.bin", &child_ino), EOK, "unlink_orphan");
    ASSERT_EQ(child_ino, ino, "correct ino");
    ASSERT_EQ(get_orphan_head(), 0, "no orphan (still has link)");

    /* File still accessible via hardlink. */
    uint32_t lookup_ino;
    ASSERT_EQ(LOOKUP("link.bin", &lookup_ino), EOK, "lookup via hardlink");
    ASSERT_EQ(lookup_ino, ino, "same inode");

    ext4_fremove(MP "link.bin");
    printf("OK\n");
}

/* ---------- Test: crash recovery frees orphaned inodes ---------- */

static void test_orphan_crash_recovery(void) {
    printf("test_orphan_crash_recovery... ");

    uint32_t ino1, ino2;
    ASSERT_EQ(create_file("crash1.bin", 0xC1, 16384, &ino1), EOK, "create 1");
    ASSERT_EQ(create_file("crash2.bin", 0xC2, 16384, &ino2), EOK, "create 2");

    uint32_t child;
    ASSERT_EQ(UNLINK_ORPHAN("crash1.bin", &child), EOK, "unlink 1");
    ASSERT_EQ(UNLINK_ORPHAN("crash2.bin", &child), EOK, "unlink 2");
    ASSERT_NEQ(get_orphan_head(), 0, "orphan list not empty");

    struct ext4_mount_stats stats_before;
    ext4_mount_point_stats(MP, &stats_before);

    ASSERT_EQ(inode_orphan_recover(mp), EOK, "orphan_recover");
    ASSERT_EQ(get_orphan_head(), 0, "orphan list empty after recovery");

    struct ext4_mount_stats stats_after;
    ext4_mount_point_stats(MP, &stats_after);
    ASSERT_TRUE(stats_after.free_inodes_count > stats_before.free_inodes_count,
                "free inodes increased after recovery");
    printf("OK\n");
}

/* ---------- Test: free_orphan reclaims blocks ---------- */

static void test_free_orphan_reclaims_blocks(void) {
    printf("test_free_orphan_reclaims_blocks... ");

    uint32_t ino;
    ASSERT_EQ(create_file("big.bin", 0xFF, 1024 * 1024, &ino), EOK, "create 1MB file");

    struct ext4_mount_stats stats_before;
    ext4_mount_point_stats(MP, &stats_before);

    uint32_t child;
    ASSERT_EQ(UNLINK_ORPHAN("big.bin", &child), EOK, "unlink");
    ASSERT_EQ(inode_free_orphan(mp, ino), EOK, "free_orphan");

    struct ext4_mount_stats stats_after;
    ext4_mount_point_stats(MP, &stats_after);

    uint32_t reclaimed = stats_after.free_blocks_count - stats_before.free_blocks_count;
    ASSERT_TRUE(reclaimed >= 200, "reclaimed significant blocks");
    printf("OK\n");
}

/* ---------- Test: orphan list survives unmount/remount ---------- */

static void test_orphan_survives_remount(void) {
    printf("test_orphan_survives_remount... ");

    uint32_t ino;
    ASSERT_EQ(create_file("persist.bin", 0xAB, 4096, &ino), EOK, "create");

    uint32_t child;
    ASSERT_EQ(UNLINK_ORPHAN("persist.bin", &child), EOK, "unlink");
    ASSERT_EQ(get_orphan_head(), ino, "on orphan list");

    /* Flush and unmount. */
    ext4_cache_write_back(MP, 0);
    ext4_cache_flush(MP);
    ASSERT_EQ(ext4_umount(MP), EOK, "umount");

    /* Remount (device stays registered across umount/mount). */
    ASSERT_EQ(ext4_mount("ext4_fs", MP, false), EOK, "remount");
    ext4_cache_write_back(MP, 1);

    mp = inode_get_mp(MP);
    ASSERT_TRUE(mp != NULL, "got mp after remount");

    ASSERT_EQ(get_orphan_head(), ino, "orphan persisted across remount");

    ASSERT_EQ(inode_orphan_recover(mp), EOK, "recover after remount");
    ASSERT_EQ(get_orphan_head(), 0, "list empty after recovery");
    printf("OK\n");
}

/* ---------- Test: unlink directory via unlink_orphan fails ---------- */

static void test_unlink_orphan_rejects_directory(void) {
    printf("test_unlink_orphan_rejects_directory... ");

    ASSERT_EQ(ext4_dir_mk(MP "testdir"), EOK, "mkdir");

    uint32_t child;
    ASSERT_EQ(UNLINK_ORPHAN("testdir", &child), EISDIR, "rejects directory");

    ext4_dir_rm(MP "testdir");
    printf("OK\n");
}

/* ---------- Main ---------- */

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Usage: %s <ext4-image>\n", argv[0]);
        return 1;
    }

    file_dev_name_set(argv[1]);
    struct ext4_blockdev *bd = file_dev_get();
    if (!bd) {
        printf("Failed to get block device\n");
        return 1;
    }

    int r = ext4_device_register(bd, "ext4_fs");
    if (r != EOK) {
        printf("ext4_device_register: %d\n", r);
        return 1;
    }

    r = ext4_mount("ext4_fs", MP, false);
    if (r != EOK) {
        printf("ext4_mount: %d\n", r);
        return 1;
    }

    ext4_cache_write_back(MP, 1);

    mp = inode_get_mp(MP);
    if (!mp) {
        printf("Failed to get mountpoint\n");
        return 1;
    }

    printf("\n=== Orphan inode tests ===\n\n");

    test_orphan_add_remove();
    test_orphan_multiple();
    test_orphan_remove_nonexistent();
    test_unlink_orphan_data_intact();
    test_unlink_orphan_with_hardlink();
    test_orphan_crash_recovery();
    test_free_orphan_reclaims_blocks();
    test_orphan_survives_remount();
    test_unlink_orphan_rejects_directory();

    printf("\n=== Results: %d passed, %d failed ===\n\n", passes, failures);

    ext4_cache_write_back(MP, 0);
    ext4_umount(MP);

    return failures > 0 ? 1 : 0;
}
