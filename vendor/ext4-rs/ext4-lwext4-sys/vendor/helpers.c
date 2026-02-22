#include <ext4_config.h>
#include <ext4_types.h>
#include <ext4_fs.h>
#include <stddef.h>

size_t lwext4_sizeof_ext4_fs(void) {
    return sizeof(struct ext4_fs);
}
