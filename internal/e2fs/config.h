// Hand-merged config.h for e2fsprogs libext2fs, works on both Linux and macOS.
// Derived from ./configure output on Linux aarch64 (Debian bookworm) and
// macOS arm64 (15.x). Only platform-differing defines use #ifdef guards.

#ifndef E2FS_CONFIG_H
#define E2FS_CONFIG_H

#define CONFIG_BUILD_FINDFS 1
#define CONFIG_MMP 1
#define CONFIG_TDB 1
#define CONFIG_TESTIO_DEBUG 1
#define ENABLE_BMAP_STATS 1

#define HAVE_BACKTRACE 1
#define HAVE_DIRENT_H 1
#define HAVE_DLOPEN 1
#define HAVE_ERRNO_H 1
#define HAVE_EXECINFO_H 1
#define HAVE_FCHOWN 1
#define HAVE_FCNTL 1
#define HAVE_FDATASYNC 1
#define HAVE_FSTAT64 1
#define HAVE_FSYNC 1
#define HAVE_FUTIMES 1
#define HAVE_GETCWD 1
#define HAVE_GETDTABLESIZE 1
#define HAVE_GETENTROPY 1
#define HAVE_GETHOSTNAME 1
#define HAVE_GETOPT_H 1
#define HAVE_GETPWUID_R 1
#define HAVE_GETRLIMIT 1
#define HAVE_GETRUSAGE 1
#define HAVE_INTPTR_T 1
#define HAVE_INTTYPES_H 1
#define HAVE_JRAND48 1
#define HAVE_MBSTOWCS 1
#define HAVE_MMAP 1
#define HAVE_MOUNT_NODEV 1
#define HAVE_MOUNT_NOSUID 1
#define HAVE_MSYNC 1
#define HAVE_NANOSLEEP 1
#define HAVE_NETINET_IN_H 1
#define HAVE_NET_IF_H 1
#define HAVE_PATHCONF 1
#define HAVE_PATHS_H 1
#define HAVE_POSIX_MEMALIGN 1
#define HAVE_PREAD 1
#define HAVE_PTHREAD 1
#define HAVE_PTHREAD_H 1
#define HAVE_PTHREAD_PRIO_INHERIT 1
#define HAVE_PWRITE 1
#define HAVE_QSORT_R 1
#define HAVE_RECLEN_DIRENT 1
#define HAVE_SEMAPHORE_H 1
#define HAVE_SETJMP_H 1
#define HAVE_SIGNAL_H 1
#define HAVE_SNPRINTF 1
#define HAVE_SRANDOM 1
#define HAVE_STDARG_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDIO_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STPCPY 1
#define HAVE_STRCASECMP 1
#define HAVE_STRDUP 1
#define HAVE_STRINGS_H 1
#define HAVE_STRING_H 1
#define HAVE_STRNLEN 1
#define HAVE_STRPTIME 1
#define HAVE_STRTOULL 1
#define HAVE_SYSCONF 1
#define HAVE_SYS_FILE_H 1
#define HAVE_SYS_IOCTL_H 1
#define HAVE_SYS_MMAN_H 1
#define HAVE_SYS_MOUNT_H 1
#define HAVE_SYS_RANDOM_H 1
#define HAVE_SYS_RESOURCE_H 1
#define HAVE_SYS_SELECT_H 1
#define HAVE_SYS_SOCKET_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_SYSCALL_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_SYS_UN_H 1
#define HAVE_SYS_WAIT_H 1
#define HAVE_SYS_XATTR_H 1
#define HAVE_TERMIOS_H 1
#define HAVE_TYPE_SSIZE_T 1
#define HAVE_UNISTD_H 1
#define HAVE_USLEEP 1
#define HAVE_UTIME 1
#define HAVE_UTIMES 1
#define HAVE_UTIME_H 1
#define HAVE_VALLOC 1
#define HAVE_VPRINTF 1
#define HAVE_WCHAR_H 1

// ---- platform-specific ----

#ifdef __linux__
#define HAVE_EXT2_IOCTLS 1
#define HAVE_FALLOCATE 1
#define HAVE_FALLOCATE64 1
#define HAVE_FSMAP_SIZEOF 1
#define HAVE_FTRUNCATE64 1
#define HAVE_GETRANDOM 1
#define HAVE_GNU_QSORT_R 1
#define HAVE_LINUX_FALLOC_H 1
#define HAVE_LINUX_FD_H 1
#define HAVE_LINUX_FSMAP_H 1
#define HAVE_LINUX_FSVERITY_H 1
#define HAVE_LINUX_LOOP_H 1
#define HAVE_LINUX_MAJOR_H 1
#define HAVE_LINUX_TYPES_H 1
#define HAVE_LLISTXATTR 1
#define HAVE_LSEEK64 1
#define HAVE_LSEEK64_PROTOTYPE 1
#define HAVE_MALLINFO 1
#define HAVE_MALLINFO2 1
#define HAVE_MALLOC_H 1
#define HAVE_MEMALIGN 1
#define HAVE_MEMPCPY 1
#define HAVE_MNTENT_H 1
#define HAVE_OPEN64 1
#define HAVE_POSIX_FADVISE 1
#define HAVE_POSIX_FADVISE64 1
#define HAVE_PRCTL 1
#define HAVE_PREAD64 1
#define HAVE_PWRITE64 1
#define HAVE_SECURE_GETENV 1
#define HAVE_SETMNTENT 1
#define HAVE_SETRESGID 1
#define HAVE_SETRESUID 1
#define HAVE_STRUCT_STAT_ST_ATIM 1
#define HAVE_SYNC_FILE_RANGE 1
#define HAVE_SYS_PRCTL_H 1
#define HAVE_SYS_SYSMACROS_H 1
#define HAVE_TERMIO_H 1
#endif

#ifdef __APPLE__
#define HAVE_BSD_QSORT_R 1
#define HAVE_CFLOCALECOPYPREFERREDLANGUAGES 1
#define HAVE_CFPREFERENCESCOPYAPPVALUE 1
#define HAVE_CHFLAGS 1
#define HAVE_GETMNTINFO 1
#define HAVE_NET_IF_DL_H 1
#define HAVE_OPTRESET 1
#define HAVE_SA_LEN 1
#define HAVE_STAT_FLAGS 1
#define HAVE_SYS_ACL_H 1
#define HAVE_SYS_DISK_H 1
#define HAVE_SYS_SOCKIO_H 1
#define _INTL_REDIRECT_MACROS 1
#endif

#define PACKAGE "e2fsprogs"
#define PACKAGE_BUGREPORT ""
#define PACKAGE_NAME ""
#define PACKAGE_STRING ""
#define PACKAGE_TARNAME ""
#define PACKAGE_URL ""
#define PACKAGE_VERSION ""
#define VERSION "0.14.1"

#define SIZEOF_INT 4
#define SIZEOF_LONG 8
#define SIZEOF_LONG_LONG 8
#define SIZEOF_OFF_T 8
#define SIZEOF_SHORT 2
#define SIZEOF_TIME_T 8
#define SIZEOF_VOID_P 8

#define STDC_HEADERS 1

#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
#ifndef _DARWIN_C_SOURCE
# define _DARWIN_C_SOURCE 1
#endif
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif

// dirpaths.h replacement — we don't use mke2fs.conf or NLS.
#define LOCALEDIR       "/usr/local/share/locale"
#define ROOT_SYSCONFDIR "/usr/local/etc"

#endif // E2FS_CONFIG_H
