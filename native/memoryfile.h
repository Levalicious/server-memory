/*
 * Memory File v3 - mmap-backed arena with a Cartesian-tree free allocator.
 *
 * The header IS the arena descriptor (packed, at offset 0). All allocations are
 * OFFSETS, not pointers (stable across mremap). Live blocks carry ZERO metadata;
 * free blocks hold a Cartesian-tree node (BST by address, max-heap by size) in
 * their own bytes => O(log n) best-fit + continuous coalescing, zero live overhead.
 *
 * Deallocation is SIZED: the caller passes the allocation size back to free()
 * (sized-deallocation; this is what removes the per-allocation size header).
 *
 * Format v3 is NOT compatible with v1: memfile_open refuses version != 3.
 */
#ifndef MEMORYFILE_H
#define MEMORYFILE_H

#include <stdint.h>
#include <stddef.h>

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

#define MEMFILE_MAGIC   0x4D454D46u  /* "MEMF" */
#define MEMFILE_VERSION 3

/* File header at offset 0. Padded to 64B so `allocated` starts 32-aligned. */
typedef struct __attribute__((packed)) {
    u32 magic;
    u32 version;
    u64 file_size;      /* current file/mmap size */
    u64 allocated;      /* bump cursor: next bump allocation starts here */
    u64 free_root;      /* Cartesian-tree root offset (0 = empty) */
    u64 free_bytes;     /* total free bytes (sum of free blocks); O(1) maintained */
    u64 free_count;     /* number of free blocks; O(1) maintained */
    u64 _pad[2];
} memfile_header_t;

typedef struct {
    int fd;
    char *path;
    void *mmap_base;
    size_t mmap_size;
    memfile_header_t *header;  /* points to offset 0 */
    int closed;
} memfile_t;

/* Lifecycle */
memfile_t *memfile_open(const char *path, size_t initial_size);
void       memfile_close(memfile_t *mf);
void       memfile_sync(memfile_t *mf);

/* Allocation - returns an offset from file start (0 = failed). Live blocks carry no header. */
u64  memfile_alloc(memfile_t *mf, u64 size);
/* SIZED free - pass the same `size` that was requested at alloc time. */
void memfile_free(memfile_t *mf, u64 offset, u64 size);
/* Coalescing is CONTINUOUS (performed in free); kept as a no-op for ABI stability. */
void memfile_coalesce(memfile_t *mf);

/* Direct read/write at offset */
int memfile_read(memfile_t *mf, u64 offset, void *buf, u64 len);
int memfile_write(memfile_t *mf, u64 offset, const void *buf, u64 len);

/* Convert offset to pointer (CAUTION: invalid after an alloc that triggers remap) */
void *memfile_ptr(memfile_t *mf, u64 offset);

/* Refresh mapping if the file was grown by another process */
int memfile_refresh(memfile_t *mf);

/* Concurrency - POSIX flock on the underlying fd */
int memfile_lock_shared(memfile_t *mf);
int memfile_lock_exclusive(memfile_t *mf);
int memfile_unlock(memfile_t *mf);

#endif /* MEMORYFILE_H */
