/*
 * Memory File - mmap-based arena allocator with automatic growth
 * 
 * The file header IS the arena struct (packed, at offset 0).
 * All allocations return offsets, not pointers.
 * Pointers become invalid after mremap, offsets remain valid.
 *
 * Originally from biscuit/server; adapted for MCP memory server.
 */

#ifndef MEMORYFILE_H
#define MEMORYFILE_H

#include <stdint.h>
#include <stddef.h>

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

#define MEMFILE_MAGIC   0x4D454D46  /* "MEMF" */
#define MEMFILE_VERSION 1

/* File header - lives at offset 0, IS the arena */
typedef struct __attribute__((packed)) {
    u32 magic;
    u32 version;
    u64 file_size;          /* Current file size */
    u64 allocated;          /* Bump pointer: next allocation from end */
    u64 free_list_head;     /* Offset to first free block (0=none) */
} memfile_header_t;

/* Free block header - lives IN the free space it describes */
typedef struct __attribute__((packed)) {
    u64 size;               /* Size of this free block (including header) */
    u64 next;               /* Offset to next free block (0=none) */
} memfile_free_t;

/* Allocation header - immediately before each allocated block */
typedef struct __attribute__((packed)) {
    u64 size;               /* Size of allocation (including this header) */
} memfile_alloc_t;

/* Handle for working with memory file */
typedef struct {
    int fd;
    char *path;
    void *mmap_base;        /* Base address of mmap */
    size_t mmap_size;       /* Current mmap size */
    memfile_header_t *header;  /* Points to offset 0 */
    int closed;             /* Set after close to prevent double-free */
} memfile_t;

/* Lifecycle */
memfile_t *memfile_open(const char *path, size_t initial_size);
void       memfile_close(memfile_t *mf);
void       memfile_sync(memfile_t *mf);

/* Allocation - returns offset from file start (0 = failed) */
u64  memfile_alloc(memfile_t *mf, u64 size);
void memfile_free(memfile_t *mf, u64 offset);

/* Defragmentation */
void memfile_coalesce(memfile_t *mf);

/* Direct read/write at offset */
int memfile_read(memfile_t *mf, u64 offset, void *buf, u64 len);
int memfile_write(memfile_t *mf, u64 offset, const void *buf, u64 len);

/* Convert offset to pointer (CAUTION: invalid after alloc that triggers remap) */
void *memfile_ptr(memfile_t *mf, u64 offset);

/* Refresh mapping if the file was grown by another process */
int memfile_refresh(memfile_t *mf);

/* Concurrency - POSIX flock on the underlying fd */
int memfile_lock_shared(memfile_t *mf);
int memfile_lock_exclusive(memfile_t *mf);
int memfile_unlock(memfile_t *mf);

#endif /* MEMORYFILE_H */
