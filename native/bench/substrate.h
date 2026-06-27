/*
 * mf substrate - shared file/mmap/growth core for memfile v3 allocator benches.
 *
 * Owns: fd, mmap, mremap growth, the fixed header. Does NOT own the allocator:
 * `free_root` is interpreted by the allocator variant (free-list head, or tree root).
 * All allocations are OFFSETS from the file start. Pointers are invalid after growth.
 */
#ifndef MF_SUBSTRATE_H
#define MF_SUBSTRATE_H

#include <stdint.h>
#include <stddef.h>

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

#define MF_MAGIC   0x4D454D46u  /* "MEMF" */
#define MF_VERSION 3

/* Fixed header at offset 0. Padded to 64B so `allocated` starts 32-aligned (tree variant). */
typedef struct __attribute__((packed)) {
    u32 magic;
    u32 version;
    u64 file_size;    /* current file/mmap size */
    u64 allocated;    /* bump cursor: next bump allocation starts here */
    u64 free_root;    /* allocator-interpreted: free-list head OR tree root (0 = empty) */
    u64 free_bytes;   /* total bytes currently free (sum of free blocks); O(1) maintained */
    u64 free_count;   /* number of free blocks/nodes; O(1) maintained */
    u64 _pad[2];      /* pad to 64B */
} mf_header_t;

typedef struct {
    int    fd;
    char  *path;
    void  *base;      /* mmap base */
    size_t size;      /* mmap size */
    mf_header_t *hdr; /* == base */
    int    closed;
} mf_t;

mf_t *mf_open(const char *path, size_t initial_size);
void  mf_close(mf_t *mf);
void  mf_sync(mf_t *mf);

/* Ensure `need` bytes are available above `allocated`; may remap (invalidates pointers). 0 ok, -1 fail. */
int   mf_ensure(mf_t *mf, u64 need);

static inline void *mf_ptr(mf_t *mf, u64 off) {
    return off ? (u8 *)mf->base + off : (void *)0;
}

#endif /* MF_SUBSTRATE_H */
