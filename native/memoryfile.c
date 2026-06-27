/*
 * Memory File v3 - mmap-backed arena, Cartesian-tree free allocator.
 *
 * Substrate (file/mmap/growth/locks) originally from biscuit/server.
 * Allocator: Stephenson "Fast Fits" Cartesian tree resident in the free blocks.
 *   - BST keyed by ADDRESS (a node's own offset) => address-neighbours for coalescing
 *   - max-HEAP keyed by SIZE                       => O(h) lowest-address fit
 * Live blocks carry zero metadata; deallocation is sized; coalescing is continuous.
 */

#include "memoryfile.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>

#ifdef MEMFILE_DOUBLE_FREE_CHECK
#include <stdio.h>   /* test-only: double-free detector diagnostics */
#endif

#define MFC_MIN_BLOCK 32u   /* sizeof {u64 size, left, right, parent} */
#define MFC_QUANTUM   32u   /* alloc granularity == min block: split leftovers stay valid */

/* Cartesian-tree node, resident in a free block's own bytes. */
typedef struct __attribute__((packed)) { u64 size, left, right, parent; } mf_node_t;

/* =========================================================================
 * Pointer conversion
 * ========================================================================= */

void *memfile_ptr(memfile_t *mf, u64 offset) {
    if (offset == 0 || offset >= mf->mmap_size) return NULL;
    return (u8 *)mf->mmap_base + offset;
}

/* Fast internal accessor (offsets are valid by construction in the allocator). */
static inline mf_node_t *ND(memfile_t *mf, u64 o) { return (mf_node_t *)((u8 *)mf->mmap_base + o); }

static inline u64 round_up32(u64 s) {
    s = (s + (MFC_QUANTUM - 1)) & ~(u64)(MFC_QUANTUM - 1);
    return s < MFC_MIN_BLOCK ? MFC_MIN_BLOCK : s;
}

/* =========================================================================
 * Direct read/write at offset
 * ========================================================================= */

int memfile_read(memfile_t *mf, u64 offset, void *buf, u64 len) {
    if (offset + len > mf->mmap_size) return -1;
    void *src = memfile_ptr(mf, offset);
    if (!src) return -1;
    memcpy(buf, src, len);
    return 0;
}

int memfile_write(memfile_t *mf, u64 offset, const void *buf, u64 len) {
    if (offset + len > mf->mmap_size) return -1;
    void *dst = memfile_ptr(mf, offset);
    if (!dst) return -1;
    memcpy(dst, buf, len);
    return 0;
}

/* =========================================================================
 * File growth via mremap
 * ========================================================================= */

static int memfile_remap(memfile_t *mf, size_t new_size) {
    if (ftruncate(mf->fd, new_size) < 0) return -1;
#ifdef __linux__
    void *new_base = mremap(mf->mmap_base, mf->mmap_size, new_size, MREMAP_MAYMOVE);
    if (new_base == MAP_FAILED) return -1;
#else
    munmap(mf->mmap_base, mf->mmap_size);
    void *new_base = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
    if (new_base == MAP_FAILED) return -1;
#endif
    mf->mmap_base = new_base;
    mf->mmap_size = new_size;
    mf->header = (memfile_header_t *)new_base;
    mf->header->file_size = new_size;
    return 0;
}

static int memfile_ensure_space(memfile_t *mf, u64 needed) {
    if (mf->header->allocated + needed <= mf->header->file_size) return 0;
    size_t new_size = mf->mmap_size * 2;
    if (new_size < mf->header->allocated + needed) new_size = mf->header->allocated + needed + 4096;
    return memfile_remap(mf, new_size);
}

/* =========================================================================
 * Cartesian-tree allocator
 * ========================================================================= */

/* Rotate child x above its parent, preserving BST order; fix parent links + root. */
static void mf_rot(memfile_t *mf, u64 x) {
    u64 p = ND(mf, x)->parent;
    u64 g = ND(mf, p)->parent;
    if (ND(mf, p)->left == x) {                 /* right rotation */
        u64 b = ND(mf, x)->right;
        ND(mf, x)->right = p; ND(mf, p)->parent = x;
        ND(mf, p)->left = b; if (b) ND(mf, b)->parent = p;
    } else {                                    /* left rotation */
        u64 b = ND(mf, x)->left;
        ND(mf, x)->left = p; ND(mf, p)->parent = x;
        ND(mf, p)->right = b; if (b) ND(mf, b)->parent = p;
    }
    ND(mf, x)->parent = g;
    if (!g) mf->header->free_root = x;
    else if (ND(mf, g)->left == p) ND(mf, g)->left = x; else ND(mf, g)->right = x;
}

static void mf_insert(memfile_t *mf, u64 off, u64 size) {
    ND(mf, off)->size = size; ND(mf, off)->left = 0; ND(mf, off)->right = 0; ND(mf, off)->parent = 0;
    mf->header->free_bytes += size; mf->header->free_count += 1;

    u64 root = mf->header->free_root;
    if (!root) { mf->header->free_root = off; return; }

    u64 cur = root, par = 0;
    while (cur) { par = cur; cur = (off < cur) ? ND(mf, cur)->left : ND(mf, cur)->right; }
    ND(mf, off)->parent = par;
    if (off < par) ND(mf, par)->left = off; else ND(mf, par)->right = off;

    while (ND(mf, off)->parent && ND(mf, ND(mf, off)->parent)->size < ND(mf, off)->size)
        mf_rot(mf, off);
}

static void mf_remove(memfile_t *mf, u64 off) {
    for (;;) {
        u64 l = ND(mf, off)->left, r = ND(mf, off)->right;
        if (!l && !r) break;
        u64 bigger = !l ? r : !r ? l : (ND(mf, l)->size >= ND(mf, r)->size ? l : r);
        mf_rot(mf, bigger);
    }
    u64 p = ND(mf, off)->parent;
    if (!p) mf->header->free_root = 0;
    else if (ND(mf, p)->left == off) ND(mf, p)->left = 0; else ND(mf, p)->right = 0;
    mf->header->free_bytes -= ND(mf, off)->size; mf->header->free_count -= 1;
}

/* Lowest-address free block with size >= n, in O(h). */
static u64 mf_fit(memfile_t *mf, u64 n) {
    u64 cur = mf->header->free_root;
    while (cur) {
        u64 l = ND(mf, cur)->left;
        if (l && ND(mf, l)->size >= n) { cur = l; continue; }
        if (ND(mf, cur)->size >= n) return cur;
        cur = ND(mf, cur)->right;
    }
    return 0;
}

static u64 mf_succ(memfile_t *mf, u64 off) {   /* smallest address > off */
    u64 cur = mf->header->free_root, s = 0;
    while (cur) { if (cur > off) { s = cur; cur = ND(mf, cur)->left; } else cur = ND(mf, cur)->right; }
    return s;
}
static u64 mf_pred(memfile_t *mf, u64 off) {   /* largest address < off */
    u64 cur = mf->header->free_root, p = 0;
    while (cur) { if (cur < off) { p = cur; cur = ND(mf, cur)->right; } else cur = ND(mf, cur)->left; }
    return p;
}

u64 memfile_alloc(memfile_t *mf, u64 size) {
    u64 n = round_up32(size);
    u64 b = mf_fit(mf, n);
    if (b) {
        u64 bsz = ND(mf, b)->size;
        mf_remove(mf, b);
        u64 rem = bsz - n;                      /* 0 or >= 32 */
        if (rem >= MFC_MIN_BLOCK) mf_insert(mf, b + n, rem);
        return b;
    }
    if (memfile_ensure_space(mf, n) < 0) return 0;
    u64 off = mf->header->allocated;
    mf->header->allocated += n;
    return off;
}

#ifdef MEMFILE_DOUBLE_FREE_CHECK
/* Test-only double-free / overlapping-free detector. NEVER compiled into the
 * published binary (binding.gyp does not define MEMFILE_DOUBLE_FREE_CHECK), so
 * the production allocator stays clean (Principle_FixCauseNotChecks). A valid
 * free targets an entirely ALLOCATED range, so it must intersect no free block. */
static u64 mf_node_at(memfile_t *mf, u64 off) {
    u64 cur = mf->header->free_root;
    while (cur) { if (cur == off) return cur; cur = (off < cur) ? ND(mf, cur)->left : ND(mf, cur)->right; }
    return 0;
}
static void mf_assert_allocated(memfile_t *mf, u64 offset, u64 n) {
    const char *why = 0; u64 p, s;
    if (mf_node_at(mf, offset)) why = "offset is already a free block";
    else if ((p = mf_pred(mf, offset)) && p + ND(mf, p)->size > offset) why = "range intersects a preceding free block";
    else if ((s = mf_succ(mf, offset)) && offset + n > s) why = "range intersects a following free block";
    if (why) {
        fprintf(stderr, "\n*** memfile DOUBLE-FREE detected: offset=%llu size=%llu (%s) ***\n",
                (unsigned long long)offset, (unsigned long long)n, why);
        abort();
    }
}
#endif

void memfile_free(memfile_t *mf, u64 offset, u64 size) {
    if (!offset) return;
    u64 n = round_up32(size);
#ifdef MEMFILE_DOUBLE_FREE_CHECK
    mf_assert_allocated(mf, offset, n);
#endif

    /* merge with the immediately-following free block, if physically adjacent */
    u64 s = mf_succ(mf, offset);
    if (s && offset + n == s) { n += ND(mf, s)->size; mf_remove(mf, s); }

    /* merge with the immediately-preceding free block, if physically adjacent */
    u64 p = mf_pred(mf, offset);
    if (p && p + ND(mf, p)->size == offset) { u64 psz = ND(mf, p)->size; mf_remove(mf, p); offset = p; n += psz; }

    mf_insert(mf, offset, n);
}

void memfile_coalesce(memfile_t *mf) { (void)mf; }   /* continuous: nothing to do */

/* =========================================================================
 * Refresh mapping after another process grows the file
 * ========================================================================= */

int memfile_refresh(memfile_t *mf) {
    struct stat st;
    if (fstat(mf->fd, &st) < 0) return -1;
    size_t actual = (size_t)st.st_size;
    if (actual <= mf->mmap_size) return 0;
#ifdef __linux__
    void *nb = mremap(mf->mmap_base, mf->mmap_size, actual, MREMAP_MAYMOVE);
    if (nb == MAP_FAILED) return -1;
#else
    munmap(mf->mmap_base, mf->mmap_size);
    void *nb = mmap(NULL, actual, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
    if (nb == MAP_FAILED) return -1;
#endif
    mf->mmap_base = nb;
    mf->mmap_size = actual;
    mf->header = (memfile_header_t *)nb;
    return 0;
}

/* =========================================================================
 * Concurrency - POSIX flock
 * ========================================================================= */

int memfile_lock_shared(memfile_t *mf)    { return flock(mf->fd, LOCK_SH); }
int memfile_lock_exclusive(memfile_t *mf) { return flock(mf->fd, LOCK_EX); }
int memfile_unlock(memfile_t *mf)         { return flock(mf->fd, LOCK_UN); }

/* =========================================================================
 * Open / close / sync
 * ========================================================================= */

memfile_t *memfile_open(const char *path, size_t initial_size) {
    memfile_t *mf = calloc(1, sizeof(memfile_t));
    if (!mf) return NULL;
    mf->fd = -1;
    mf->path = strdup(path);

    struct stat st;
    int exists = (stat(path, &st) == 0 && st.st_size > 0);

    if (exists) {
        mf->fd = open(path, O_RDWR);
        if (mf->fd < 0) goto fail;
        mf->mmap_size = (size_t)st.st_size;
        mf->mmap_base = mmap(NULL, mf->mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
        if (mf->mmap_base == MAP_FAILED) goto fail_fd;
        mf->header = (memfile_header_t *)mf->mmap_base;
        /* Refuse anything that is not a v3 file: never misread/corrupt older data. */
        if (mf->header->magic != MEMFILE_MAGIC || mf->header->version != MEMFILE_VERSION) {
            munmap(mf->mmap_base, mf->mmap_size);
            goto fail_fd;
        }
    } else {
        mf->fd = open(path, O_RDWR | O_CREAT, 0644);
        if (mf->fd < 0) goto fail;
        if (initial_size < sizeof(memfile_header_t) + 64) initial_size = 4096;
        if (ftruncate(mf->fd, initial_size) < 0) goto fail_fd;
        mf->mmap_size = initial_size;
        mf->mmap_base = mmap(NULL, mf->mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
        if (mf->mmap_base == MAP_FAILED) { unlink(path); goto fail_fd; }
        mf->header = (memfile_header_t *)mf->mmap_base;
        mf->header->magic = MEMFILE_MAGIC;
        mf->header->version = MEMFILE_VERSION;
        mf->header->file_size = initial_size;
        mf->header->allocated = sizeof(memfile_header_t);  /* 64: 32-aligned bump start */
        mf->header->free_root = 0;
        mf->header->free_bytes = 0;
        mf->header->free_count = 0;
    }
    return mf;

fail_fd:
    close(mf->fd);
fail:
    free(mf->path);
    free(mf);
    return NULL;
}

void memfile_sync(memfile_t *mf) {
    if (!mf || mf->closed || !mf->mmap_base) return;
    msync(mf->mmap_base, mf->mmap_size, MS_SYNC);
}

void memfile_close(memfile_t *mf) {
    if (!mf || mf->closed) return;
    mf->closed = 1;
    memfile_sync(mf);
    if (mf->mmap_base) munmap(mf->mmap_base, mf->mmap_size);
    if (mf->fd >= 0) close(mf->fd);
    free(mf->path);
}
