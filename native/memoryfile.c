/*
 * Memory File - mmap-based arena allocator
 *
 * Originally from biscuit/server; adapted for MCP memory server.
 * Added: flock-based concurrency, read/write helpers.
 */

#include "memoryfile.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>

/* =========================================================================
 * Pointer conversion
 * ========================================================================= */

void *memfile_ptr(memfile_t *mf, u64 offset) {
    if (offset == 0 || offset >= mf->mmap_size) return NULL;
    return (u8*)mf->mmap_base + offset;
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
    if (ftruncate(mf->fd, new_size) < 0) {
        return -1;
    }

    void *new_base = mremap(mf->mmap_base, mf->mmap_size, new_size, MREMAP_MAYMOVE);
    if (new_base == MAP_FAILED) {
        return -1;
    }

    mf->mmap_base = new_base;
    mf->mmap_size = new_size;
    mf->header = (memfile_header_t*)new_base;
    mf->header->file_size = new_size;

    return 0;
}

static int memfile_ensure_space(memfile_t *mf, u64 needed) {
    if (mf->header->allocated + needed <= mf->header->file_size) {
        return 0;
    }

    size_t new_size = mf->mmap_size * 2;
    if (new_size < mf->header->allocated + needed) {
        new_size = mf->header->allocated + needed + 4096;
    }

    return memfile_remap(mf, new_size);
}

/* =========================================================================
 * Allocation
 * ========================================================================= */

u64 memfile_alloc(memfile_t *mf, u64 size) {
    u64 total_size = size + sizeof(memfile_alloc_t);

    /* Align to 8 bytes */
    total_size = (total_size + 7) & ~7ULL;

    /* Try free list first (first-fit) */
    u64 prev_offset = 0;
    u64 free_offset = mf->header->free_list_head;

    while (free_offset != 0) {
        memfile_free_t *free_block = (memfile_free_t*)memfile_ptr(mf, free_offset);

        if (free_block->size >= total_size) {
            u64 remaining = free_block->size - total_size;

            if (remaining >= sizeof(memfile_free_t) + 8) {
                /* Split the block */
                u64 new_free_offset = free_offset + total_size;
                memfile_free_t *new_free = (memfile_free_t*)memfile_ptr(mf, new_free_offset);
                new_free->size = remaining;
                new_free->next = free_block->next;

                if (prev_offset == 0) {
                    mf->header->free_list_head = new_free_offset;
                } else {
                    memfile_free_t *prev = (memfile_free_t*)memfile_ptr(mf, prev_offset);
                    prev->next = new_free_offset;
                }
            } else {
                /* Use entire block (avoid tiny leftover) */
                total_size = free_block->size;

                if (prev_offset == 0) {
                    mf->header->free_list_head = free_block->next;
                } else {
                    memfile_free_t *prev = (memfile_free_t*)memfile_ptr(mf, prev_offset);
                    prev->next = free_block->next;
                }
            }

            memfile_alloc_t *alloc = (memfile_alloc_t*)memfile_ptr(mf, free_offset);
            alloc->size = total_size;

            return free_offset + sizeof(memfile_alloc_t);
        }

        prev_offset = free_offset;
        free_offset = free_block->next;
    }

    /* No suitable free block - bump allocate from end */
    if (memfile_ensure_space(mf, total_size) < 0) {
        return 0;
    }

    u64 offset = mf->header->allocated;
    memfile_alloc_t *alloc = (memfile_alloc_t*)memfile_ptr(mf, offset);
    alloc->size = total_size;
    mf->header->allocated += total_size;

    return offset + sizeof(memfile_alloc_t);
}

void memfile_free(memfile_t *mf, u64 offset) {
    if (offset == 0) return;

    u64 alloc_offset = offset - sizeof(memfile_alloc_t);
    memfile_alloc_t *alloc = (memfile_alloc_t*)memfile_ptr(mf, alloc_offset);

    /* Free list node lives in the freed space itself */
    memfile_free_t *free_block = (memfile_free_t*)alloc;
    free_block->size = alloc->size;
    free_block->next = mf->header->free_list_head;

    mf->header->free_list_head = alloc_offset;
}

/* =========================================================================
 * Coalescing - merge adjacent free blocks
 * ========================================================================= */

void memfile_coalesce(memfile_t *mf) {
    if (mf->header->free_list_head == 0) return;

    /* Count free blocks */
    u32 free_count = 0;
    u64 offset = mf->header->free_list_head;
    while (offset != 0) {
        free_count++;
        memfile_free_t *block = (memfile_free_t*)memfile_ptr(mf, offset);
        offset = block->next;
    }

    if (free_count < 2) return;

    /* Collect into temp array */
    struct { u64 offset; u64 size; } *blocks = malloc(free_count * sizeof(*blocks));

    offset = mf->header->free_list_head;
    for (u32 i = 0; i < free_count; i++) {
        memfile_free_t *block = (memfile_free_t*)memfile_ptr(mf, offset);
        blocks[i].offset = offset;
        blocks[i].size = block->size;
        offset = block->next;
    }

    /* Sort by offset (insertion sort - fine for expected small N) */
    for (u32 i = 1; i < free_count; i++) {
        u64 key_off = blocks[i].offset;
        u64 key_size = blocks[i].size;
        int j = i - 1;
        while (j >= 0 && blocks[j].offset > key_off) {
            blocks[j + 1] = blocks[j];
            j--;
        }
        blocks[j + 1].offset = key_off;
        blocks[j + 1].size = key_size;
    }

    /* Merge adjacent */
    u32 write_idx = 0;
    for (u32 i = 0; i < free_count; i++) {
        if (write_idx > 0 &&
            blocks[write_idx - 1].offset + blocks[write_idx - 1].size == blocks[i].offset) {
            blocks[write_idx - 1].size += blocks[i].size;
        } else {
            if (write_idx != i) {
                blocks[write_idx] = blocks[i];
            }
            write_idx++;
        }
    }

    /* Rebuild free list in offset order */
    mf->header->free_list_head = blocks[0].offset;
    for (u32 i = 0; i < write_idx; i++) {
        memfile_free_t *block = (memfile_free_t*)memfile_ptr(mf, blocks[i].offset);
        block->size = blocks[i].size;
        block->next = (i + 1 < write_idx) ? blocks[i + 1].offset : 0;
    }

    free(blocks);
}

/* =========================================================================
 * Concurrency - POSIX flock
 * ========================================================================= */

int memfile_lock_shared(memfile_t *mf) {
    return flock(mf->fd, LOCK_SH);
}

int memfile_lock_exclusive(memfile_t *mf) {
    return flock(mf->fd, LOCK_EX);
}

int memfile_unlock(memfile_t *mf) {
    return flock(mf->fd, LOCK_UN);
}

/* =========================================================================
 * Open/close
 * ========================================================================= */

memfile_t *memfile_open(const char *path, size_t initial_size) {
    memfile_t *mf = calloc(1, sizeof(memfile_t));
    if (!mf) return NULL;
    mf->path = strdup(path);

    struct stat st;
    int exists = (stat(path, &st) == 0 && st.st_size > 0);

    if (exists) {
        mf->fd = open(path, O_RDWR);
        if (mf->fd < 0) goto fail;

        mf->mmap_size = st.st_size;
        mf->mmap_base = mmap(NULL, mf->mmap_size, PROT_READ | PROT_WRITE,
                             MAP_SHARED, mf->fd, 0);
        if (mf->mmap_base == MAP_FAILED) goto fail_fd;

        mf->header = (memfile_header_t*)mf->mmap_base;

        if (mf->header->magic != MEMFILE_MAGIC) {
            munmap(mf->mmap_base, mf->mmap_size);
            goto fail_fd;
        }
    } else {
        mf->fd = open(path, O_RDWR | O_CREAT, 0644);
        if (mf->fd < 0) goto fail;

        if (initial_size < sizeof(memfile_header_t) + 64) {
            initial_size = 4096;
        }

        if (ftruncate(mf->fd, initial_size) < 0) goto fail_fd;

        mf->mmap_size = initial_size;
        mf->mmap_base = mmap(NULL, mf->mmap_size, PROT_READ | PROT_WRITE,
                             MAP_SHARED, mf->fd, 0);
        if (mf->mmap_base == MAP_FAILED) {
            unlink(path);
            goto fail_fd;
        }

        mf->header = (memfile_header_t*)mf->mmap_base;
        mf->header->magic = MEMFILE_MAGIC;
        mf->header->version = MEMFILE_VERSION;
        mf->header->file_size = initial_size;
        mf->header->allocated = sizeof(memfile_header_t);
        mf->header->free_list_head = 0;
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
    munmap(mf->mmap_base, mf->mmap_size);
    close(mf->fd);
    free(mf->path);
}
