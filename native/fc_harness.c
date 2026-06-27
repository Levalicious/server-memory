/*
 * Frama-C/EVA analysis harness for the Cartesian allocator.
 * Backs the memfile with a STATIC buffer (no mmap/mremap) so abstract
 * interpretation can model memory and check every deref/arith in alloc/free.
 * Allocations stay within the buffer => no growth => memfile_remap never runs.
 *
 *   frama-c -eva -main fc_main fc_harness.c memoryfile.c
 */
#include "memoryfile.h"

#define MEMSZ (1u << 14)
static u8 G[MEMSZ];

int fc_main(void) {
    memfile_t mf;
    mf.fd = -1; mf.path = (char *)0; mf.mmap_base = G; mf.mmap_size = MEMSZ;
    mf.header = (memfile_header_t *)G; mf.closed = 0;
    mf.header->magic = MEMFILE_MAGIC; mf.header->version = MEMFILE_VERSION;
    mf.header->file_size = MEMSZ; mf.header->allocated = sizeof(memfile_header_t);
    mf.header->free_root = 0; mf.header->free_bytes = 0; mf.header->free_count = 0;

    /* a sequence touching split, take-whole, coalesce-forward/backward, tree rotations */
    u64 a = memfile_alloc(&mf, 40);
    u64 b = memfile_alloc(&mf, 100);
    u64 c = memfile_alloc(&mf, 32);
    u64 d = memfile_alloc(&mf, 200);
    memfile_free(&mf, b, 100);     /* hole between a and c */
    memfile_free(&mf, c, 32);      /* should coalesce with b's freed block */
    u64 e = memfile_alloc(&mf, 64);/* best-fit into a coalesced hole */
    memfile_free(&mf, a, 40);
    memfile_free(&mf, d, 200);
    memfile_free(&mf, e, 64);
    return 0;
}
