/*
 * mf_radix - ConstantSpaceAllocator, explicit-coalesce variant.
 *
 *  - Zero live-block metadata. A live block is `round16(size)` bytes of pure payload.
 *  - Sized deallocation: caller passes size to free; we recompute the canonical block size.
 *  - Free blocks hold {size, next} (16B) in their own bytes; LIFO singly-linked free list.
 *  - Coalescing is EXPLICIT and batched: LSD radix sort of the free list by offset,
 *    then one linear merge pass. No auxiliary F-sized array.
 *  - Optional self-trigger: when free_count exceeds mfr_coalesce_threshold, free() coalesces.
 */
#ifndef MF_RADIX_H
#define MF_RADIX_H

#include "substrate.h"

#define MFR_MIN_BLOCK 16u   /* sizeof {u64 size, u64 next} */
#define MFR_QUANTUM   16u   /* == min block, so split leftovers are always valid free blocks */

u64  mfr_alloc(mf_t *mf, u64 size);          /* returns offset, 0 = fail */
void mfr_free(mf_t *mf, u64 off, u64 size);  /* sized free */
void mfr_coalesce(mf_t *mf);                 /* explicit batch radix coalesce */
u64  mfr_freelist_len(mf_t *mf);             /* free_count (O(1)) */
u64  mfr_free_bytes(mf_t *mf);               /* free_bytes (O(1)) */

/* If >0, mfr_free() coalesces whenever free_count exceeds this (0 = off). */
extern u64 mfr_coalesce_threshold;
extern u64 mfr_coalesce_calls;               /* diagnostic: coalesce passes performed */

#endif /* MF_RADIX_H */
