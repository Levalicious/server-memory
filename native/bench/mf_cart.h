/*
 * mf_cart - ConstantSpaceAllocator, Cartesian-tree (Stephenson "Fast Fits") variant.
 *
 *  - Zero live-block metadata. Sized free. 32B-quantized (node {size,left,right,parent}).
 *  - The free set is a Cartesian tree resident in the free blocks:
 *      BST keyed by ADDRESS (the node's own offset)  -> address-neighbors for coalescing
 *      max-HEAP keyed by SIZE                          -> O(h) lowest-address fit
 *  - Coalescing is CONTINUOUS: free() merges with the address-adjacent free neighbors
 *    (the tree invariant guarantees no two free blocks are ever physically adjacent).
 *  - No balancing yet: literal Fast-Fits. Worst-case depth O(n) under size/address skew.
 */
#ifndef MF_CART_H
#define MF_CART_H

#include "substrate.h"

#define MFC_MIN_BLOCK 32u   /* sizeof {u64 size, left, right, parent} */
#define MFC_QUANTUM   32u

u64  mfc_alloc(mf_t *mf, u64 size);          /* returns offset, 0 = fail */
void mfc_free(mf_t *mf, u64 off, u64 size);  /* sized free; coalesces continuously */
void mfc_coalesce(mf_t *mf);                 /* no-op: coalescing is continuous */
u64  mfc_freelist_len(mf_t *mf);             /* free_count (O(1)) */
u64  mfc_free_bytes(mf_t *mf);               /* free_bytes (O(1)) */
int  mfc_validate(mf_t *mf);                 /* test-only: 0 ok; checks BST+heap+parent+counters */

#endif /* MF_CART_H */
