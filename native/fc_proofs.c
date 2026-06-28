/*
 * WP/ACSL proofs of v3 store algorithms (memory-model-clean abstractions of the
 * real code, so WP can reason without the opaque mmap region).
 *
 *   frama-c -wp -wp-rte -wp-prover alt-ergo,z3 fc_proofs.c
 *
 * 1) round_up32: the size-quantization guarantee the sized-free design rests on.
 * 2) probe_find: the open-addressing linear probe used by the string table AND
 *    the persistent name index — proved memory-safe (no OOB bucket access) and
 *    terminating, for any bucket-count and key.
 */
#include <stdint.h>

/*@ requires size <= 0xffffffffffffffe0;     // no wraparound on size + 31
  @ assigns \nothing;
  @ ensures \result % 32 == 0;
  @ ensures \result >= 32;
  @ ensures \result >= size;
  @*/
uint64_t round_up32(uint64_t size) {
    uint64_t q = (size + 31) / 32;
    uint64_t s = q * 32;
    return s < 32 ? 32 : s;
}

/*@ requires bc > 0 && bc <= 0x7fffffff;
  @ requires \valid_read(buckets + (0 .. bc - 1));
  @ assigns \nothing;
  @ ensures \result == -1 || (0 <= \result < bc);
  @ ensures \result >= 0 ==> buckets[\result] == key;
  @*/
int probe_find(uint64_t *buckets, unsigned bc, uint64_t key) {
    unsigned bucket = (unsigned)(key % bc);
    /*@ loop invariant 0 <= i <= bc;
      @ loop assigns i;
      @ loop variant bc - i;
      @*/
    for (unsigned i = 0; i < bc; i++) {
        unsigned slot = (bucket + i) % bc;
        //@ assert 0 <= slot < bc;
        if (buckets[slot] == 0) return -1;
        if (buckets[slot] == key) return (int)slot;
    }
    return -1;
}
