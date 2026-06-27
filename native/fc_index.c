/*
 * ACSL/WP: the persistent name-index lookup, lifted onto a modeled memory region
 * with the REAL on-disk offset arithmetic (16-byte buckets: {u32 name_id, u64 off}
 * at idx + 8 + slot*16). Proves memory-safety (every bucket access in-bounds) and
 * termination, for any bucket count, given the index block fits in memory.
 *
 *   frama-c -wp -wp-rte -wp-prover alt-ergo,z3 fc_index.c
 *
 * Reads go through spec'd accessors (rd32/rd64) — the real code reads via memcpy
 * for aliasing safety; here we model the read with an in-bounds contract so the
 * proof is about the OFFSET ARITHMETIC, not libc memcpy's separation obligations.
 * (bucket = hash32(name_id) % bc in real code; safety holds for any bucket < bc.)
 */
#include <stdint.h>

/*@ requires o + 8 <= msize;
  @ requires \valid_read(mem + (0 .. msize - 1));
  @ assigns \nothing; */
uint64_t rd64(unsigned char *mem, uint64_t msize, uint64_t o);

/*@ requires o + 4 <= msize;
  @ requires \valid_read(mem + (0 .. msize - 1));
  @ assigns \nothing; */
uint32_t rd32(unsigned char *mem, uint64_t msize, uint64_t o);

/*@ requires bc > 0 && bc <= 0x7fffffff;
  @ requires \valid_read(mem + (0 .. msize - 1));
  @ requires idx + 8 + (uint64_t)bc * 16 <= msize;   // index block fits in memory
  @ assigns \nothing;
  @*/
uint64_t ni_lookup_model(unsigned char *mem, uint64_t msize, uint64_t idx,
                         uint32_t bc, uint32_t name_id) {
    uint32_t bucket = name_id % bc;
    /*@ loop invariant 0 <= i <= bc;
      @ loop assigns i;
      @ loop variant bc - i;
      @*/
    for (uint32_t i = 0; i < bc; i++) {
        uint32_t slot = (bucket + i) % bc;
        //@ assert 0 <= slot < bc;
        uint64_t base = idx + 8 + (uint64_t)slot * 16;
        //@ assert base + 16 <= msize;
        uint64_t off = rd64(mem, msize, base + 8);
        uint32_t nid = rd32(mem, msize, base + 0);
        if (off == 0) return 0;
        if (nid == name_id) return off;
    }
    return 0;
}
