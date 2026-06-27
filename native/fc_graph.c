/*
 * ACSL/WP: adjacency edge ENCODING + edge-iteration bounds for the v3 graph store.
 * Every traversal (neighbors, find_path, edges, remove_edge) decodes adjacency
 * entries via this packing and walks the adjacency block, so both are load-bearing.
 *
 *   frama-c -wp -wp-rte -wp-prover alt-ergo,z3 fc_graph.c
 *
 * 1) adj_pack / adj_pack_injective: a target offset and a 2-bit direction share one
 *    u64, ((target<<2)|dir). Proved a ROUND-TRIP (unpack o pack == identity) and
 *    INJECTIVE, given target fits in 62 bits and dir < 4 — no high-offset truncation,
 *    no two distinct (target,dir) pairs aliasing to the same packed word.
 * 2) read_edges_model: iterating n = min(count,max) 24-byte entries at
 *    adj + 8 + i*24 (the real ADJ_HEADER_SIZE / ADJ_ENTRY_SIZE) is memory-safe —
 *    every 24-byte access in-bounds — and terminating, given the block fits.
 *
 * Reads go through spec'd accessors (rd32/rd64); the real code reads via memcpy for
 * aliasing safety, so modeling the read with an in-bounds contract keeps the proof
 * about the OFFSET ARITHMETIC, not libc memcpy separation (same idiom as fc_index).
 */
#include <stdint.h>

#define ADJ_HEADER_SIZE 8u
#define ADJ_ENTRY_SIZE  24u
#define DIR_MASK        3u
#define TARGET_MAX      0x3fffffffffffffffULL   /* 2^62 - 1: survives << 2 losslessly */

/* ---- 1. adjacency encoding: round-trip + injectivity (pure arithmetic) ---- */

/* The real encoding is ((target<<2)|(dir&3u)). With dir < 4 the two fields occupy
 * disjoint bit ranges, so it equals target*4 + dir exactly — and target < 2^62
 * means the shift never truncates. We prove the ALGORITHM (lossless round-trip +
 * injectivity) over that arithmetic form; decode >>2 / &3 are the unsigned /4 / %4
 * proved below. Same abstraction boundary as fc_index modeling hash32 % bc. */
/*@ requires target <= TARGET_MAX;
  @ requires dir <= DIR_MASK;
  @ assigns \nothing;
  @ ensures \result == target * 4 + dir;
  @ ensures \result / 4 == target;     // decode target  (== packed >> 2)
  @ ensures \result % 4 == dir;        // decode direction(== packed & 3)
  @*/
uint64_t adj_pack(uint64_t target, uint64_t dir) {
    return target * 4 + dir;
}

/*@ requires t1 <= TARGET_MAX && d1 <= DIR_MASK;
  @ requires t2 <= TARGET_MAX && d2 <= DIR_MASK;
  @ assigns \nothing;
  @ ensures \result != 0 <==> (t1 == t2 && d1 == d2);
  @*/
int adj_pack_injective(uint64_t t1, uint64_t d1, uint64_t t2, uint64_t d2) {
    return adj_pack(t1, d1) == adj_pack(t2, d2);
}

/* ---- 2. edge-iteration bounds (real adj-block offset arithmetic) ---- */

/*@ requires o + 8 <= msize;
  @ requires \valid_read(mem + (0 .. msize - 1));
  @ assigns \nothing; */
uint64_t rd64(unsigned char *mem, uint64_t msize, uint64_t o);

/*@ requires o + 4 <= msize;
  @ requires \valid_read(mem + (0 .. msize - 1));
  @ assigns \nothing; */
uint32_t rd32(unsigned char *mem, uint64_t msize, uint64_t o);

/*@ requires \valid_read(mem + (0 .. msize - 1));
  @ requires n <= count;
  @ requires adj + ADJ_HEADER_SIZE + (uint64_t)count * ADJ_ENTRY_SIZE <= msize;
  @ assigns \nothing;
  @*/
void read_edges_model(unsigned char *mem, uint64_t msize, uint64_t adj,
                      uint32_t count, uint32_t n) {
    /*@ loop invariant 0 <= i <= n;
      @ loop assigns i;
      @ loop variant n - i;
      @*/
    for (uint32_t i = 0; i < n; i++) {
        uint64_t base = adj + ADJ_HEADER_SIZE + (uint64_t)i * ADJ_ENTRY_SIZE;
        //@ assert base + ADJ_ENTRY_SIZE <= msize;
        uint64_t packed = rd64(mem, msize, base + 0);
        uint32_t rel    = rd32(mem, msize, base + 8);
        uint64_t mtime  = rd64(mem, msize, base + 16);
        //@ assert (packed >> 2) <= TARGET_MAX;   // decoded target always in packable range
        (void)packed; (void)rel; (void)mtime;
    }
}
