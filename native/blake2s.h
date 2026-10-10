#ifndef NATIVE_BLAKE2S_H
#define NATIVE_BLAKE2S_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Blake2s (RFC 7693), unkeyed, one-shot. CPU twin of the GPU shader Blake2s
 * (shaders/blake2s_node.comp, smt_fold_*.comp, smt_combine_*.comp): identical
 * IV, SIGMA, rotations (16/12/8/7), and parameter block
 *   h[0] ^= 0x01010000 ^ outlen   (fanout=1, depth=1, keylen=0, digest=outlen)
 * with little-endian word load/store. This is THE unified SMT node-identity
 * hash: a node built on the CPU (svdag/smt.c) and the same node built on the
 * GPU fold get byte-identical digests, which is what lets the GPU-resident SMT
 * spill to / merge with the CPU side.
 *
 * Blake2s is the GPU-native choice (32-bit words map to lanes directly, unlike
 * Blake2b's 64-bit ops). outlen in [1,32]; SMT keys use outlen = 32.
 */
int hg_blake2s(void* out, uint8_t outlen, const void* in, size_t inlen);

#ifdef __cplusplus
}
#endif
#endif /* NATIVE_BLAKE2S_H */
