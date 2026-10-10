/* Blake2s (RFC 7693), unkeyed, one-shot. A faithful CPU port of the GPU
 * shader Blake2s (shaders/blake2s_node.comp et al.): identical IV, SIGMA,
 * rotations (16/12/8/7), parameter block (h[0]^=0x01010000^outlen), and
 * little-endian word load/store -- so CPU- and GPU-built SMT node hashes are
 * byte-identical. See blake2s.h. */

#include "blake2s.h"
#include <string.h>

static const uint32_t IV[8] = {
    0x6A09E667u, 0xBB67AE85u, 0x3C6EF372u, 0xA54FF53Au,
    0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu, 0x5BE0CD19u
};

static const uint8_t SIGMA[10][16] = {
    {  0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15 },
    { 14,10, 4, 8, 9,15,13, 6, 1,12, 0, 2,11, 7, 5, 3 },
    { 11, 8,12, 0, 5, 2,15,13,10,14, 3, 6, 7, 1, 9, 4 },
    {  7, 9, 3, 1,13,12,11,14, 2, 6, 5,10, 4, 0,15, 8 },
    {  9, 0, 5, 7, 2, 4,10,15,14, 1,11,12, 6, 8, 3,13 },
    {  2,12, 6,10, 0,11, 8, 3, 4,13, 7, 5,15,14, 1, 9 },
    { 12, 5, 1,15,14,13, 4,10, 0, 7, 6, 3, 9, 2, 8,11 },
    { 13,11, 7,14,12, 1, 3, 9, 5, 0,15, 4, 8, 6, 2,10 },
    {  6,15,14, 9,11, 3, 0, 8,12, 2,13, 7, 1, 4,10, 5 },
    { 10, 2, 8, 4, 7, 6, 1, 5,15,11, 9,14, 3,12,13, 0 }
};

static inline uint32_t rotr32(uint32_t x, unsigned n) { return (x >> n) | (x << (32u - n)); }

static void b2s_compress(uint32_t h[8], const uint32_t m[16], uint32_t t0, uint32_t t1, uint32_t f0) {
    uint32_t v[16];
    for (int i = 0; i < 8; ++i) v[i] = h[i];
    v[8] = IV[0]; v[9] = IV[1]; v[10] = IV[2]; v[11] = IV[3];
    v[12] = IV[4] ^ t0; v[13] = IV[5] ^ t1; v[14] = IV[6] ^ f0; v[15] = IV[7];
#define G(a,b,c,d,x,y) \
    v[a] = v[a] + v[b] + (x); v[d] = rotr32(v[d] ^ v[a], 16u); v[c] = v[c] + v[d]; v[b] = rotr32(v[b] ^ v[c], 12u); \
    v[a] = v[a] + v[b] + (y); v[d] = rotr32(v[d] ^ v[a],  8u); v[c] = v[c] + v[d]; v[b] = rotr32(v[b] ^ v[c],  7u);
#define RND(r) \
    G(0,4, 8,12, m[SIGMA[r][ 0]], m[SIGMA[r][ 1]]) \
    G(1,5, 9,13, m[SIGMA[r][ 2]], m[SIGMA[r][ 3]]) \
    G(2,6,10,14, m[SIGMA[r][ 4]], m[SIGMA[r][ 5]]) \
    G(3,7,11,15, m[SIGMA[r][ 6]], m[SIGMA[r][ 7]]) \
    G(0,5,10,15, m[SIGMA[r][ 8]], m[SIGMA[r][ 9]]) \
    G(1,6,11,12, m[SIGMA[r][10]], m[SIGMA[r][11]]) \
    G(2,7, 8,13, m[SIGMA[r][12]], m[SIGMA[r][13]]) \
    G(3,4, 9,14, m[SIGMA[r][14]], m[SIGMA[r][15]])
    RND(0) RND(1) RND(2) RND(3) RND(4) RND(5) RND(6) RND(7) RND(8) RND(9)
#undef RND
#undef G
    for (int i = 0; i < 8; ++i) h[i] ^= v[i] ^ v[i + 8];
}

static inline uint32_t load32le(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

int hg_blake2s(void* out, uint8_t outlen, const void* in, size_t inlen) {
    if (outlen == 0u || outlen > 32u) return -1;

    uint32_t h[8];
    for (int i = 0; i < 8; ++i) h[i] = IV[i];
    h[0] ^= 0x01010000u ^ (uint32_t)outlen;   /* fanout=1, depth=1, keylen=0, digest=outlen */

    const uint8_t* p = (const uint8_t*)in;
    uint32_t t0 = 0u, t1 = 0u;
    size_t remain = inlen;

    /* Full 64-byte blocks, leaving the final (possibly full) block for
     * finalization with the last-block flag. */
    while (remain > 64u) {
        uint32_t m[16];
        for (int j = 0; j < 16; ++j) m[j] = load32le(p + j * 4);
        t0 += 64u; if (t0 < 64u) t1++;
        b2s_compress(h, m, t0, t1, 0u);
        p += 64; remain -= 64u;
    }

    /* Final block: zero-padded to 64 bytes, last-block flag f0 = ~0. */
    uint8_t blk[64];
    memset(blk, 0, sizeof blk);
    if (remain) memcpy(blk, p, remain);
    uint32_t m[16];
    for (int j = 0; j < 16; ++j) m[j] = load32le(blk + j * 4);
    t0 += (uint32_t)remain; if (t0 < (uint32_t)remain) t1++;
    b2s_compress(h, m, t0, t1, 0xFFFFFFFFu);

    uint8_t hb[32];
    for (int i = 0; i < 8; ++i) {
        hb[i * 4 + 0] = (uint8_t)(h[i]      );
        hb[i * 4 + 1] = (uint8_t)(h[i] >>  8);
        hb[i * 4 + 2] = (uint8_t)(h[i] >> 16);
        hb[i * 4 + 3] = (uint8_t)(h[i] >> 24);
    }
    memcpy(out, hb, outlen);
    return 0;
}
