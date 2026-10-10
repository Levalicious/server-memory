/*
 * Landed as the server-memory v4 anti-entropy core (2026-10-09): same
 * code as sunder; includes adapted, nothing else changed.
 * Rateless IBLT set reconciliation (Yang/Gilad/Alizadeh, SIGCOMM 2024),
 * regular variant, alpha = 0.5, over fixed 32-byte symbols: SMT node
 * identities. Port of our ck3tool implementation (ext/riblt_core.c encoder,
 * src/sync/riblt.py decoder); KB: RIBLT_MappingRule, RIBLT_CodedSymbol,
 * RIBLT_Peeling_Decode, RIBLT_Bug_PeelMustTrackFutureCells_2026_05_23.
 *
 * A source symbol x maps to coded index i with probability 1/(1 + i/2),
 * deterministically in (key, x): both peers derive the same index
 * sequence from a per-symbol xorshift64 stream and the closed-form
 * inverse CDF of the skip distance. The encoder is a pure function of
 * (key, set): one coded stream serves every peer (universality). A cell
 * is (sum = XOR of mapped symbols, count, checksum = XOR of keyed 64-bit
 * hashes). The decoder combines local and remote cells index by index
 * (sum/checksum XOR, count subtract): symbols in both sets cancel, so the
 * combined cells hold only the symmetric difference, recovered by peeling
 * cells of count +-1 whose checksum matches. Every symbol touches cell 0,
 * so the diff is fully recovered exactly when cell 0 has been fed and is
 * zero after peeling. Hence the wire contract: the datagram carrying
 * cell 0 is delivered reliably (acked, retransmitted); every later cell
 * is fire-and-forget, and a lost one only delays completion
 * (Rule_RIBLT_Cell0Reliable_RestRateless).
 *
 * Checksum and mapping seeds use Blake2s (vendored) truncated to 64 bits
 * instead of ck3tool's XXH3; the choice does not affect correctness.
 */
#ifndef NATIVE_RIBLT_H
#define NATIVE_RIBLT_H

#include <stddef.h>
#include <stdint.h>

#define RIBLT_WIDTH 32u

typedef struct riblt_cell {
    uint8_t  sum[RIBLT_WIDTH];
    int32_t  count;
    uint32_t _pad;
    uint64_t checksum;
} riblt_cell;

/* Lazy, heap-backed encoder over one set. */
typedef struct riblt_enc {
    uint64_t    seed;
    uint32_t    n;
    uint8_t*    src;        /* n * RIBLT_WIDTH */
    uint64_t*   xs;         /* per-source xorshift state */
    uint64_t*   heap;       /* (next_idx << 32 | sidx), min-heap, n entries */
    uint32_t    heap_len;
    uint32_t    coded_len;
    uint32_t    coded_cap;
    uint32_t    _pad;
    riblt_cell* coded;
} riblt_enc;

typedef struct riblt_peeled {
    uint64_t xs;            /* mapping iterator state */
    uint32_t next;          /* next coded index it maps to */
    int32_t  sign;          /* +1 local-only, -1 remote-only */
    uint8_t  sym[RIBLT_WIDTH];
} riblt_peeled;

typedef struct riblt_dec {
    riblt_enc*     local;
    uint64_t       seed;
    riblt_cell*    comb;    /* combined cells; fed[] says which are real */
    uint8_t*       fed;
    uint32_t       comb_len, comb_cap;
    riblt_peeled*  peeled;
    uint32_t       peeled_n, peeled_cap;
    uint64_t*      pq;      /* (next_idx << 32 | seq) min-heap over peeled */
    uint32_t       pq_len;
    uint32_t       fed_n;   /* cells fed so far */
    uint8_t*       local_only;   /* symbols only in the local set (count +1) */
    uint32_t       local_only_n, local_only_cap;
    uint8_t*       remote_only;  /* symbols only in the remote set (count -1) */
    uint32_t       remote_only_n, remote_only_cap;
    uint32_t       first_decode_after;   /* cells fed when decode first held; 0 = not yet */
    uint32_t       _pad;
} riblt_dec;

/* Keyed 64-bit hash of a symbol (checksum / mapping seed). */
uint64_t riblt_hash64(uint64_t seed, const uint8_t sym[RIBLT_WIDTH]);
uint64_t riblt_seed(const void* key, size_t key_len);

/* symbols: n * RIBLT_WIDTH bytes, copied; a SET (a duplicate pair cancels
 * under XOR and is silently invisible). Returns 0 or -1 (OOM). */
int  riblt_enc_init(riblt_enc* e, const void* key, size_t key_len, const uint8_t* symbols, uint32_t n);
void riblt_enc_free(riblt_enc* e);
/* Coded cell idx, producing every cell up to it as needed. NULL on OOM. */
const riblt_cell* riblt_enc_cell(riblt_enc* e, uint32_t idx);

/* The decoder combines against `local` (not owned; must outlive the decoder). */
int  riblt_dec_init(riblt_dec* d, riblt_enc* local, const void* key, size_t key_len);
void riblt_dec_free(riblt_dec* d);
/* Feed the remote peer's cell at idx. Indices must increase: a cell at an
 * index at or below the last fed one is ignored (the caller reorders
 * within a window; lost cells are skipped, never back-filled). Returns 1
 * when the symmetric difference is fully recovered, 0 to keep feeding,
 * -1 on OOM. */
int  riblt_dec_feed(riblt_dec* d, uint32_t idx, const riblt_cell* remote);
int  riblt_dec_decoded(const riblt_dec* d);

#endif
