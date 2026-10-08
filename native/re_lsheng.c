/*
 * re_lsheng.c — Lazy Sheng runtime (PSHUFB DFA, lanes filled on demand), with
 * three eviction policies for the "17th distinct state" case (see re_lsheng.h):
 * LSH_FLUSH, LSH_LRU, LSH_SCALAR. The hot per-byte loop (one PSHUFB + a sentinel
 * check) is identical across policies; they differ only in the slow path.
 */
#include "re_lsheng.h"
#include "regex_internal.h"

#include <immintrin.h>
#include <stdlib.h>
#include <string.h>

#define LANES 16
#define UNKNOWN 0xFFu

#define BIT_TEST(bits, pc) (((bits)[(pc) >> 6] >> ((pc) & 63)) & 1u)

struct ReLsheng {
    const Regex   *re;
    int            nwords, match_pc, nlanes;
    ReLshengPolicy policy;
    unsigned char  masks[256][LANES];        /* masks[b][lane] = next lane, 0xFF = unknown */
    uint64_t      *lane_bits[LANES];
    unsigned char  acc_noeol[LANES], acc_eol[LANES];
    unsigned       last_used[LANES], clock;  /* LRU recency */
    int           *stack, *seeds, *seeds2, *frontier;
    uint64_t      *tmp, *tmp2, *curb, *work;
    long           fills, flushes, evicts;
};

/* acc_eol for an arbitrary state bitset (MATCH reachable if EOL fires) */
static int acc_eol_of(struct ReLsheng *s, const uint64_t *bits) {
    if (BIT_TEST(bits, s->match_pc)) return 1;
    int ns = 0;
    for (int pc = 0; pc < s->re->ninst; pc++)
        if (BIT_TEST(bits, pc) && s->re->insts[pc].op == OP_EOL) s->seeds2[ns++] = pc + 1;
    if (!ns) return 0;
    re_closure(s->re, s->seeds2, ns, 0, 1, s->tmp2, s->nwords, s->stack);
    return (int)BIT_TEST(s->tmp2, s->match_pc);
}

/* successor bitset of `from` on byte b (+ unanchored restart) into `out` */
static void lsheng_succ(struct ReLsheng *s, const uint64_t *from, unsigned b, uint64_t *out) {
    int ns = 0;
    for (int pc = 0; pc < s->re->ninst; pc++) {
        if (!BIT_TEST(from, pc)) continue;
        const Inst *in = &s->re->insts[pc];
        if (in->op == OP_CHAR)  { if (in->c == (unsigned char)b) s->seeds[ns++] = pc + 1; }
        else if (in->op == OP_CLASS) { if (re_class_test(s->re, in->cls, b)) s->seeds[ns++] = pc + 1; }
    }
    s->seeds[ns++] = 0;
    re_closure(s->re, s->seeds, ns, 0, 0, out, s->nwords, s->stack);
}

static int lsheng_find(struct ReLsheng *s, const uint64_t *bits) {
    for (int l = 0; l < s->nlanes; l++)
        if (memcmp(s->lane_bits[l], bits, (size_t)s->nwords * 8) == 0) return l;
    return -1;
}
/* install `bits` into lane `l` (fresh or reassigned) */
static void lsheng_set_lane(struct ReLsheng *s, int l, const uint64_t *bits) {
    memcpy(s->lane_bits[l], bits, (size_t)s->nwords * 8);
    s->acc_noeol[l] = (unsigned char)BIT_TEST(bits, s->match_pc);
    s->acc_eol[l]   = (unsigned char)acc_eol_of(s, bits);
    s->last_used[l] = ++s->clock;
}
static int lsheng_add(struct ReLsheng *s, const uint64_t *bits) {
    int l = s->nlanes++;
    lsheng_set_lane(s, l, bits);
    return l;
}
static void lsheng_flush(struct ReLsheng *s) {
    memset(s->masks, 0xFF, sizeof s->masks);
    s->nlanes = 0;
    int seed0 = 0;
    re_closure(s->re, &seed0, 1, 1, 0, s->tmp, s->nwords, s->stack);
    lsheng_add(s, s->tmp);
    s->flushes++;
}
/* reassign lane `victim` to a new state: invalidate its incoming edges (any
 * mask == victim) and its outgoing row, then install the new bits. */
static void lsheng_evict_into(struct ReLsheng *s, int victim, const uint64_t *bits) {
    for (int b = 0; b < 256; b++) {
        s->masks[b][victim] = UNKNOWN;                        /* outgoing from victim */
        for (int l = 0; l < LANES; l++)
            if (s->masks[b][l] == (unsigned char)victim) s->masks[b][l] = UNKNOWN;  /* incoming */
    }
    lsheng_set_lane(s, victim, bits);
    s->evicts++;
}

/* Slow path: returns the next lane (>=0), or -2 to request a scalar tail
 * (LSH_SCALAR only; target bitset left in s->curb), or -1 on OOM. */
static int lsheng_slow(struct ReLsheng *s, int cur, unsigned b) {
    s->fills++;
    lsheng_succ(s, s->lane_bits[cur], b, s->tmp);

    int tl = lsheng_find(s, s->tmp);
    if (tl >= 0) { s->masks[b][cur] = (unsigned char)tl; return tl; }
    if (s->nlanes < LANES) { tl = lsheng_add(s, s->tmp); s->masks[b][cur] = (unsigned char)tl; return tl; }

    /* lane table full */
    if (s->policy == LSH_FLUSH) {
        memcpy(s->curb, s->tmp, (size_t)s->nwords * 8);   /* flush reuses tmp */
        lsheng_flush(s);
        return lsheng_add(s, s->curb);                    /* target relaned; don't memoise */
    }
    if (s->policy == LSH_LRU) {
        int victim = -1; unsigned best = ~0u;
        for (int l = 0; l < LANES; l++)
            if (l != cur && s->last_used[l] < best) { best = s->last_used[l]; victim = l; }
        memcpy(s->curb, s->tmp, (size_t)s->nwords * 8);   /* evict scans masks; keep target safe */
        lsheng_evict_into(s, victim, s->curb);
        s->masks[b][cur] = (unsigned char)victim;
        return victim;
    }
    /* LSH_SCALAR: leave lanes untouched, finish this search on a bitset walk */
    memcpy(s->curb, s->tmp, (size_t)s->nwords * 8);
    s->evicts++;
    return -2;
}

/* scalar tail from state `s->curb` at position `j` (state after consuming j bytes) */
static int lsheng_scalar_tail(struct ReLsheng *s, const unsigned char *p, size_t j, size_t len) {
    memcpy(s->work, s->curb, (size_t)s->nwords * 8);
    for (;; j++) {
        if (BIT_TEST(s->work, s->match_pc)) return 1;
        if (acc_eol_of(s, s->work)) {
            if (j == len) return 1;
            if (j + 1 == len && p[j] == '\n') return 1;
        }
        if (j == len) break;
        lsheng_succ(s, s->work, p[j], s->tmp);
        memcpy(s->work, s->tmp, (size_t)s->nwords * 8);
    }
    return 0;
}

ReLsheng *re_lsheng_build(const Regex *re, ReLshengPolicy policy) {
    if (!re || re->ninst < 1) return NULL;
    if (re_dfa_hard_anchor(re)) return NULL;
    struct ReLsheng *s = calloc(1, sizeof *s);
    if (!s) return NULL;
    s->re = re; s->nwords = (re->ninst + 63) / 64; s->match_pc = re->ninst - 1; s->policy = policy;
    s->stack    = malloc((size_t)re->ninst * sizeof(int));
    s->seeds    = malloc(((size_t)re->ninst + 1) * sizeof(int));
    s->seeds2   = malloc((size_t)re->ninst * sizeof(int));
    s->frontier = malloc((size_t)re->ninst * sizeof(int));
    s->tmp  = malloc((size_t)s->nwords * 8);
    s->tmp2 = malloc((size_t)s->nwords * 8);
    s->curb = malloc((size_t)s->nwords * 8);
    s->work = malloc((size_t)s->nwords * 8);
    int ok = s->stack && s->seeds && s->seeds2 && s->frontier && s->tmp && s->tmp2 && s->curb && s->work;
    for (int l = 0; l < LANES && ok; l++) { s->lane_bits[l] = malloc((size_t)s->nwords * 8); ok = ok && s->lane_bits[l]; }
    if (!ok) { re_lsheng_free(s); return NULL; }
    memset(s->masks, 0xFF, sizeof s->masks);
    int seed0 = 0;
    re_closure(re, &seed0, 1, 1, 0, s->tmp, s->nwords, s->stack);
    lsheng_add(s, s->tmp);
    return s;
}

void re_lsheng_free(ReLsheng *s) {
    if (!s) return;
    for (int l = 0; l < LANES; l++) free(s->lane_bits[l]);
    free(s->stack); free(s->seeds); free(s->seeds2); free(s->frontier);
    free(s->tmp); free(s->tmp2); free(s->curb); free(s->work);
    free(s);
}

int re_lsheng_search(ReLsheng *s, const char *text, size_t len) {
    const unsigned char *p = (const unsigned char *)text;
    int cur = 0;
    for (size_t i = 0; ; i++) {
        if (s->acc_noeol[cur]) return 1;
        if (s->acc_eol[cur]) {
            if (i == len) return 1;
            if (i + 1 == len && p[i] == '\n') return 1;
        }
        if (i == len) break;
        unsigned b = p[i];
        __m128i m  = _mm_loadu_si128((const __m128i *)s->masks[b]);
        __m128i sv = _mm_set1_epi8((char)cur);
        int nx = (unsigned char)_mm_cvtsi128_si32(_mm_shuffle_epi8(m, sv));
        if ((unsigned)nx == UNKNOWN) {
            nx = lsheng_slow(s, cur, b);
            if (nx == -2) return lsheng_scalar_tail(s, p, i + 1, len);   /* deep input, no poison */
            if (nx < 0) return 0;
        }
        s->last_used[nx] = ++s->clock;
        cur = nx;
    }
    return 0;
}

long re_lsheng_fills(const ReLsheng *s)   { return s ? s->fills : 0; }
long re_lsheng_flushes(const ReLsheng *s) { return s ? s->flushes : 0; }
long re_lsheng_evicts(const ReLsheng *s)  { return s ? s->evicts : 0; }
