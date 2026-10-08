/*
 * re_sheng.c — Sheng PSHUFB DFA runtime (quiet, for throughput measurement).
 *
 * Table layout (Concept_PSHUFB_Mechanics): one 16-byte shuffle mask per input
 * byte value; mask lane `s` holds the next state from state `s`. A transition
 * is therefore  state = PSHUFB(mask_for_byte, broadcast(state))  — one shuffle,
 * 1-cycle latency, and the mask load is off the serial path.
 *
 * Requires SSSE3 (_mm_shuffle_epi8). Compiled with -mssse3 in the bench target.
 */
#include "re_sheng.h"

#include <immintrin.h>
#include <stdlib.h>
#include <string.h>

struct ReSheng {
    __m128i       trans[256];   /* trans[b] lane s = next state from s on byte b */
    unsigned char start;
    int           nstates;
    unsigned int  acc_noeol;    /* bitmask over states (for the noisy runner) */
    unsigned int  acc_eol;
};

ReSheng *re_sheng_build(const ReDfa *d) {
    int n = re_dfa_state_count(d);
    if (n <= 0 || n > 16) return NULL;

    /* 16-byte alignment for the __m128i table */
    ReSheng *s = aligned_alloc(16, (sizeof(ReSheng) + 15) & ~(size_t)15);
    if (!s) return NULL;

    for (int b = 0; b < 256; b++) {
        unsigned char row[16];
        for (int st = 0; st < 16; st++)
            row[st] = (st < n) ? (unsigned char)re_dfa_trans(d, st, b) : 0;
        s->trans[b] = _mm_loadu_si128((const __m128i *)row);
    }
    s->start   = (unsigned char)re_dfa_start(d);
    s->nstates = n;
    s->acc_noeol = s->acc_eol = 0;
    for (int st = 0; st < n; st++) {
        if (re_dfa_accept_noeol(d, st)) s->acc_noeol |= 1u << st;
        if (re_dfa_accept_eol(d, st))   s->acc_eol   |= 1u << st;
    }
    return s;
}

void re_sheng_free(ReSheng *s) { free(s); }

int re_sheng_run_quiet(const ReSheng *s, const char *text, size_t len) {
    const unsigned char *p = (const unsigned char *)text;
    __m128i st = _mm_set1_epi8((char)s->start);      /* state broadcast to all lanes */
    for (size_t i = 0; i < len; i++)
        st = _mm_shuffle_epi8(s->trans[p[i]], st);   /* next = mask[byte][state] */
    return (unsigned char)_mm_cvtsi128_si32(st);
}

/* Noisy matcher. Mirrors re_dfa_search's per-position accept semantics exactly:
 * acc_noeol may match at any position (checked at 0, then after each byte);
 * acc_eol ($) matches only at end-of-text or before a single trailing '\n'
 * (the only end-of-line positions in non-multiline mode), so it is checked just
 * at those terminal positions rather than every byte. */
int re_sheng_search(const ReSheng *s, const char *text, size_t len) {
    const unsigned char *p = (const unsigned char *)text;
    unsigned st = s->start;
    if ((s->acc_noeol >> st) & 1u) return 1;                 /* position 0 */
    if (len == 0) return (s->acc_eol >> st) & 1u;            /* $ on empty text */

    __m128i v = _mm_set1_epi8((char)st);
    unsigned prev = st;                                       /* state before consuming last byte */
    for (size_t i = 0; i < len; i++) {
        prev = st;
        v  = _mm_shuffle_epi8(s->trans[p[i]], v);
        st = (unsigned)(unsigned char)_mm_cvtsi128_si32(v);
        if ((s->acc_noeol >> st) & 1u) return 1;             /* match at position i+1 */
    }
    if ((s->acc_eol >> st) & 1u) return 1;                             /* $ at end of text */
    if (p[len - 1] == '\n' && ((s->acc_eol >> prev) & 1u)) return 1;   /* $ before trailing '\n' */
    return 0;
}
