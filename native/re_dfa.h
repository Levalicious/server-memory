/*
 * re_dfa.h — deterministic finite automaton built from a compiled Regex.
 *
 * This is the scalar foundation for the Sheng/SIMD backends
 * (Plan_ShengTieredExec_2026_07_14). It determinizes the NFA (subset
 * construction over the SAME compiled program the Pike VM runs) into an
 * explicit transition table, then matches with one table lookup per byte and
 * no per-call NFA work.
 *
 * Semantics are identical to re_nfa_search (boolean, unanchored, byte-level,
 * case-sensitive, with Python-default `$` incl. the single-trailing-newline
 * rule) — enforced by differential-testing every DFA verdict against the NFA.
 *
 * Determinization can blow up (up to 2^(NFA states)), so the builder caps the
 * state count and returns NULL if exceeded; the caller then falls back to the
 * NFA. Nothing here allocates during a match.
 */
#ifndef RE_DFA_H
#define RE_DFA_H

#include <stddef.h>
#include "regex.h"

typedef struct ReDfa ReDfa;

/* Build a DFA from a compiled program. Returns NULL if the state count exceeds
 * the cap (pattern too branchy — use the NFA) or on allocation failure. */
ReDfa *re_dfa_build(const Regex *re);
void   re_dfa_free(ReDfa *d);

/* 1 if the pattern matches anywhere in text[0..len), else 0. Identical verdict
 * to re_nfa_search by construction; O(len) with a single table step per byte. */
int    re_dfa_search(const ReDfa *d, const char *text, size_t len);

/* Number of DFA states — for the Sheng tiering decision (<=16 => Sheng-able)
 * and for tests. */
int    re_dfa_state_count(const ReDfa *d);

/* Table accessors, for backends (Sheng) that repack the DFA into their own
 * representation. States are 0..count-1; `byte` is 0..255. */
int    re_dfa_start(const ReDfa *d);
int    re_dfa_trans(const ReDfa *d, int state, int byte);
int    re_dfa_accept_noeol(const ReDfa *d, int state);  /* match on reaching this state */
int    re_dfa_accept_eol(const ReDfa *d, int state);    /* match only at $ position */

/* ---- Lazy (on-demand, cached) DFA -----------------------------------------
 * The graceful large-state scalar tier: builds states/transitions only as the
 * input visits them, and a bounded cache flushes+rebuilds instead of failing.
 * No eager blowup, no hard state cliff. Identical verdict to re_nfa_search (reuses
 * the same closure). A search MUTATES the cache — not thread-safe. */
typedef struct ReLdfa ReLdfa;

/* budget <= 0 uses the default cache size; NULL only on non-terminal `$`
 * (hard anchor) or OOM, so the caller falls back to the NFA. */
ReLdfa *re_ldfa_build(const Regex *re, int budget);
void    re_ldfa_free(ReLdfa *d);
int     re_ldfa_search(ReLdfa *d, const char *text, size_t len);
int     re_ldfa_peak_states(const ReLdfa *d);   /* cached states now (for tests/telemetry) */

#endif /* RE_DFA_H */
