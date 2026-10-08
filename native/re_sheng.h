/*
 * re_sheng.h — Sheng: a PSHUFB-based DFA runtime (Langdale 2018, from Hyperscan)
 * for DFAs with <= 16 states. Each state is one byte; each transition is a
 * single _mm_shuffle_epi8 that looks up the per-input-byte shuffle mask
 * (mask[state] = next state). The mask lookup is off the critical path, so the
 * serial dependency is just the 1-cycle shuffle.
 *
 * Built from a ReDfa (the shared, NFA-validated determinization). This header
 * exposes the QUIET runner first — it computes states but does NOT detect a
 * match — purely to measure raw transition throughput vs a scalar DFA
 * (Concept_ShengSpeedResult: ~6.5x on Skylake). The noisy (accept-detecting)
 * matcher is added next; stop-on-first-accept (Insight_NoisyShengStopIsRight).
 */
#ifndef RE_SHENG_H
#define RE_SHENG_H

#include <stddef.h>
#include "re_dfa.h"

typedef struct ReSheng ReSheng;

/* Build from a DFA. Returns NULL if the DFA has more than 16 states (use the
 * scalar DFA / larger-state tier) or on allocation failure. */
ReSheng *re_sheng_build(const ReDfa *d);
void     re_sheng_free(ReSheng *s);

/* QUIET: run the transition loop over text[0..len) and return the final state.
 * Detects nothing — a throughput probe only. Returning the state stops the
 * compiler from eliminating the loop. */
int      re_sheng_run_quiet(const ReSheng *s, const char *text, size_t len);

/* NOISY: correct boolean matcher (identical verdict to re_search / re_dfa_search,
 * differential-tested). Stop-on-first-accept via a bitmask test off the hot
 * path: (acc_noeol >> state) & 1 after each transition, plus the $ (acc_eol)
 * check at end-of-text and before a single trailing newline. */
int      re_sheng_search(const ReSheng *s, const char *text, size_t len);

#endif /* RE_SHENG_H */
