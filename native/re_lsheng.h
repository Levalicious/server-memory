/*
 * re_lsheng.h — Lazy Sheng: a PSHUFB DFA whose 16-lane transition table is
 * populated ON DEMAND (Design_LazySheng_2026_07_14). This unifies the lazy DFA
 * and Sheng: DFA states are discovered as the input visits them and assigned
 * Sheng lanes 0..15; the per-byte shuffle masks are filled lazily.
 *
 * The enabling trick: PSHUFB(mask[byte], lane) reads only the CURRENT lane's
 * entry, so a partially-filled mask still returns the right next-lane. A slot
 * that hasn't been computed reads back as a sentinel (0xFF); the runner then
 * takes a slow path once to compute + lane the successor and fill the slot.
 *
 * Because Sheng-ability is now decided by the number of DISTINCT VISITED
 * (active) states rather than the eager total, patterns whose full DFA exceeds
 * 16 states still run on Sheng as long as any given input stays within 16 live
 * lanes. When a 17th distinct state appears the lane table flushes (keeping the
 * current state) and re-warms — correct always, fast whenever active <= 16.
 *
 * Verdict is identical to re_search / re_dfa_search (differential-tested).
 * A search MUTATES the lane table — not thread-safe.
 */
#ifndef RE_LSHENG_H
#define RE_LSHENG_H

#include <stddef.h>
#include "regex.h"

typedef struct ReLsheng ReLsheng;

/* Eviction policy when a 17th distinct state is needed:
 *   LSH_FLUSH  — drop the whole lane table, re-seed start (poisons the cache).
 *   LSH_LRU    — evict the least-recently-used lane; invalidate only its mask
 *                entries, keeping the other warm lanes.
 *   LSH_SCALAR — leave the lane table untouched; finish THIS search on a scalar
 *                bitset walk (so a deep input degrades locally, no poisoning). */
typedef enum { LSH_FLUSH = 0, LSH_LRU = 1, LSH_SCALAR = 2 } ReLshengPolicy;

/* NULL only on non-terminal `$` (hard anchor) or OOM -> caller uses the NFA. */
ReLsheng *re_lsheng_build(const Regex *re, ReLshengPolicy policy);
void      re_lsheng_free(ReLsheng *s);
int       re_lsheng_search(ReLsheng *s, const char *text, size_t len);

/* telemetry (warm-up vs steady-state; policy comparison) */
long      re_lsheng_fills(const ReLsheng *s);    /* slow-path successor computations */
long      re_lsheng_flushes(const ReLsheng *s);  /* full flushes (LSH_FLUSH) */
long      re_lsheng_evicts(const ReLsheng *s);   /* LRU evictions / scalar-tail entries */

#endif /* RE_LSHENG_H */
