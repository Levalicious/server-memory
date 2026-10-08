/*
 * re_trigram.h — Cox trigram prefilter for the byte-level regex engine.
 *
 * Implements Russ Cox, "Regular Expression Matching with a Trigram Index"
 * (2012), directly over regex.c's AST (ReNode). Because the prefilter walks
 * the SAME tree the matcher compiles, the two can never disagree: the filter
 * only ever discards documents that provably cannot match
 * (Insight_PrefilterMatcherOneParser). A dropped true match would be a silent
 * wrong result, so soundness — not speed — is the invariant under test.
 *
 * Three cooperating pieces:
 *   1. extractor  ReNode AST  -> ReTrigramQuery  (boolean expr over trigrams)
 *   2. index      documents   -> ReTrigramIndex  (trigram -> posting list)
 *   3. pipeline   (re, ast, index, docs) -> matching doc ids, by narrowing the
 *                 scan to candidates and then verifying each with re_nfa_search.
 *
 * DELTA from the orphaned TS reference (src/trigram.ts + src/regex_query.ts):
 * search is case-SENSITIVE (Decision_CaseSensitiveSearch_2026_07_14), so NO
 * case folding on either side — index and query pack raw bytes verbatim.
 *
 * The in-memory index is build-once (immutable). Incremental add/remove and
 * persistence are single-writer-store concerns (Insight_TrigramIsSingleWriter);
 * this module is the reusable query/eval machinery those will wire up.
 */
#ifndef RE_TRIGRAM_H
#define RE_TRIGRAM_H

#include <stddef.h>
#include <stdint.h>
#include "regex.h"   /* ReNode, Regex */

/* ---- trigram query (Cox extraction over the AST) --------------------------
 * Opaque; owns an arena holding the whole boolean-expression tree. */
typedef struct ReTrigramQuery ReTrigramQuery;

/* Build the required-trigram query for `ast`. Never fails destructively: on
 * allocation trouble it degrades toward "match-all" (a weaker filter is always
 * sound). Returns NULL only if `ast` is NULL. Free with re_trigram_free. */
ReTrigramQuery *re_trigram_build(const ReNode *ast);
void            re_trigram_free(ReTrigramQuery *q);

int  re_trigram_is_all (const ReTrigramQuery *q); /* 1 => no filter; must full-scan */
int  re_trigram_is_none(const ReTrigramQuery *q); /* 1 => provably zero matches     */

/* Render the query as a boolean expression for tests/inspection, e.g.
 *   AND('foo','bar')      OR('foo','bar')      'foo'      ALL      NONE
 * Non-printable trigram bytes are shown as \xHH. NUL-terminates; truncates to
 * `cap`. Returns `buf`. */
char *re_trigram_debug(const ReTrigramQuery *q, char *buf, size_t cap);

/* ---- corpus + inverted index ---------------------------------------------- */
typedef struct { const char *ptr; size_t len; } ReDoc;

typedef struct ReTrigramIndex ReTrigramIndex;

/* Build an immutable inverted index over docs[0..ndocs). The document id is the
 * array index. Returns NULL on allocation failure. */
ReTrigramIndex *re_trigram_index_build(const ReDoc *docs, uint32_t ndocs);
void            re_trigram_index_free(ReTrigramIndex *idx);
uint32_t        re_trigram_index_ndocs(const ReTrigramIndex *idx);
uint32_t        re_trigram_index_distinct(const ReTrigramIndex *idx); /* #distinct trigrams */

/* ---- evaluation -----------------------------------------------------------
 * Candidate doc-id set from evaluating a query against an index:
 *   all == 1  => query is "match-all": no constraint, caller must scan all docs
 *                (ids/n unused).
 *   all == 0  => ids[0..n) is the ascending candidate list (n == 0 means the
 *                query is provably unsatisfiable in this corpus). */
typedef struct { uint32_t *ids; uint32_t n; int all; } ReCandidates;

ReCandidates re_trigram_eval(const ReTrigramQuery *q, const ReTrigramIndex *idx);
void         re_candidates_free(ReCandidates *c);

/* ---- integrated prefilter + verify ---------------------------------------- */
typedef struct {
    uint32_t total;      /* docs in the corpus                                   */
    uint32_t candidates; /* docs the prefilter kept (== total on a full scan)    */
    uint32_t verified;   /* docs actually run through re_nfa_search                  */
    uint32_t matched;    /* docs that matched                                    */
    int      filtered;   /* 1 if the prefilter narrowed the scan, else 0         */
} ReTrigramStats;

/* Full pipeline. Builds the query from `ast`, narrows to candidates via `idx`,
 * runs the real matcher `re` on each candidate, and writes matching doc ids in
 * ascending order into out_ids[0..out_cap). Returns the total match count (which
 * may exceed out_cap; only the first out_cap ids are stored). `stats` may be
 * NULL. The returned set is IDENTICAL to running re_nfa_search over every doc. */
uint32_t re_trigram_search(const Regex *re, const ReNode *ast,
                           const ReTrigramIndex *idx,
                           const ReDoc *docs, uint32_t ndocs,
                           uint32_t *out_ids, uint32_t out_cap,
                           ReTrigramStats *stats);

/* ---- mutable live index (u64 doc ids) for the graph search path -----------
 * Incrementally-maintained inverted index keyed by a u64 document id (the
 * entity offset in the store). Unlike the immutable CSR index above it supports
 * set/remove, so it can track live mutations. In-memory only (not persisted) —
 * a single-writer-store feature run here as an interim. A document's trigram
 * footprint is the UNION of its field byte-ranges' trigrams; because a real
 * match lives in one field whose trigrams are a subset of that union, keying on
 * the union is sound (it can only ever keep extra candidates, never drop one). */
typedef struct ReTrigramLive ReTrigramLive;

ReTrigramLive *re_trigram_live_new(void);
void           re_trigram_live_free(ReTrigramLive *idx);

/* (Re)index `doc` as the union of the trigrams of `nfields` byte ranges.
 * Idempotent: fully replaces any previous contribution of `doc`. */
void re_trigram_live_set(ReTrigramLive *idx, uint64_t doc,
                         const char *const *fields, const size_t *lens, int nfields);
void re_trigram_live_remove(ReTrigramLive *idx, uint64_t doc);

uint32_t re_trigram_live_ndocs(const ReTrigramLive *idx);
uint32_t re_trigram_live_distinct(const ReTrigramLive *idx);
size_t   re_trigram_live_bytes(const ReTrigramLive *idx);   /* approximate heap footprint */

/* Candidate entity ids from evaluating `q` against the live index. Same
 * contract as ReCandidates but with u64 ids: all==1 => no constraint. */
typedef struct { uint64_t *ids; uint32_t n; int all; } ReCandidates64;

ReCandidates64 re_trigram_live_eval(const ReTrigramQuery *q, const ReTrigramLive *idx);
void           re_candidates64_free(ReCandidates64 *c);

#endif /* RE_TRIGRAM_H */
