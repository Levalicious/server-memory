/*
 * re_dfa.c — NFA -> DFA (subset construction) + scalar table runner.
 *
 * A DFA state is a set of NFA program counters (the non-EOL epsilon-closure of
 * some seed set), stored as a bitset and interned so equal sets share one state.
 * Transitions and both accept flags are pure functions of that bitset, so the
 * DFA can never disagree with the Pike VM — same program, same closure.
 *
 * Anchors:
 *   - Unanchored search is baked in: every transition re-injects the NFA start
 *     pc (an implicit `.*?` prefix), so a match may begin at any position.
 *   - `^` (OP_BOL) passes only in the START state's closure (bol=true); every
 *     other state closes with bol=false, so a re-injected start can't satisfy ^.
 *   - `$` (OP_EOL) is zero-width and position-dependent, so it is NOT followed
 *     during transitions. Instead each state carries acc_eol = "MATCH reachable
 *     if we let EOL fire here", checked at end-of-text and before a single
 *     trailing newline (Python-default `$`), exactly like the NFA.
 *
 * Determinization can be exponential; the builder caps states and returns NULL
 * on overflow so the caller falls back to the NFA.
 */
#include "re_dfa.h"
#include "regex_internal.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define DFA_MAX_STATES 4096

#define BIT_TEST(bits, pc) (((bits)[(pc) >> 6] >> ((pc) & 63)) & 1u)
#define BIT_SET(bits, pc)  ((bits)[(pc) >> 6] |= (uint64_t)1 << ((pc) & 63))

typedef struct { uint64_t *bits; int *trans; unsigned char acc_noeol, acc_eol; } DState;

struct ReDfa {
    DState       *st; int n, cap;
    int           nwords, match_pc, start;
    const Regex  *re;              /* kept only during build (for acc_eol closures) */
    const Inst   *insts;
    int           st_ninst;
};

/* Open-addressing intern table: bitset -> state index. */
typedef struct { int *slot; int mask; } HTab;

static uint64_t hash_bits(const uint64_t *b, int nw) {
    uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < nw; i++) { h ^= b[i]; h *= 1099511628211ULL; }
    return h;
}

/* Non-EOL (or, if allow_eol, EOL-passing) epsilon-closure of `seeds` into
 * `out` (nwords). `stack` is scratch of >= ninst ints. Returns 1 if MATCH is in
 * the closure. When allow_eol is false, `out` is the canonical DFA-state set. */
static int closure(const Regex *re, const int *seeds, int nseed, int bol, int allow_eol,
                   uint64_t *out, int nwords, int *stack) {
    memset(out, 0, (size_t)nwords * 8);
    int sp = 0;
    for (int i = 0; i < nseed; i++) {
        int pc = seeds[i];
        if (!BIT_TEST(out, pc)) { BIT_SET(out, pc); stack[sp++] = pc; }
    }
    int accept = 0;
    while (sp) {
        int pc = stack[--sp];
        const Inst *in = &re->insts[pc];
        int nx = -1, ny = -1;
        switch (in->op) {
            case OP_JMP:   nx = in->x; break;
            case OP_SPLIT: nx = in->x; ny = in->y; break;
            case OP_BOL:   if (bol) nx = pc + 1; break;
            case OP_EOL:   if (allow_eol) nx = pc + 1; break;
            case OP_MATCH: accept = 1; break;
            default: break;   /* CHAR / CLASS stay in the frontier */
        }
        if (nx >= 0 && !BIT_TEST(out, nx)) { BIT_SET(out, nx); stack[sp++] = nx; }
        if (ny >= 0 && !BIT_TEST(out, ny)) { BIT_SET(out, ny); stack[sp++] = ny; }
    }
    return accept;
}

/* Find or create the state whose canonical set is `bits`. Returns index, or -1
 * on state-cap overflow / OOM. Fills accept flags on creation. `seeds2`/`tmp2`
 * are scratch (>= ninst / nwords) for the acc_eol closure. */
static int intern(ReDfa *d, HTab *ht, const uint64_t *bits, int *stack, int *seeds2, uint64_t *tmp2) {
    uint64_t h = hash_bits(bits, d->nwords);
    int i = (int)(h & (uint64_t)ht->mask);
    while (ht->slot[i] != -1) {
        int idx = ht->slot[i];
        if (memcmp(d->st[idx].bits, bits, (size_t)d->nwords * 8) == 0) return idx;
        i = (i + 1) & ht->mask;
    }
    if (d->n >= DFA_MAX_STATES) return -1;
    if (d->n == d->cap) {
        int nc = d->cap ? d->cap * 2 : 64;
        DState *ns = realloc(d->st, (size_t)nc * sizeof(DState));
        if (!ns) return -1;
        d->st = ns; d->cap = nc;
    }
    DState *s = &d->st[d->n];
    s->bits = malloc((size_t)d->nwords * 8);
    if (!s->bits) return -1;
    memcpy(s->bits, bits, (size_t)d->nwords * 8);
    s->trans = NULL;
    s->acc_noeol = (unsigned char)BIT_TEST(bits, d->match_pc);
    /* acc_eol: MATCH reachable once the EOL nodes in this set fire. All EOLs are
     * terminal here — non-terminal `$` was rejected in re_dfa_build. */
    if (s->acc_noeol) {
        s->acc_eol = 1;
    } else {
        int ns = 0;
        for (int pc = 0; pc < d->st_ninst; pc++)
            if (BIT_TEST(bits, pc) && d->insts[pc].op == OP_EOL) seeds2[ns++] = pc + 1;
        if (ns) { closure(d->re, seeds2, ns, 0, 1, tmp2, d->nwords, stack); s->acc_eol = (unsigned char)BIT_TEST(tmp2, d->match_pc); }
        else s->acc_eol = 0;
    }
    int idx = d->n++;
    ht->slot[i] = idx;
    return idx;
}

/* A `$` is "terminal" if, following epsilon from its successor, you can only
 * reach MATCH (via JMP/SPLIT/other EOLs). A NON-terminal `$` — one that leads to
 * a consuming instruction or a `^` — needs position-dependent mid-match
 * assertion handling the pure DFA doesn't do (Python's `$` matches before a
 * trailing newline, so `$[^a]` can match the newline). We reject those and let
 * the caller fall back to the NFA. Sound over-approximation (position-blind). */
static int eps_reaches_consuming(const Regex *re, int start, char *seen, int *stack) {
    int sp = 0;
    if (!seen[start]) { seen[start] = 1; stack[sp++] = start; }
    while (sp) {
        int pc = stack[--sp];
        const Inst *in = &re->insts[pc];
        int a = -1, b = -1;
        switch (in->op) {
            case OP_CHAR: case OP_CLASS: case OP_BOL: return 1;  /* hard */
            case OP_MATCH: break;
            case OP_JMP:   a = in->x; break;
            case OP_SPLIT: a = in->x; b = in->y; break;
            case OP_EOL:   a = pc + 1; break;
        }
        if (a >= 0 && !seen[a]) { seen[a] = 1; stack[sp++] = a; }
        if (b >= 0 && !seen[b]) { seen[b] = 1; stack[sp++] = b; }
    }
    return 0;
}
static int dfa_has_hard_anchor(const Regex *re) {
    char *seen = calloc((size_t)re->ninst, 1);
    int  *stk  = malloc((size_t)re->ninst * sizeof(int));
    if (!seen || !stk) { free(seen); free(stk); return 1; }  /* be safe: fall back */
    int hard = 0;
    for (int pc = 0; pc < re->ninst && !hard; pc++) {
        if (re->insts[pc].op != OP_EOL) continue;
        memset(seen, 0, (size_t)re->ninst);
        if (eps_reaches_consuming(re, pc + 1, seen, stk)) hard = 1;
    }
    free(seen); free(stk);
    return hard;
}

ReDfa *re_dfa_build(const Regex *re) {
    if (!re || re->ninst < 1) return NULL;
    if (dfa_has_hard_anchor(re)) return NULL;   /* non-terminal $ -> use the NFA */
    ReDfa *d = calloc(1, sizeof *d);
    if (!d) return NULL;
    d->nwords = (re->ninst + 63) / 64;
    d->match_pc = re->ninst - 1;          /* re_compile_ast emits OP_MATCH last */
    /* stash extras via the (private) helper fields */
    d->re = re; d->st_ninst = re->ninst; d->insts = re->insts;

    int   *stack   = malloc((size_t)re->ninst * sizeof(int));
    int   *seeds   = malloc(((size_t)re->ninst + 1) * sizeof(int));
    int   *seeds2  = malloc((size_t)re->ninst * sizeof(int));
    uint64_t *tmp  = malloc((size_t)d->nwords * 8);
    uint64_t *tmp2 = malloc((size_t)d->nwords * 8);
    int   *frontier = malloc((size_t)re->ninst * sizeof(int));
    uint64_t *curb = malloc((size_t)d->nwords * 8);
    int   *queue   = malloc(DFA_MAX_STATES * sizeof(int));
    HTab   ht = { NULL, 0 };
    int htcap = 1; while (htcap < DFA_MAX_STATES * 2) htcap <<= 1;
    ht.slot = malloc((size_t)htcap * sizeof(int)); ht.mask = htcap - 1;
    if (!stack || !seeds || !seeds2 || !tmp || !tmp2 || !frontier || !curb || !queue || !ht.slot) goto fail;
    for (int i = 0; i < htcap; i++) ht.slot[i] = -1;

    /* start state: closure of {0} with bol=true (^ can fire only here) */
    seeds[0] = 0;
    closure(re, seeds, 1, /*bol=*/1, /*allow_eol=*/0, tmp, d->nwords, stack);
    d->start = intern(d, &ht, tmp, stack, seeds2, tmp2);
    if (d->start < 0) goto fail;

    int qh = 0, qt = 0; queue[qt++] = d->start;
    while (qh < qt) {
        int si = queue[qh++];
        memcpy(curb, d->st[si].bits, (size_t)d->nwords * 8);   /* snapshot: intern may realloc d->st */
        int nf = 0;
        for (int pc = 0; pc < re->ninst; pc++)
            if (BIT_TEST(curb, pc) && (re->insts[pc].op == OP_CHAR || re->insts[pc].op == OP_CLASS))
                frontier[nf++] = pc;

        int trans[256];
        for (int b = 0; b < 256; b++) {
            int ns = 0;
            for (int k = 0; k < nf; k++) {
                int pc = frontier[k]; const Inst *in = &re->insts[pc];
                int hit = (in->op == OP_CHAR) ? (in->c == (unsigned char)b)
                                              : re_class_test(re, in->cls, (unsigned)b);
                if (hit) seeds[ns++] = pc + 1;
            }
            seeds[ns++] = 0;   /* unanchored restart (implicit .*?) */
            closure(re, seeds, ns, /*bol=*/0, /*allow_eol=*/0, tmp, d->nwords, stack);
            int t = intern(d, &ht, tmp, stack, seeds2, tmp2);
            if (t < 0) goto fail;                 /* state cap: fall back to NFA */
            if (t == qt) { if (qt >= DFA_MAX_STATES) goto fail; queue[qt++] = t; }
            trans[b] = t;
        }
        int *tr = malloc(256 * sizeof(int));
        if (!tr) goto fail;
        memcpy(tr, trans, sizeof trans);
        d->st[si].trans = tr;
    }

    free(stack); free(seeds); free(seeds2); free(tmp); free(tmp2); free(frontier); free(curb); free(queue); free(ht.slot);
    return d;

fail:
    free(stack); free(seeds); free(seeds2); free(tmp); free(tmp2); free(frontier); free(curb); free(queue); free(ht.slot);
    re_dfa_free(d);
    return NULL;
}

void re_dfa_free(ReDfa *d) {
    if (!d) return;
    for (int i = 0; i < d->n; i++) { free(d->st[i].bits); free(d->st[i].trans); }
    free(d->st);
    free(d);
}

int re_dfa_state_count(const ReDfa *d) { return d ? d->n : 0; }

int re_dfa_start(const ReDfa *d)                     { return d->start; }
int re_dfa_trans(const ReDfa *d, int state, int byte){ return d->st[state].trans[byte]; }
int re_dfa_accept_noeol(const ReDfa *d, int state)   { return d->st[state].acc_noeol; }
int re_dfa_accept_eol(const ReDfa *d, int state)     { return d->st[state].acc_eol; }

int re_dfa_search(const ReDfa *d, const char *text, size_t len) {
    const unsigned char *s = (const unsigned char *)text;
    int cur = d->start;
    for (size_t i = 0; ; i++) {
        const DState *st = &d->st[cur];
        if (st->acc_noeol) return 1;
        if (st->acc_eol) {
            if (i == len) return 1;                              /* end of text */
            if (i + 1 == len && s[i] == '\n') return 1;          /* before a single trailing \n */
        }
        if (i == len) break;
        cur = st->trans[s[i]];
    }
    return 0;
}

/* ======================================================================
 * Lazy (on-demand, cached) DFA — the graceful large-state scalar tier.
 *
 * Same states / closure / accept semantics as the eager DFA (it reuses the very
 * same closure() and hard-anchor check), but states and transitions are built
 * only when the input visits them, and a bounded state cache FLUSHES (rebuilds
 * from the start) instead of failing when full. So there is no eager blowup and
 * no hard state cliff: a huge DFA whose input touches few states pays for few
 * states, and a genuinely huge live set degrades to recompute cost, never an
 * NFA fallback. NOT thread-safe — a search mutates the cache.
 * ====================================================================== */

#define LDFA_DEFAULT_BUDGET 16384

typedef struct { uint64_t *bits; int *trans; unsigned char acc_noeol, acc_eol; } LState;

struct ReLdfa {
    const Regex *re;
    int          nwords, match_pc, budget, start;
    LState      *st; int n, cap;
    int         *ht, htmask;
    int         *stack, *seeds, *seeds2, *frontier;
    uint64_t    *tmp, *tmp2, *curb;
};

/* intern `bits` -> state id (create if new); assumes budget headroom. */
static int ldfa_intern(struct ReLdfa *L, const uint64_t *bits) {
    uint64_t h = hash_bits(bits, L->nwords);
    int i = (int)(h & (uint64_t)L->htmask);
    while (L->ht[i] != -1) {
        int idx = L->ht[i];
        if (memcmp(L->st[idx].bits, bits, (size_t)L->nwords * 8) == 0) return idx;
        i = (i + 1) & L->htmask;
    }
    if (L->n == L->cap) {
        int nc = L->cap ? L->cap * 2 : 64;
        LState *ns = realloc(L->st, (size_t)nc * sizeof(LState));
        if (!ns) return -1;
        L->st = ns; L->cap = nc;
    }
    LState *s = &L->st[L->n];
    s->bits  = malloc((size_t)L->nwords * 8);
    s->trans = malloc(256 * sizeof(int));
    if (!s->bits || !s->trans) { free(s->bits); free(s->trans); return -1; }
    memcpy(s->bits, bits, (size_t)L->nwords * 8);
    memset(s->trans, 0xFF, 256 * sizeof(int));   /* -1 = not yet computed */
    s->acc_noeol = (unsigned char)BIT_TEST(bits, L->match_pc);
    if (s->acc_noeol) s->acc_eol = 1;
    else {
        int ns2 = 0;
        for (int pc = 0; pc < L->re->ninst; pc++)
            if (BIT_TEST(bits, pc) && L->re->insts[pc].op == OP_EOL) L->seeds2[ns2++] = pc + 1;
        if (ns2) { closure(L->re, L->seeds2, ns2, 0, 1, L->tmp2, L->nwords, L->stack);
                   s->acc_eol = (unsigned char)BIT_TEST(L->tmp2, L->match_pc); }
        else s->acc_eol = 0;
    }
    int idx = L->n++;
    L->ht[i] = idx;
    return idx;
}

/* drop the whole cache and re-seed the start (the safety valve for huge DFAs) */
static void ldfa_flush(struct ReLdfa *L) {
    for (int i = 0; i < L->n; i++) { free(L->st[i].bits); free(L->st[i].trans); }
    L->n = 0;
    for (int i = 0; i <= L->htmask; i++) L->ht[i] = -1;
    int seed0 = 0;
    closure(L->re, &seed0, 1, /*bol=*/1, /*allow_eol=*/0, L->tmp, L->nwords, L->stack);
    L->start = ldfa_intern(L, L->tmp);
}

/* transition from state `cur` on byte `b`; builds the target on demand and may
 * flush. Returns the target id in the current numbering, or -1 on OOM. */
static int ldfa_step(struct ReLdfa *L, int cur, unsigned b) {
    int cached = L->st[cur].trans[b];
    if (cached >= 0) return cached;

    /* snapshot cur's set (ldfa_intern may realloc L->st), build the successor */
    memcpy(L->curb, L->st[cur].bits, (size_t)L->nwords * 8);
    int ns = 0;
    for (int pc = 0; pc < L->re->ninst; pc++) {
        if (!BIT_TEST(L->curb, pc)) continue;
        const Inst *in = &L->re->insts[pc];
        if (in->op == OP_CHAR)  { if (in->c == (unsigned char)b) L->seeds[ns++] = pc + 1; }
        else if (in->op == OP_CLASS) { if (re_class_test(L->re, in->cls, b)) L->seeds[ns++] = pc + 1; }
    }
    L->seeds[ns++] = 0;   /* unanchored restart */
    closure(L->re, L->seeds, ns, /*bol=*/0, /*allow_eol=*/0, L->tmp, L->nwords, L->stack);

    /* already cached as a state? (then we can also memoise cur -> it) */
    uint64_t h = hash_bits(L->tmp, L->nwords);
    int i = (int)(h & (uint64_t)L->htmask);
    while (L->ht[i] != -1) {
        int idx = L->ht[i];
        if (memcmp(L->st[idx].bits, L->tmp, (size_t)L->nwords * 8) == 0) { L->st[cur].trans[b] = idx; return idx; }
        i = (i + 1) & L->htmask;
    }
    /* a new state: flush first if the cache is full (then cur is gone — don't
     * memoise). ldfa_flush() reuses L->tmp for the start closure, so stash the
     * target bits in curb (no longer needed) before flushing. */
    if (L->n >= L->budget) {
        memcpy(L->curb, L->tmp, (size_t)L->nwords * 8);
        ldfa_flush(L);
        return ldfa_intern(L, L->curb);
    }
    int id = ldfa_intern(L, L->tmp);
    if (id >= 0) L->st[cur].trans[b] = id;   /* L->st re-fetched (realloc-safe) */
    return id;
}

ReLdfa *re_ldfa_build(const Regex *re, int budget) {
    if (!re || re->ninst < 1) return NULL;
    if (dfa_has_hard_anchor(re)) return NULL;    /* non-terminal $ -> NFA */
    struct ReLdfa *L = calloc(1, sizeof *L);
    if (!L) return NULL;
    L->re = re; L->nwords = (re->ninst + 63) / 64; L->match_pc = re->ninst - 1;
    L->budget = (budget > 0) ? budget : LDFA_DEFAULT_BUDGET;
    if (L->budget < 8) L->budget = 8;
    int htcap = 1; while (htcap < L->budget * 2) htcap <<= 1;
    L->ht = malloc((size_t)htcap * sizeof(int)); L->htmask = htcap - 1;
    L->stack = malloc((size_t)re->ninst * sizeof(int));
    L->seeds = malloc(((size_t)re->ninst + 1) * sizeof(int));
    L->seeds2 = malloc((size_t)re->ninst * sizeof(int));
    L->frontier = malloc((size_t)re->ninst * sizeof(int));
    L->tmp = malloc((size_t)L->nwords * 8);
    L->tmp2 = malloc((size_t)L->nwords * 8);
    L->curb = malloc((size_t)L->nwords * 8);
    if (!L->ht || !L->stack || !L->seeds || !L->seeds2 || !L->frontier || !L->tmp || !L->tmp2 || !L->curb) {
        re_ldfa_free(L); return NULL;
    }
    for (int i = 0; i < htcap; i++) L->ht[i] = -1;
    int seed0 = 0;
    closure(re, &seed0, 1, /*bol=*/1, /*allow_eol=*/0, L->tmp, L->nwords, L->stack);
    L->start = ldfa_intern(L, L->tmp);
    if (L->start < 0) { re_ldfa_free(L); return NULL; }
    return L;
}

void re_ldfa_free(ReLdfa *L) {
    if (!L) return;
    for (int i = 0; i < L->n; i++) { free(L->st[i].bits); free(L->st[i].trans); }
    free(L->st); free(L->ht); free(L->stack); free(L->seeds); free(L->seeds2);
    free(L->frontier); free(L->tmp); free(L->tmp2); free(L->curb); free(L);
}

int re_ldfa_search(ReLdfa *L, const char *text, size_t len) {
    const unsigned char *s = (const unsigned char *)text;
    int cur = L->start;
    for (size_t i = 0; ; i++) {
        if (L->st[cur].acc_noeol) return 1;
        if (L->st[cur].acc_eol) {
            if (i == len) return 1;
            if (i + 1 == len && s[i] == '\n') return 1;
        }
        if (i == len) break;
        int nx = ldfa_step(L, cur, s[i]);
        if (nx < 0) return 0;    /* OOM (shouldn't happen with a sane budget) */
        cur = nx;
    }
    return 0;
}

int re_ldfa_peak_states(const ReLdfa *L) { return L ? L->n : 0; }

/* Exported wrappers over the static primitives, for the lazy-Sheng backend
 * (re_lsheng.c) so it builds states from the SAME closure/anchor logic. */
int re_closure(const Regex *re, const int *seeds, int nseed, int bol, int allow_eol,
               uint64_t *out, int nwords, int *stack) {
    return closure(re, seeds, nseed, bol, allow_eol, out, nwords, stack);
}
int re_dfa_hard_anchor(const Regex *re) { return dfa_has_hard_anchor(re); }
