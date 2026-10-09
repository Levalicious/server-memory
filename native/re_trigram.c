/*
 * re_trigram.c — Cox trigram prefilter over the regex AST (see re_trigram.h).
 *
 * Pipeline mirrors src/regex_query.ts + src/trigram.ts, ported to C over
 * ReNode and made case-EXACT. The Info lattice per node:
 *
 *   emptyable : the subtree can match the empty string
 *   exact     : bounded set of complete matches, or OPEN (unbounded/unknown)
 *   prefix    : bounded set of strings each match must begin with, or OPEN
 *   suffix    : symmetric to prefix
 *   match     : boolean trigram query that must hold for ANY match
 *
 * Concatenation x·y is where trigrams appear: a trigram straddling the x/y
 * boundary (from x's suffixes glued to y's prefixes) is required, which is how
 * `foo.*bar` recovers AND('foo','bar') even though neither side alone forces a
 * 3-byte window past the join.
 *
 * SOUNDNESS RULE, applied everywhere: when precision would cost unbounded work
 * (or an allocation fails), we degrade toward OPEN / match-all. That can only
 * make the filter weaker (keep more candidates), never drop a real match.
 *
 * Everything for one extraction lives in a bump arena freed as a unit, so the
 * churny cross/union intermediates never leak (ASan-clean by construction).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "re_trigram.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Cap on exact/prefix/suffix set sizes; past this a combinator collapses to
 * OPEN. Matches the TS SS_LIMIT. */
#define SS_LIMIT 256
/* A char class is enumerated into single-byte members only when it is at most
 * this dense; `.` (255 bytes), negated classes and \D/\W/\S exceed it and go
 * OPEN (sound: they impose no trigram constraint). \w is 63, \d is 10. */
#define CLASS_ENUM_LIMIT 64

/* ===========================================================================
 * Bump arena
 * =========================================================================== */
typedef struct ABlock { struct ABlock *next; size_t used, cap; unsigned char *data; } ABlock;
typedef struct { ABlock *head; } Arena;

#define ARENA_BLOCK ((size_t)1 << 16)

static void *arena_alloc(Arena *a, size_t n) {
    n = (n + 15u) & ~(size_t)15u;
    ABlock *b = a->head;
    if (!b || b->used + n > b->cap) {
        size_t cap = ARENA_BLOCK;
        if (n > cap) cap = n;
        ABlock *nb = (ABlock *)malloc(sizeof *nb);
        if (!nb) return NULL;
        nb->data = (unsigned char *)malloc(cap);
        if (!nb->data) { free(nb); return NULL; }
        nb->used = 0; nb->cap = cap; nb->next = a->head;
        a->head = nb; b = nb;
    }
    void *p = b->data + b->used;
    b->used += n;
    return p;
}

static void arena_dispose(Arena *a) {
    for (ABlock *b = a->head; b; ) { ABlock *n = b->next; free(b->data); free(b); b = n; }
    a->head = NULL;
}

/* ===========================================================================
 * Trigram query (TQ) — boolean expression over 24-bit trigrams
 * =========================================================================== */
typedef enum { TQ_ALL, TQ_NONE, TQ_TRI, TQ_AND, TQ_OR } TQKind;
typedef struct TQ { TQKind kind; uint32_t tri; struct TQ **ks; int nk; } TQ;

/* ALL/NONE are immutable singletons — no allocation, so the soundness-degrade
 * path (return tq_all on OOM) can never itself fail. */
static const TQ TQ_ALL_NODE  = { TQ_ALL,  0, NULL, 0 };
static const TQ TQ_NONE_NODE = { TQ_NONE, 0, NULL, 0 };
static TQ *tq_all (void) { return (TQ *)&TQ_ALL_NODE;  }
static TQ *tq_none(void) { return (TQ *)&TQ_NONE_NODE; }

static TQ *tq_tri(Arena *a, uint32_t t) {
    TQ *q = (TQ *)arena_alloc(a, sizeof *q);
    if (!q) return tq_all();               /* degrade: weaker filter, sound */
    q->kind = TQ_TRI; q->tri = t; q->ks = NULL; q->nk = 0;
    return q;
}

/* Shared tail for tq_and/tq_or: `buf[0..m)` are the flattened survivors,
 * already free of the identity element; `mkkind` is the node kind to emit. */
static TQ *tq_finish(Arena *a, TQ **buf, int m, TQKind mkkind, TQ *identity) {
    /* dedupe trigram leaves (a repeated trigram doesn't tighten the query) */
    int out = 0;
    for (int i = 0; i < m; i++) {
        TQ *q = buf[i];
        if (q->kind == TQ_TRI) {
            int dup = 0;
            for (int k = 0; k < out; k++)
                if (buf[k]->kind == TQ_TRI && buf[k]->tri == q->tri) { dup = 1; break; }
            if (dup) continue;
        }
        buf[out++] = q;
    }
    if (out == 0) return identity;
    if (out == 1) return buf[0];
    TQ  *node = (TQ  *)arena_alloc(a, sizeof *node);
    TQ **ks   = (TQ **)arena_alloc(a, (size_t)out * sizeof *ks);
    if (!node || !ks) return identity;     /* degrade to identity (all/none) */
    memcpy(ks, buf, (size_t)out * sizeof *ks);
    node->kind = mkkind; node->tri = 0; node->ks = ks; node->nk = out;
    return node;
}

/* AND: drop ALL operands; any NONE => NONE; flatten nested AND; 0 => ALL. */
static TQ *tq_and(Arena *a, TQ **qs, int n) {
    int cap = 0;
    for (int i = 0; i < n; i++) cap += (qs[i]->kind == TQ_AND) ? qs[i]->nk : 1;
    if (cap == 0) return tq_all();
    TQ **buf = (TQ **)malloc((size_t)cap * sizeof *buf);
    if (!buf) return tq_all();
    int m = 0;
    for (int i = 0; i < n; i++) {
        TQ *q = qs[i];
        if (q->kind == TQ_ALL)  continue;
        if (q->kind == TQ_NONE) { free(buf); return tq_none(); }
        if (q->kind == TQ_AND)  { for (int j = 0; j < q->nk; j++) buf[m++] = q->ks[j]; }
        else                    buf[m++] = q;
    }
    TQ *res = tq_finish(a, buf, m, TQ_AND, tq_all());
    free(buf);
    return res;
}

/* OR: any ALL operand => ALL; drop NONE; flatten nested OR; 0 => NONE. */
static TQ *tq_or(Arena *a, TQ **qs, int n) {
    int cap = 0;
    for (int i = 0; i < n; i++) cap += (qs[i]->kind == TQ_OR) ? qs[i]->nk : 1;
    if (cap == 0) return tq_none();
    TQ **buf = (TQ **)malloc((size_t)cap * sizeof *buf);
    if (!buf) return tq_all();             /* can't build OR => no filter */
    int m = 0;
    for (int i = 0; i < n; i++) {
        TQ *q = qs[i];
        if (q->kind == TQ_ALL)  { free(buf); return tq_all(); }
        if (q->kind == TQ_NONE) continue;
        if (q->kind == TQ_OR)   { for (int j = 0; j < q->nk; j++) buf[m++] = q->ks[j]; }
        else                    buf[m++] = q;
    }
    TQ *res = tq_finish(a, buf, m, TQ_OR, tq_none());
    free(buf);
    return res;
}

/* ===========================================================================
 * Bounded string sets (SS) + per-node Info
 * =========================================================================== */
typedef struct { uint8_t *b; int len; } Bytes;
typedef struct { Bytes *v; int n; int open; } SS;   /* open => unbounded/unknown */

typedef struct { int emptyable; SS exact, prefix, suffix; TQ *match; } Info;

static SS ss_open(void)  { SS s; s.v = NULL; s.n = 0; s.open = 1; return s; }

static uint32_t pack_tri(const uint8_t *b, int i) {
    return (uint32_t)b[i] | ((uint32_t)b[i + 1] << 8) | ((uint32_t)b[i + 2] << 16);
}

static int bytes_eq(const Bytes *x, const Bytes *y) {
    return x->len == y->len && (x->len == 0 || memcmp(x->b, y->b, (size_t)x->len) == 0);
}

/* A set holding exactly one member of `len` bytes copied from `p` (len may be
 * 0 for the empty string). Returns OPEN on OOM. */
static SS ss_one(Arena *a, const uint8_t *p, int len) {
    Bytes *v = (Bytes *)arena_alloc(a, sizeof *v);
    if (!v) return ss_open();
    if (len > 0) {
        v->b = (uint8_t *)arena_alloc(a, (size_t)len);
        if (!v->b) return ss_open();
        memcpy(v->b, p, (size_t)len);
    } else v->b = NULL;
    v->len = len;
    SS s; s.v = v; s.n = 1; s.open = 0; return s;
}

/* Append `cand` to `dst` (dedup by value). Returns 0 and marks dst OPEN if it
 * would exceed SS_LIMIT. dst->v must have room for SS_LIMIT members. */
static int ss_push(SS *dst, const Bytes *cand) {
    for (int i = 0; i < dst->n; i++) if (bytes_eq(&dst->v[i], cand)) return 1;
    if (dst->n >= SS_LIMIT) { dst->open = 1; return 0; }
    dst->v[dst->n++] = *cand;
    return 1;
}

static Bytes bytes_cat(Arena *a, const Bytes *x, const Bytes *y) {
    Bytes r;
    r.len = x->len + y->len;
    if (r.len == 0) { r.b = NULL; return r; }
    r.b = (uint8_t *)arena_alloc(a, (size_t)r.len);
    if (!r.b) { r.len = -1; return r; }     /* signal OOM */
    if (x->len) memcpy(r.b,            x->b, (size_t)x->len);
    if (y->len) memcpy(r.b + x->len,   y->b, (size_t)y->len);
    return r;
}

/* Cross product x·y (concatenation of every pair). OPEN if either side is OPEN
 * or the product would exceed the cap. */
static SS ss_cross(Arena *a, const SS *x, const SS *y) {
    if (x->open || y->open) return ss_open();
    if ((long)x->n * (long)y->n > SS_LIMIT) return ss_open();
    SS r; r.open = 0; r.n = 0;
    r.v = (Bytes *)arena_alloc(a, (size_t)SS_LIMIT * sizeof *r.v);
    if (!r.v) return ss_open();
    for (int i = 0; i < x->n; i++)
        for (int j = 0; j < y->n; j++) {
            Bytes c = bytes_cat(a, &x->v[i], &y->v[j]);
            if (c.len < 0) return ss_open();
            if (!ss_push(&r, &c)) return r;   /* hit cap => OPEN, already marked */
        }
    return r;
}

static SS ss_union(Arena *a, const SS *x, const SS *y) {
    if (x->open || y->open) return ss_open();
    SS r; r.open = 0; r.n = 0;
    r.v = (Bytes *)arena_alloc(a, (size_t)SS_LIMIT * sizeof *r.v);
    if (!r.v) return ss_open();
    for (int i = 0; i < x->n; i++) if (!ss_push(&r, &x->v[i])) return r;
    for (int i = 0; i < y->n; i++) if (!ss_push(&r, &y->v[i])) return r;
    return r;
}

/* Trigram query straddling the x-suffix / y-prefix join: OR over every (s,p)
 * pair of the AND of the trigrams that span the boundary in s+p. */
static TQ *boundary_query(Arena *a, const SS *xs, const SS *yp) {
    if (xs->open || yp->open) return tq_all();
    if (xs->n == 0 || yp->n == 0) return tq_all();
    TQ **branches = (TQ **)malloc((size_t)xs->n * (size_t)yp->n * sizeof *branches);
    if (!branches) return tq_all();
    int nb = 0;
    for (int i = 0; i < xs->n; i++)
        for (int j = 0; j < yp->n; j++) {
            Bytes comb = bytes_cat(a, &xs->v[i], &yp->v[j]);
            if (comb.len < 0) { free(branches); return tq_all(); }
            int slen = xs->v[i].len;
            int lo = slen - 2; if (lo < 0) lo = 0;
            int hi = comb.len - 3; if (hi > slen - 1) hi = slen - 1;
            if (hi < lo) { branches[nb++] = tq_all(); continue; }  /* no spanning tri */
            int ntri = hi - lo + 1;
            TQ **tris = (TQ **)malloc((size_t)ntri * sizeof *tris);
            if (!tris) { free(branches); return tq_all(); }
            int t = 0;
            for (int k = lo; k <= hi; k++) tris[t++] = tq_tri(a, pack_tri(comb.b, k));
            branches[nb++] = tq_and(a, tris, t);
            free(tris);
        }
    TQ *res = tq_or(a, branches, nb);
    free(branches);
    return res;
}

/* ---- leaf Info builders --------------------------------------------------- */
static Info info_empty(Arena *a) {                 /* epsilon / anchors */
    Info in; in.emptyable = 1;
    in.exact = ss_one(a, NULL, 0); in.prefix = in.exact; in.suffix = in.exact;
    in.match = tq_all(); return in;
}
static Info info_open(void) {                      /* dot / negated / \D\W\S */
    Info in; in.emptyable = 0;
    in.exact = ss_open(); in.prefix = ss_open(); in.suffix = ss_open();
    in.match = tq_all(); return in;
}
static Info info_literal(Arena *a, uint8_t c) {    /* single byte, case-EXACT */
    Info in; in.emptyable = 0;
    in.exact = ss_one(a, &c, 1); in.prefix = in.exact; in.suffix = in.exact;
    in.match = tq_all(); return in;
}
static Info info_class(Arena *a, const unsigned char set[32]) {
    int pc = 0;
    for (int i = 0; i < 32; i++) for (int bit = 0; bit < 8; bit++) pc += (set[i] >> bit) & 1;
    if (pc == 0 || pc > CLASS_ENUM_LIMIT) return info_open();
    SS s; s.open = 0; s.n = 0;
    s.v = (Bytes *)arena_alloc(a, (size_t)pc * sizeof *s.v);
    if (!s.v) return info_open();
    for (int byte = 0; byte < 256; byte++)
        if ((set[byte >> 3] >> (byte & 7)) & 1) {
            uint8_t b = (uint8_t)byte;
            Bytes m; m.b = (uint8_t *)arena_alloc(a, 1);
            if (!m.b) return info_open();
            m.b[0] = b; m.len = 1;
            s.v[s.n++] = m;
        }
    Info in; in.emptyable = 0; in.exact = s; in.prefix = s; in.suffix = s;
    in.match = tq_all(); return in;
}

/* ---- combinators ---------------------------------------------------------- */
static Info info_concat(Arena *a, Info x, Info y) {
    Info in; in.emptyable = x.emptyable && y.emptyable;

    /* exact: cross of both, only when both finite */
    if (!x.exact.open && !y.exact.open) in.exact = ss_cross(a, &x.exact, &y.exact);
    else in.exact = ss_open();

    /* prefix */
    if (!x.exact.open) {
        if (!y.prefix.open) {
            SS c = ss_cross(a, &x.exact, &y.prefix);
            in.prefix = c.open ? x.prefix : c;   /* over-cap => less-precise fallback */
        } else in.prefix = x.prefix;
    } else {
        in.prefix = x.prefix;
        if (x.emptyable && !in.prefix.open && !y.prefix.open)
            in.prefix = ss_union(a, &in.prefix, &y.prefix);
    }

    /* suffix (symmetric) */
    if (!y.exact.open) {
        if (!x.suffix.open) {
            SS c = ss_cross(a, &x.suffix, &y.exact);
            in.suffix = c.open ? y.suffix : c;
        } else in.suffix = y.suffix;
    } else {
        in.suffix = y.suffix;
        if (y.emptyable && !in.suffix.open && !x.suffix.open)
            in.suffix = ss_union(a, &in.suffix, &x.suffix);
    }

    TQ *parts[3] = { x.match, y.match, boundary_query(a, &x.suffix, &y.prefix) };
    in.match = tq_and(a, parts, 3);
    return in;
}

static Info info_alt(Arena *a, Info x, Info y) {
    Info in; in.emptyable = x.emptyable || y.emptyable;
    in.exact  = (!x.exact.open  && !y.exact.open ) ? ss_union(a, &x.exact,  &y.exact ) : ss_open();
    in.prefix = (!x.prefix.open && !y.prefix.open) ? ss_union(a, &x.prefix, &y.prefix) : ss_open();
    in.suffix = (!x.suffix.open && !y.suffix.open) ? ss_union(a, &x.suffix, &y.suffix) : ss_open();
    TQ *parts[2] = { x.match, y.match };
    in.match = tq_or(a, parts, 2);
    return in;
}

static Info info_star(void) {          /* matches empty => require nothing */
    Info in; in.emptyable = 1;
    in.exact = ss_open(); in.prefix = ss_open(); in.suffix = ss_open();
    in.match = tq_all(); return in;
}
static Info info_plus(Info x) {        /* x x* : at least one x is required */
    Info in; in.emptyable = x.emptyable;
    in.exact = ss_open(); in.prefix = x.prefix; in.suffix = x.suffix; in.match = x.match;
    return in;
}
static Info info_quest(Arena *a, Info x) { return info_alt(a, x, info_empty(a)); }

static Info compute_info(Arena *a, const ReNode *n);

static Info info_repeat(Arena *a, const ReNode *n) {
    Info x = compute_info(a, n->a);
    int mn = n->min, mx = n->max;              /* mx < 0 => unbounded */
    if (mn == 0 && mx == 1)  return info_quest(a, x);
    if (mn == 0 && mx < 0)   return info_star();
    if (mn == 1 && mx < 0)   return info_plus(x);
    if (mn >= 1 && mx == mn) {                 /* x{n} = x concatenated n times */
        Info acc = x;
        for (int i = 1; i < mn; i++) acc = info_concat(a, acc, x);
        return acc;
    }
    if (mn >= 1) {                             /* x{min,max} = x{min} then any tail */
        Info acc = x;
        for (int i = 1; i < mn; i++) acc = info_concat(a, acc, x);
        return info_concat(a, acc, info_star());
    }
    return info_star();                        /* min == 0 => matches empty */
}

static Info compute_info(Arena *a, const ReNode *n) {
    switch (n->kind) {
        case RE_EMPTY:  return info_empty(a);
        case RE_CHAR:   return info_literal(a, n->c);
        case RE_CLASS:  return info_class(a, n->set);
        case RE_BOL:
        case RE_EOL:    return info_empty(a);          /* zero-width anchors */
        case RE_CONCAT: return info_concat(a, compute_info(a, n->a), compute_info(a, n->b));
        case RE_ALT:    return info_alt(a, compute_info(a, n->a), compute_info(a, n->b));
        case RE_STAR:   (void)compute_info(a, n->a); return info_star();
        case RE_PLUS:   return info_plus(compute_info(a, n->a));
        case RE_QUEST:  return info_quest(a, compute_info(a, n->a));
        case RE_REPEAT: return info_repeat(a, n);
    }
    return info_open();                                 /* unreachable */
}

/* Query built purely from the finite `exact` set: OR over members, each an AND
 * of its own trigrams. Returns NULL (caller falls back to info.match) if any
 * member is shorter than 3 bytes — such a member would slip past the index. */
static TQ *tq_from_exact(Arena *a, const SS *ex) {
    if (ex->open || ex->n == 0) return NULL;
    TQ **branches = (TQ **)malloc((size_t)ex->n * sizeof *branches);
    if (!branches) return NULL;
    int nb = 0;
    for (int i = 0; i < ex->n; i++) {
        const Bytes *e = &ex->v[i];
        if (e->len < 3) { free(branches); return NULL; }
        int ntri = e->len - 2;
        TQ **tris = (TQ **)malloc((size_t)ntri * sizeof *tris);
        if (!tris) { free(branches); return NULL; }
        for (int k = 0; k + 3 <= e->len; k++) tris[k] = tq_tri(a, pack_tri(e->b, k));
        branches[nb++] = tq_and(a, tris, ntri);
        free(tris);
    }
    TQ *res = tq_or(a, branches, nb);
    free(branches);
    return res;
}

/* ===========================================================================
 * Public: query build / free / classify / debug
 * =========================================================================== */
struct ReTrigramQuery { Arena arena; TQ *root; };

ReTrigramQuery *re_trigram_build(const ReNode *ast) {
    if (!ast) return NULL;
    ReTrigramQuery *q = (ReTrigramQuery *)calloc(1, sizeof *q);
    if (!q) return NULL;
    Info info = compute_info(&q->arena, ast);
    TQ *root = NULL;
    if (!info.exact.open && info.exact.n > 0) root = tq_from_exact(&q->arena, &info.exact);
    if (!root) root = info.match;              /* may be ALL (no filter) */
    q->root = root ? root : tq_all();
    return q;
}

void re_trigram_free(ReTrigramQuery *q) {
    if (!q) return;
    arena_dispose(&q->arena);
    free(q);
}

int re_trigram_is_all (const ReTrigramQuery *q) { return q && q->root->kind == TQ_ALL;  }
int re_trigram_is_none(const ReTrigramQuery *q) { return q && q->root->kind == TQ_NONE; }

/* ---- debug renderer ------------------------------------------------------- */
typedef struct { char *p; size_t left; } Sink;
static void sink_puts(Sink *s, const char *str) {
    while (*str && s->left > 1) { *s->p++ = *str++; s->left--; }
}
static void sink_tri(Sink *s, uint32_t t) {
    uint8_t bs[3] = { (uint8_t)(t & 0xff), (uint8_t)((t >> 8) & 0xff), (uint8_t)((t >> 16) & 0xff) };
    sink_puts(s, "'");
    for (int i = 0; i < 3; i++) {
        uint8_t c = bs[i];
        if (c >= 0x20 && c < 0x7f && c != '\'' && c != '\\') {
            if (s->left > 1) { *s->p++ = (char)c; s->left--; }
        } else {
            char buf[5]; snprintf(buf, sizeof buf, "\\x%02x", c); sink_puts(s, buf);
        }
    }
    sink_puts(s, "'");
}
static void sink_tq(Sink *s, const TQ *q) {
    switch (q->kind) {
        case TQ_ALL:  sink_puts(s, "ALL");  break;
        case TQ_NONE: sink_puts(s, "NONE"); break;
        case TQ_TRI:  sink_tri(s, q->tri);  break;
        case TQ_AND:
        case TQ_OR:
            sink_puts(s, q->kind == TQ_AND ? "AND(" : "OR(");
            for (int i = 0; i < q->nk; i++) { if (i) sink_puts(s, ","); sink_tq(s, q->ks[i]); }
            sink_puts(s, ")");
            break;
    }
}
char *re_trigram_debug(const ReTrigramQuery *q, char *buf, size_t cap) {
    if (!buf || cap == 0) return buf;
    Sink s = { buf, cap };
    if (q && q->root) sink_tq(&s, q->root); else sink_puts(&s, "ALL");
    *s.p = '\0';
    return buf;
}

/* ===========================================================================
 * Inverted index (immutable CSR: trigram -> ascending posting list)
 * =========================================================================== */
struct ReTrigramIndex {
    uint32_t  ndocs;
    uint32_t  ntri;       /* distinct trigrams               */
    uint32_t *tris;       /* [ntri] ascending                */
    uint32_t *offs;       /* [ntri+1] slice bounds in docids */
    uint32_t *docids;     /* flat posting lists              */
    uint32_t  ndocids;
};

static int cmp_u64(const void *pa, const void *pb) {
    uint64_t a = *(const uint64_t *)pa, b = *(const uint64_t *)pb;
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}
static int cmp_u32(const void *pa, const void *pb);   /* defined below; radix OOM fallback */

/* Persistent radix scratch: allocated on the first large sort, grown on demand,
 * released once at process exit. No per-sort malloc/free; and because the
 * ping-pong scatter fully overwrites the destination every pass, the scratch is
 * NEVER zeroed. Single-threaded (the index is single-writer). */
static void  *g_rscratch = NULL;
static size_t g_rscratch_bytes = 0;
static void rscratch_free(void) { free(g_rscratch); g_rscratch = NULL; g_rscratch_bytes = 0; }
static void *rscratch(size_t bytes) {
    if (g_rscratch_bytes >= bytes) return g_rscratch;
    void *n = realloc(g_rscratch, bytes);
    if (!n) return NULL;
    if (!g_rscratch) atexit(rscratch_free);       /* register cleanup once */
    g_rscratch = n; g_rscratch_bytes = bytes;
    return g_rscratch;
}

static void isort_u32(uint32_t *a, size_t n) {
    for (size_t i = 1; i < n; i++) { uint32_t v = a[i]; size_t j = i; while (j && a[j-1] > v) { a[j] = a[j-1]; j--; } a[j] = v; }
}
static void isort_u64(uint64_t *a, size_t n) {
    for (size_t i = 1; i < n; i++) { uint64_t v = a[i]; size_t j = i; while (j && a[j-1] > v) { a[j] = a[j-1]; j--; } a[j] = v; }
}

/* Ascending sort: insertion for small n, else LSD 8-bit radix — ping-pong into
 * the persistent scratch, skip passes with a single populated bucket, prefetch
 * the scatter's write cursor. Adapted from psat/abomination sortlastnum.
 * Comparison-sort fallback only if the scratch alloc fails. */
static void radix_u32(uint32_t *a, size_t n) {
    if (n < 64) { isort_u32(a, n); return; }
    uint32_t *tmp = (uint32_t *)rscratch(n * sizeof *tmp);
    if (!tmp) { qsort(a, n, sizeof *a, cmp_u32); return; }
    uint32_t *src = a, *dst = tmp;
    for (int shift = 0; shift < 32; shift += 8) {
        size_t cnt[256]; memset(cnt, 0, sizeof cnt);
        for (size_t i = 0; i < n; i++) cnt[(src[i] >> shift) & 0xFFu]++;
        int ne = 0; for (int b = 0; b < 256; b++) if (cnt[b]) ne++;
        if (ne == 1) continue;                                  /* all share this digit */
        size_t off = 0; for (int b = 0; b < 256; b++) { size_t c = cnt[b]; cnt[b] = off; off += c; }
        for (size_t i = 0; i < n; i++) {
            unsigned d = (src[i] >> shift) & 0xFFu;
            dst[cnt[d]++] = src[i];
            __builtin_prefetch(dst + cnt[d] + 1, 1, 3);         /* next write slot, bucket d */
        }
        uint32_t *t = src; src = dst; dst = t;                  /* pointer-swap, no memcpy */
    }
    if (src != a) memcpy(a, src, n * sizeof *a);
}
static void radix_u64(uint64_t *a, size_t n) {
    if (n < 64) { isort_u64(a, n); return; }
    uint64_t *tmp = (uint64_t *)rscratch(n * sizeof *tmp);
    if (!tmp) { qsort(a, n, sizeof *a, cmp_u64); return; }
    uint64_t *src = a, *dst = tmp;
    for (int shift = 0; shift < 64; shift += 8) {
        size_t cnt[256]; memset(cnt, 0, sizeof cnt);
        for (size_t i = 0; i < n; i++) cnt[(src[i] >> shift) & 0xFFu]++;
        int ne = 0; for (int b = 0; b < 256; b++) if (cnt[b]) ne++;
        if (ne == 1) continue;
        size_t off = 0; for (int b = 0; b < 256; b++) { size_t c = cnt[b]; cnt[b] = off; off += c; }
        for (size_t i = 0; i < n; i++) {
            unsigned d = (src[i] >> shift) & 0xFFu;
            dst[cnt[d]++] = src[i];
            __builtin_prefetch(dst + cnt[d] + 1, 1, 3);
        }
        uint64_t *t = src; src = dst; dst = t;
    }
    if (src != a) memcpy(a, src, n * sizeof *a);
}

ReTrigramIndex *re_trigram_index_build(const ReDoc *docs, uint32_t ndocs) {
    /* one (trigram<<32 | docid) pair per window; dedup happens after the sort */
    size_t npairs = 0;
    for (uint32_t d = 0; d < ndocs; d++) if (docs[d].len >= 3) npairs += docs[d].len - 2;

    uint64_t *pairs = NULL;
    if (npairs) {
        pairs = (uint64_t *)malloc(npairs * sizeof *pairs);
        if (!pairs) return NULL;
    }
    size_t k = 0;
    for (uint32_t d = 0; d < ndocs; d++) {
        const uint8_t *b = (const uint8_t *)docs[d].ptr;
        size_t L = docs[d].len;
        for (size_t i = 0; i + 3 <= L; i++)
            pairs[k++] = ((uint64_t)pack_tri(b, (int)i) << 32) | d;
    }
    if (npairs) radix_u64(pairs, npairs);

    /* dedupe identical (trigram,doc) pairs */
    size_t m = 0;
    for (size_t i = 0; i < npairs; i++)
        if (i == 0 || pairs[i] != pairs[i - 1]) pairs[m++] = pairs[i];

    uint32_t ntri = 0;
    for (size_t i = 0; i < m; i++)
        if (i == 0 || (pairs[i] >> 32) != (pairs[i - 1] >> 32)) ntri++;

    ReTrigramIndex *idx = (ReTrigramIndex *)calloc(1, sizeof *idx);
    if (!idx) { free(pairs); return NULL; }
    idx->ndocs = ndocs; idx->ntri = ntri; idx->ndocids = (uint32_t)m;
    idx->tris   = ntri ? (uint32_t *)malloc((size_t)ntri * sizeof *idx->tris) : NULL;
    idx->offs   = (uint32_t *)malloc(((size_t)ntri + 1) * sizeof *idx->offs);
    idx->docids = m ? (uint32_t *)malloc(m * sizeof *idx->docids) : NULL;
    if ((ntri && !idx->tris) || !idx->offs || (m && !idx->docids)) {
        re_trigram_index_free(idx); free(pairs); return NULL;
    }
    uint32_t ti = 0; size_t di = 0; int64_t cur = -1;
    for (size_t i = 0; i < m; i++) {
        uint32_t t   = (uint32_t)(pairs[i] >> 32);
        uint32_t doc = (uint32_t)(pairs[i] & 0xffffffffu);
        if ((int64_t)t != cur) { cur = t; idx->tris[ti] = t; idx->offs[ti] = (uint32_t)di; ti++; }
        idx->docids[di++] = doc;
    }
    idx->offs[ntri] = (uint32_t)m;
    free(pairs);
    return idx;
}

void re_trigram_index_free(ReTrigramIndex *idx) {
    if (!idx) return;
    free(idx->tris); free(idx->offs); free(idx->docids); free(idx);
}
uint32_t re_trigram_index_ndocs   (const ReTrigramIndex *idx) { return idx ? idx->ndocs : 0; }
uint32_t re_trigram_index_distinct(const ReTrigramIndex *idx) { return idx ? idx->ntri  : 0; }

/* posting slice for `t`, or NULL if absent (=> empty). */
static const uint32_t *idx_lookup(const ReTrigramIndex *idx, uint32_t t, uint32_t *out_n) {
    uint32_t lo = 0, hi = idx->ntri;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (idx->tris[mid] < t) lo = mid + 1; else hi = mid;
    }
    if (lo < idx->ntri && idx->tris[lo] == t) {
        *out_n = idx->offs[lo + 1] - idx->offs[lo];
        return idx->docids + idx->offs[lo];
    }
    *out_n = 0;
    return NULL;
}

/* ===========================================================================
 * Evaluation
 * =========================================================================== */
/* Owned candidate list. all=1 => "no constraint"; ids/n unused. */
typedef struct { uint32_t *ids; uint32_t n; int all; } CL;

static CL cl_all(void)   { CL c; c.ids = NULL; c.n = 0; c.all = 1; return c; }
static CL cl_empty(void) { CL c; c.ids = NULL; c.n = 0; c.all = 0; return c; }
static void cl_free(CL *c) { free(c->ids); c->ids = NULL; c->n = 0; }

static CL cl_copy(const uint32_t *v, uint32_t n) {
    CL c; c.all = 0; c.n = n;
    c.ids = n ? (uint32_t *)malloc((size_t)n * sizeof *c.ids) : NULL;
    if (n && !c.ids) return cl_all();          /* OOM => no constraint (sound) */
    if (n) memcpy(c.ids, v, (size_t)n * sizeof *c.ids);
    return c;
}

/* ascending-sorted intersection, consuming neither input */
static CL cl_intersect(const CL *a, const CL *b) {
    CL r; r.all = 0; r.n = 0;
    uint32_t cap = a->n < b->n ? a->n : b->n;
    r.ids = cap ? (uint32_t *)malloc((size_t)cap * sizeof *r.ids) : NULL;
    if (cap && !r.ids) return cl_all();
    uint32_t i = 0, j = 0;
    while (i < a->n && j < b->n) {
        if      (a->ids[i] < b->ids[j]) i++;
        else if (a->ids[i] > b->ids[j]) j++;
        else { r.ids[r.n++] = a->ids[i]; i++; j++; }
    }
    return r;
}

static int cmp_u32(const void *pa, const void *pb) {
    uint32_t a = *(const uint32_t *)pa, b = *(const uint32_t *)pb;
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}

static CL eval(const TQ *q, const ReTrigramIndex *idx) {
    switch (q->kind) {
        case TQ_ALL:  return cl_all();
        case TQ_NONE: return cl_empty();
        case TQ_TRI: {
            uint32_t n; const uint32_t *slice = idx_lookup(idx, q->tri, &n);
            return slice ? cl_copy(slice, n) : cl_empty();
        }
        case TQ_AND: {
            CL acc; int have = 0;
            for (int i = 0; i < q->nk; i++) {
                CL c = eval(q->ks[i], idx);
                if (c.all) { cl_free(&c); continue; }       /* no constraint */
                if (c.n == 0) { cl_free(&c); if (have) cl_free(&acc); return cl_empty(); }
                if (!have) { acc = c; have = 1; }
                else { CL n = cl_intersect(&acc, &c); cl_free(&acc); cl_free(&c); acc = n;
                       if (acc.all) return acc; }
            }
            return have ? acc : cl_all();
        }
        case TQ_OR: {
            /* union of children; any ALL child makes the whole OR unfilterable */
            uint32_t cap = 0, cnt = 0;
            CL *parts = q->nk ? (CL *)malloc((size_t)q->nk * sizeof *parts) : NULL;
            if (q->nk && !parts) return cl_all();
            for (int i = 0; i < q->nk; i++) {
                CL c = eval(q->ks[i], idx);
                if (c.all) { for (uint32_t z = 0; z < cnt; z++) cl_free(&parts[z]); free(parts); cl_free(&c); return cl_all(); }
                parts[cnt++] = c; cap += c.n;
            }
            CL merged; merged.all = 0; merged.n = 0;
            merged.ids = cap ? (uint32_t *)malloc((size_t)cap * sizeof *merged.ids) : NULL;
            if (cap && !merged.ids) { for (uint32_t z = 0; z < cnt; z++) cl_free(&parts[z]); free(parts); return cl_all(); }
            for (uint32_t z = 0; z < cnt; z++)
                for (uint32_t i = 0; i < parts[z].n; i++) merged.ids[merged.n++] = parts[z].ids[i];
            for (uint32_t z = 0; z < cnt; z++) cl_free(&parts[z]);
            free(parts);
            if (merged.n > 1) {
                radix_u32(merged.ids, merged.n);
                uint32_t w = 1;
                for (uint32_t i = 1; i < merged.n; i++) if (merged.ids[i] != merged.ids[w - 1]) merged.ids[w++] = merged.ids[i];
                merged.n = w;
            }
            return merged;
        }
    }
    return cl_all();
}

ReCandidates re_trigram_eval(const ReTrigramQuery *q, const ReTrigramIndex *idx) {
    ReCandidates out; out.ids = NULL; out.n = 0; out.all = 1;
    if (!q || !idx) return out;
    CL c = eval(q->root, idx);
    out.all = c.all; out.ids = c.ids; out.n = c.n;   /* transfer ownership */
    return out;
}

void re_candidates_free(ReCandidates *c) {
    if (!c) return;
    free(c->ids); c->ids = NULL; c->n = 0;
}

/* ===========================================================================
 * Integrated prefilter + verify
 * =========================================================================== */
uint32_t re_trigram_search(const Regex *re, const ReNode *ast,
                           const ReTrigramIndex *idx,
                           const ReDoc *docs, uint32_t ndocs,
                           uint32_t *out_ids, uint32_t out_cap,
                           ReTrigramStats *stats) {
    ReTrigramQuery *q = re_trigram_build(ast);
    ReCandidates    c = re_trigram_eval(q, idx);

    uint32_t matched = 0, verified = 0, candidates;
    int filtered;

    if (c.all) {
        candidates = ndocs; filtered = 0;
        for (uint32_t d = 0; d < ndocs; d++) {
            verified++;
            if (re_nfa_search(re, docs[d].ptr, docs[d].len)) {
                if (matched < out_cap) out_ids[matched] = d;
                matched++;
            }
        }
    } else {
        candidates = c.n; filtered = 1;
        for (uint32_t i = 0; i < c.n; i++) {
            uint32_t d = c.ids[i];
            if (d >= ndocs) continue;
            verified++;
            if (re_nfa_search(re, docs[d].ptr, docs[d].len)) {
                if (matched < out_cap) out_ids[matched] = d;
                matched++;
            }
        }
    }

    if (stats) {
        stats->total = ndocs; stats->candidates = candidates;
        stats->verified = verified; stats->matched = matched; stats->filtered = filtered;
    }
    re_candidates_free(&c);
    re_trigram_free(q);
    return matched;
}

/* ===========================================================================
 * Mutable live index (u64 doc ids) — incrementally maintained
 * ===========================================================================
 * Two open-addressing hashmaps: trigram -> sorted posting list (insert-only; a
 * trigram whose postings empty just keeps a 0-length bucket), and doc -> the
 * sorted trigram set it contributed (so remove/reindex can reverse it). On any
 * allocation failure `broken` is latched and every eval returns "all" (=> the
 * caller full-scans) — degrading to correct-but-slow, never to a dropped match.
 */
/* posting = open-addressing hash SET of u64 doc ids (0 = empty slot; entity
 * offsets are never 0). O(1) add / has / del (backward-shift), no sort, no
 * memmove — so incremental maintenance (esp. delete) stays O(1). */
typedef struct { uint32_t tri; uint64_t *slots; uint32_t cap, cnt; int used; } LTri;
typedef struct { uint64_t doc; uint32_t *tris; uint32_t ntri; int used; } LDoc;

struct ReTrigramLive {
    LTri    *tb; uint32_t tbcap, tbcnt;   /* trigram buckets */
    LDoc    *db; uint32_t dbcap, dbcnt;   /* doc buckets     */
    int      broken;
};

static uint32_t mix32(uint32_t x)  { x^=x>>16; x*=0x7feb352du; x^=x>>15; x*=0x846ca68bu; x^=x>>16; return x; }
static uint32_t mix64h(uint64_t x) { x^=x>>33; x*=0xff51afd7ed558ccdULL; x^=x>>33; x*=0xc4ceb9fe1a85ec53ULL; x^=x>>33; return (uint32_t)x; }

ReTrigramLive *re_trigram_live_new(void) {
    ReTrigramLive *L = (ReTrigramLive *)calloc(1, sizeof *L);
    if (!L) return NULL;
    L->tbcap = 1024; L->dbcap = 1024;
    L->tb = (LTri *)calloc(L->tbcap, sizeof *L->tb);
    L->db = (LDoc *)calloc(L->dbcap, sizeof *L->db);
    if (!L->tb || !L->db) { free(L->tb); free(L->db); free(L); return NULL; }
    return L;
}

void re_trigram_live_free(ReTrigramLive *L) {
    if (!L) return;
    for (uint32_t i = 0; i < L->tbcap; i++) if (L->tb[i].used) free(L->tb[i].slots);
    for (uint32_t i = 0; i < L->dbcap; i++) if (L->db[i].used) free(L->db[i].tris);
    free(L->tb); free(L->db); free(L);
}

/* ---- trigram bucket map (insert-only) ---- */
static void ltri_grow(ReTrigramLive *L) {
    uint32_t ncap = L->tbcap * 2, mask = ncap - 1;
    LTri *nt = (LTri *)calloc(ncap, sizeof *nt);
    if (!nt) { L->broken = 1; return; }
    for (uint32_t i = 0; i < L->tbcap; i++) if (L->tb[i].used) {
        uint32_t s = mix32(L->tb[i].tri) & mask;
        while (nt[s].used) s = (s + 1) & mask;
        nt[s] = L->tb[i];
    }
    free(L->tb); L->tb = nt; L->tbcap = ncap;
}
static LTri *ltri_get(ReTrigramLive *L, uint32_t tri, int create) {
    if (create && (uint64_t)(L->tbcnt + 1) * 10 >= (uint64_t)L->tbcap * 7) ltri_grow(L);
    uint32_t mask = L->tbcap - 1, slot = mix32(tri) & mask;
    for (;;) {
        if (!L->tb[slot].used) {
            if (!create) return NULL;
            L->tb[slot].used = 1; L->tb[slot].tri = tri;
            L->tb[slot].slots = NULL; L->tb[slot].cap = L->tb[slot].cnt = 0;
            L->tbcnt++;
            return &L->tb[slot];
        }
        if (L->tb[slot].tri == tri) return &L->tb[slot];
        slot = (slot + 1) & mask;
    }
}
/* ---- posting hash-set ops (O(1) add / has / del). Empty slot = PSET_EMPTY
 * (UINT64_MAX, never a valid doc id/offset), so doc id 0 is storable. ---- */
#define PSET_EMPTY UINT64_MAX
static int lneeds_reloc(uint32_t natural, uint32_t empty, uint32_t current);  /* defined below */
static void pset_fill_empty(uint64_t *a, uint32_t n) { for (uint32_t i = 0; i < n; i++) a[i] = PSET_EMPTY; }
static void pset_grow(ReTrigramLive *L, LTri *b) {
    uint32_t ncap = b->cap ? b->cap * 2 : 8, mask = ncap - 1;
    uint64_t *ns = (uint64_t *)malloc((size_t)ncap * sizeof *ns);
    if (!ns) { L->broken = 1; return; }
    pset_fill_empty(ns, ncap);
    for (uint32_t i = 0; i < b->cap; i++) if (b->slots[i] != PSET_EMPTY) {
        uint32_t s = mix64h(b->slots[i]) & mask;
        while (ns[s] != PSET_EMPTY) s = (s + 1) & mask;
        ns[s] = b->slots[i];
    }
    free(b->slots); b->slots = ns; b->cap = ncap;
}
static void pset_add(ReTrigramLive *L, LTri *b, uint64_t doc) {
    if (b->cap == 0 || (uint64_t)(b->cnt + 1) * 10 >= (uint64_t)b->cap * 7) { pset_grow(L, b); if (L->broken) return; }
    uint32_t mask = b->cap - 1, s = mix64h(doc) & mask;
    while (b->slots[s] != PSET_EMPTY) { if (b->slots[s] == doc) return; s = (s + 1) & mask; }
    b->slots[s] = doc; b->cnt++;
}
static int pset_has(const LTri *b, uint64_t doc) {
    if (b->cap == 0) return 0;
    uint32_t mask = b->cap - 1, s = mix64h(doc) & mask;
    while (b->slots[s] != PSET_EMPTY) { if (b->slots[s] == doc) return 1; s = (s + 1) & mask; }
    return 0;
}
static void pset_del(LTri *b, uint64_t doc) {
    if (b->cap == 0) return;
    uint32_t mask = b->cap - 1, s = mix64h(doc) & mask;
    for (;;) { if (b->slots[s] == PSET_EMPTY) return; if (b->slots[s] == doc) break; s = (s + 1) & mask; }
    b->slots[s] = PSET_EMPTY; b->cnt--;
    uint32_t removed = s, t = (s + 1) & mask;              /* backward-shift */
    while (b->slots[t] != PSET_EMPTY) {
        uint32_t nat = mix64h(b->slots[t]) & mask;
        if (lneeds_reloc(nat, removed, t)) { b->slots[removed] = b->slots[t]; b->slots[t] = PSET_EMPTY; removed = t; }
        t = (t + 1) & mask;
    }
}

/* ---- doc bucket map (supports delete via backward-shift) ---- */
static int lneeds_reloc(uint32_t natural, uint32_t empty, uint32_t current) {
    if (natural <= current) return natural <= empty && empty < current;
    return natural <= empty || empty < current;
}
static void ldoc_grow(ReTrigramLive *L) {
    uint32_t ncap = L->dbcap * 2, mask = ncap - 1;
    LDoc *nd = (LDoc *)calloc(ncap, sizeof *nd);
    if (!nd) { L->broken = 1; return; }
    for (uint32_t i = 0; i < L->dbcap; i++) if (L->db[i].used) {
        uint32_t s = mix64h(L->db[i].doc) & mask;
        while (nd[s].used) s = (s + 1) & mask;
        nd[s] = L->db[i];
    }
    free(L->db); L->db = nd; L->dbcap = ncap;
}
static LDoc *ldoc_find(const ReTrigramLive *L, uint64_t doc) {
    uint32_t mask = L->dbcap - 1, slot = mix64h(doc) & mask;
    for (;;) {
        if (!L->db[slot].used) return NULL;
        if (L->db[slot].doc == doc) return (LDoc *)&L->db[slot];
        slot = (slot + 1) & mask;
    }
}
static LDoc *ldoc_insert(ReTrigramLive *L, uint64_t doc) {
    if ((uint64_t)(L->dbcnt + 1) * 10 >= (uint64_t)L->dbcap * 7) ldoc_grow(L);
    uint32_t mask = L->dbcap - 1, slot = mix64h(doc) & mask;
    while (L->db[slot].used) { if (L->db[slot].doc == doc) return &L->db[slot]; slot = (slot + 1) & mask; }
    L->db[slot].used = 1; L->db[slot].doc = doc; L->db[slot].tris = NULL; L->db[slot].ntri = 0;
    L->dbcnt++;
    return &L->db[slot];
}
static void ldoc_delete(ReTrigramLive *L, uint64_t doc) {
    uint32_t mask = L->dbcap - 1, slot = mix64h(doc) & mask;
    for (;;) {
        if (!L->db[slot].used) return;
        if (L->db[slot].doc == doc) break;
        slot = (slot + 1) & mask;
    }
    L->db[slot].used = 0; L->db[slot].tris = NULL; L->db[slot].ntri = 0; L->dbcnt--;
    uint32_t removed = slot, s = (slot + 1) & mask;
    while (L->db[s].used) {
        uint32_t nat = mix64h(L->db[s].doc) & mask;
        if (lneeds_reloc(nat, removed, s)) { L->db[removed] = L->db[s]; L->db[s].used = 0; removed = s; }
        s = (s + 1) & mask;
    }
}

void re_trigram_live_remove(ReTrigramLive *L, uint64_t doc) {
    if (!L) return;
    LDoc *d = ldoc_find(L, doc);
    if (!d) return;
    for (uint32_t i = 0; i < d->ntri; i++) {
        LTri *b = ltri_get(L, d->tris[i], 0);
        if (b) pset_del(b, doc);
    }
    free(d->tris);
    ldoc_delete(L, doc);
}

void re_trigram_live_set(ReTrigramLive *L, uint64_t doc,
                         const char *const *fields, const size_t *lens, int nf) {
    if (!L || L->broken) return;

    /* new sorted+deduped trigram set for this doc */
    size_t nwin = 0;
    for (int f = 0; f < nf; f++) if (lens[f] >= 3) nwin += lens[f] - 2;
    uint32_t nn = 0; uint32_t *nt = NULL;
    if (nwin) {
        nt = (uint32_t *)malloc(nwin * sizeof *nt);
        if (!nt) { L->broken = 1; return; }
        size_t k = 0;
        for (int f = 0; f < nf; f++) {
            const uint8_t *b = (const uint8_t *)fields[f]; size_t Lf = lens[f];
            for (size_t i = 0; i + 3 <= Lf; i++) nt[k++] = pack_tri(b, (int)i);
        }
        radix_u32(nt, k);
        for (size_t i = 0; i < k; i++) if (i == 0 || nt[i] != nt[i - 1]) nt[nn++] = nt[i];
    }

    /* merge-diff against the doc's previous set (both sorted): old-only => remove
     * from that posting, new-only => append, in-both => leave untouched. This is
     * what keeps create/observation writes to pure O(1) appends. */
    LDoc *d = ldoc_find(L, doc);
    uint32_t *ot = d ? d->tris : NULL, on = d ? d->ntri : 0;
    uint32_t i = 0, j = 0;
    while (i < on || j < nn) {
        if (j >= nn || (i < on && ot[i] < nt[j])) {
            LTri *b = ltri_get(L, ot[i], 0); if (b) pset_del(b, doc); i++;
        } else if (i >= on || nt[j] < ot[i]) {
            LTri *b = ltri_get(L, nt[j], 1); if (b) pset_add(L, b, doc); else L->broken = 1; j++;
        } else { i++; j++; }
    }

    if (!d) d = ldoc_insert(L, doc);
    if (!d) { free(nt); L->broken = 1; return; }
    free(d->tris); d->tris = nt; d->ntri = nn;
}

uint32_t re_trigram_live_ndocs   (const ReTrigramLive *L) { return L ? L->dbcnt : 0; }
uint32_t re_trigram_live_distinct(const ReTrigramLive *L) { return L ? L->tbcnt : 0; }

/* Emit every 3-byte window of s[0..len) (raw order; duplicates included). */
void re_trigram_foreach(const char *s, size_t len,
                        void (*cb)(void *ctx, uint32_t tri), void *ctx) {
    const uint8_t *b = (const uint8_t *)s;
    for (size_t i = 0; i + 3 <= len; i++) cb(ctx, pack_tri(b, (int)i));
}

size_t re_trigram_live_bytes(const ReTrigramLive *L) {
    if (!L) return 0;
    size_t b = sizeof *L
             + (size_t)L->tbcap * sizeof(LTri)
             + (size_t)L->dbcap * sizeof(LDoc);
    for (uint32_t i = 0; i < L->tbcap; i++) if (L->tb[i].used) b += (size_t)L->tb[i].cap * sizeof(uint64_t);
    for (uint32_t i = 0; i < L->dbcap; i++) if (L->db[i].used) b += (size_t)L->db[i].ntri * sizeof(uint32_t);
    return b;
}

/* ---- eval over the live index (u64 candidate lists) ---- */
typedef struct { uint64_t *ids; uint32_t n; int all; } CL64;
static CL64 cl64_all(void)   { CL64 c; c.ids = NULL; c.n = 0; c.all = 1; return c; }
static CL64 cl64_empty(void) { CL64 c; c.ids = NULL; c.n = 0; c.all = 0; return c; }
static void cl64_free(CL64 *c) { free(c->ids); c->ids = NULL; c->n = 0; }
static CL64 cl64_copy(const uint64_t *v, uint32_t n) {
    CL64 c; c.all = 0; c.n = n;
    c.ids = n ? (uint64_t *)malloc((size_t)n * sizeof *c.ids) : NULL;
    if (n && !c.ids) return cl64_all();
    if (n) memcpy(c.ids, v, (size_t)n * sizeof *c.ids);
    return c;
}
/* Keep only acc elements present in the sorted list b[0..bn), by binary search;
 * acc stays sorted. O(|acc|·log|b|) — so intersecting a small survivor set
 * against a near-universal posting costs |acc| probes, not |b|. */
static void cl64_probe(CL64 *acc, const uint64_t *b, uint32_t bn) {
    uint32_t w = 0;
    for (uint32_t i = 0; i < acc->n; i++) {
        uint64_t val = acc->ids[i];
        uint32_t lo = 0, hi = bn;
        while (lo < hi) { uint32_t m = (lo + hi) >> 1; if (b[m] < val) lo = m + 1; else hi = m; }
        if (lo < bn && b[lo] == val) acc->ids[w++] = val;
    }
    acc->n = w;
}
/* keep only acc members present in the posting hash set b (O(|acc|) probes). */
static void cl64_probe_pset(CL64 *acc, const LTri *b) {
    uint32_t w = 0;
    for (uint32_t i = 0; i < acc->n; i++) if (pset_has(b, acc->ids[i])) acc->ids[w++] = acc->ids[i];
    acc->n = w;
}
/* materialize a posting hash set into a fresh (unsorted) candidate array. */
static CL64 pset_materialize(const LTri *b) {
    CL64 c; c.all = 0; c.n = 0;
    c.ids = b->cnt ? (uint64_t *)malloc((size_t)b->cnt * sizeof *c.ids) : NULL;
    if (b->cnt && !c.ids) return cl64_all();
    for (uint32_t i = 0; i < b->cap; i++) if (b->slots[i] != PSET_EMPTY) c.ids[c.n++] = b->slots[i];
    return c;
}
static CL64 eval_live(const TQ *q, const ReTrigramLive *L) {
    switch (q->kind) {
        case TQ_ALL:  return cl64_all();
        case TQ_NONE: return cl64_empty();
        case TQ_TRI: {
            LTri *b = ltri_get((ReTrigramLive *)L, q->tri, 0);
            if (!b || b->cnt == 0) return cl64_empty();
            return pset_materialize(b);
        }
        case TQ_AND: {
            /* Operands: TRI leaves are BORROWED posting hash sets (no copy — a
             * near-universal trigram is never materialized); OR children are
             * evaluated to owned, sorted arrays. Intersect smallest-first: seed
             * from the smallest, probe survivors into the rest (pset_has for a
             * posting, binary search for an OR array) — O(|smallest|·k). */
            struct av { const LTri *post; uint64_t *arr; uint32_t n; int own; } *v =
                (struct av *)malloc((size_t)q->nk * sizeof *v);
            if (!v) return cl64_all();
            int nv = 0, empty = 0;
            for (int i = 0; i < q->nk; i++) {
                const TQ *ch = q->ks[i];
                if (ch->kind == TQ_TRI) {
                    LTri *b = ltri_get((ReTrigramLive *)L, ch->tri, 0);
                    if (!b || b->cnt == 0) { empty = 1; break; }
                    v[nv].post = b; v[nv].arr = NULL; v[nv].n = b->cnt; v[nv].own = 0; nv++;
                } else {
                    CL64 c = eval_live(ch, L);
                    if (c.all) { cl64_free(&c); continue; }
                    if (c.n == 0) { cl64_free(&c); empty = 1; break; }
                    if (c.n > 1) radix_u64(c.ids, c.n);  /* sorted for probe */
                    v[nv].post = NULL; v[nv].arr = c.ids; v[nv].n = c.n; v[nv].own = 1; nv++;
                }
            }
            if (empty || nv == 0) {
                for (int i = 0; i < nv; i++) if (v[i].own) free(v[i].arr);
                free(v);
                return empty ? cl64_empty() : cl64_all();
            }
            for (int i = 1; i < nv; i++) {              /* insertion sort by size */
                struct av key = v[i]; int j = i - 1;
                while (j >= 0 && v[j].n > key.n) { v[j + 1] = v[j]; j--; }
                v[j + 1] = key;
            }
            CL64 acc = v[0].post ? pset_materialize(v[0].post) : cl64_copy(v[0].arr, v[0].n);
            for (int k = 1; k < nv && acc.n > 0; k++) {
                if (v[k].post) cl64_probe_pset(&acc, v[k].post);
                else           cl64_probe(&acc, v[k].arr, v[k].n);
            }
            for (int i = 0; i < nv; i++) if (v[i].own) free(v[i].arr);
            free(v);
            return acc;
        }
        case TQ_OR: {
            uint32_t cap = 0, cnt = 0;
            CL64 *parts = q->nk ? (CL64 *)malloc((size_t)q->nk * sizeof *parts) : NULL;
            if (q->nk && !parts) return cl64_all();
            for (int i = 0; i < q->nk; i++) {
                CL64 c = eval_live(q->ks[i], L);
                if (c.all) { for (uint32_t z = 0; z < cnt; z++) cl64_free(&parts[z]); free(parts); cl64_free(&c); return cl64_all(); }
                parts[cnt++] = c; cap += c.n;
            }
            CL64 merged; merged.all = 0; merged.n = 0;
            merged.ids = cap ? (uint64_t *)malloc((size_t)cap * sizeof *merged.ids) : NULL;
            if (cap && !merged.ids) { for (uint32_t z = 0; z < cnt; z++) cl64_free(&parts[z]); free(parts); return cl64_all(); }
            for (uint32_t z = 0; z < cnt; z++) for (uint32_t i = 0; i < parts[z].n; i++) merged.ids[merged.n++] = parts[z].ids[i];
            for (uint32_t z = 0; z < cnt; z++) cl64_free(&parts[z]);
            free(parts);
            if (merged.n > 1) {
                radix_u64(merged.ids, merged.n);
                uint32_t w = 1;
                for (uint32_t i = 1; i < merged.n; i++) if (merged.ids[i] != merged.ids[w - 1]) merged.ids[w++] = merged.ids[i];
                merged.n = w;
            }
            return merged;
        }
    }
    return cl64_all();
}

ReCandidates64 re_trigram_live_eval(const ReTrigramQuery *q, const ReTrigramLive *L) {
    ReCandidates64 out; out.ids = NULL; out.n = 0; out.all = 1;
    if (!q || !L || L->broken) return out;   /* broken => full scan (sound) */
    CL64 c = eval_live(q->root, L);
    if (!c.all && c.n > 1) radix_u64(c.ids, c.n);  /* ascending candidate contract */
    out.all = c.all; out.ids = c.ids; out.n = c.n;
    return out;
}

/* ---- eval over a caller-provided posting store (array-only) ---------------- */

static CL64 eval_prov(const TQ *q, const ReTriProvider *p) {
    switch (q->kind) {
        case TQ_ALL:  return cl64_all();
        case TQ_NONE: return cl64_empty();
        case TQ_TRI: {
            uint32_t n = 0;
            uint64_t *ids = p->leaf(p->ctx, q->tri, &n);
            if (!ids || n == 0) { free(ids); return cl64_empty(); }
            { CL64 c; c.all = 0; c.ids = ids; c.n = n; return c; }  /* ascending by contract */
        }
        case TQ_AND: {
            CL64 *v = (CL64 *)malloc((size_t)(q->nk ? q->nk : 1) * sizeof *v);
            if (!v) return cl64_all();
            int nv = 0, empty = 0;
            for (int i = 0; i < q->nk; i++) {
                CL64 c = eval_prov(q->ks[i], p);
                if (c.all) { free(c.ids); continue; }         /* no constraint */
                if (c.n == 0) { free(c.ids); empty = 1; break; }
                v[nv++] = c;
            }
            if (empty) { for (int i = 0; i < nv; i++) cl64_free(&v[i]); free(v); return cl64_empty(); }
            if (nv == 0) { free(v); return cl64_all(); }
            /* seed from the smallest survivor set; probe it through the rest
             * (sorted => binary-search probes; the recorded smallest-first policy) */
            uint32_t mi = 0;
            for (int i = 1; i < nv; i++) if (v[i].n < v[mi].n) mi = (uint32_t)i;
            CL64 acc = v[mi]; v[mi].ids = NULL; v[mi].n = 0;
            for (int i = 0; i < nv; i++)
                if (i != (int)mi) { cl64_probe(&acc, v[i].ids, v[i].n); cl64_free(&v[i]); }
            free(v);
            return acc;
        }
        case TQ_OR: {
            CL64 *v = (CL64 *)malloc((size_t)(q->nk ? q->nk : 1) * sizeof *v);
            if (!v) return cl64_all();
            int nv = 0, all = 0;
            uint64_t total = 0;
            for (int i = 0; i < q->nk; i++) {
                CL64 c = eval_prov(q->ks[i], p);
                if (c.all) { all = 1; free(c.ids); break; }   /* union with everything = everything */
                total += c.n;
                v[nv++] = c;
            }
            if (all) { for (int i = 0; i < nv; i++) cl64_free(&v[i]); free(v); return cl64_all(); }
            uint64_t *m = total ? (uint64_t *)malloc((size_t)total * sizeof *m) : NULL;
            if (total && !m) { for (int i = 0; i < nv; i++) cl64_free(&v[i]); free(v); return cl64_all(); }
            uint64_t w = 0;
            for (int i = 0; i < nv; i++) {
                if (v[i].n) memcpy(m + w, v[i].ids, (size_t)v[i].n * sizeof *m);
                w += v[i].n;
                cl64_free(&v[i]);
            }
            free(v);
            if (w > 1) radix_u64(m, w);
            uint64_t u = 0;
            for (uint64_t i = 0; i < w; i++) if (i == 0 || m[i] != m[i - 1]) m[u++] = m[i];
            CL64 c; c.all = 0; c.ids = m; c.n = (uint32_t)u;
            return c;
        }
        default: return cl64_all();
    }
}

ReCandidates64 re_trigram_eval_provider(const ReTrigramQuery *q, const ReTriProvider *p) {
    ReCandidates64 out; out.ids = NULL; out.n = 0; out.all = 1;
    if (!q || !p) return out;
    CL64 c = eval_prov(q->root, p);
    out.all = c.all; out.ids = c.ids; out.n = c.n;
    return out;
}

void re_candidates64_free(ReCandidates64 *c) {
    if (!c) return;
    free(c->ids); c->ids = NULL; c->n = 0;
}
