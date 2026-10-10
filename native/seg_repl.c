/*
 * seg_repl.c — the pairwise anti-entropy ROUND (docs/shard-seam-design-note.md
 * §8/§5, build step 5a-ii). Given two stores holding the same logical dataset
 * (clones), one round reconciles:
 *
 *   edges  — canonical symbols ([lo][hi][relhash][dlo][mtime]); a symbol the
 *            peer lacks is PULLED (the edge is (re)created there) unless the
 *            peer's chain remove-watermark covers its mtime, in which case
 *            the peer deleted it after that version and the deletion is
 *            PUSHED instead (never resurrected — note §5/§9).
 *   vstate — node rows; whole-row LWW by (mtime, obs_mtime, content bytes,
 *            then psi) applied via g4_vstate_apply (cross-store string
 *            translation). Rows for nodes the peer cannot read are SKIPPED
 *            (entity replication + tombstones are step 5b), as are gen-only
 *            differences (directory reconciliation, 5b).
 *
 * The round runs inside the caller's txns (one per store); sets are extracted
 * before any apply. Symbols are canonical-both-halves: sets are deduped
 * before encoding.
 *
 * Clone caveat, stated: node ids are compared directly, so both stores must
 * share the clone lineage (node-id spaces coincide). The shard era resolves
 * node ids through the directory first — that translation lands with the
 * channel (5b).
 */
#include "segstore.h"
#include "riblt.h"

#include <stdlib.h>
#include <string.h>

/* ---- symbol sets ---- */

typedef struct { u8 *v; u32 n, cap; } rsymset_t;

static void rcollect(void *ctx, const u8 *s) {
    rsymset_t *x = (rsymset_t *)ctx;
    if (x->n == x->cap) {
        x->cap = x->cap ? x->cap * 2 : 1024;
        x->v = (u8 *)realloc(x->v, (size_t)x->cap * RIBLT_WIDTH);
        if (!x->v) abort();
    }
    memcpy(x->v + (size_t)x->n * RIBLT_WIDTH, s, RIBLT_WIDTH);
    x->n++;
}

static int rsym_cmp(const void *a, const void *b) { return memcmp(a, b, RIBLT_WIDTH); }

static void rsym_uniq(rsymset_t *s) {
    u32 w = 0;
    for (u32 i = 0; i < s->n; i++) {
        if (w == 0 || memcmp(s->v + (size_t)(w - 1) * RIBLT_WIDTH,
                             s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH) != 0)
            memcpy(s->v + (size_t)w++ * RIBLT_WIDTH, s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH);
    }
    s->n = w;
}

/* ---- relhash -> local sid map (deletes recreate the type string) ---- */

typedef struct { u64 *h; u32 *sid; u32 cap, cnt; } rmap_t;

static void rmap_grow(rmap_t *m, u32 nc) {
    u64 *nh = (u64 *)calloc(nc, 8);
    u32 *ns = (u32 *)calloc(nc, 4);
    if (!nh || !ns) abort();
    for (u32 i = 0; i < m->cap; i++) {
        if (!m->h[i]) continue;
        u32 s = (u32)m->h[i] & (nc - 1);
        while (nh[s]) s = (s + 1) & (nc - 1);
        nh[s] = m->h[i];
        ns[s] = m->sid[i];
    }
    free(m->h); free(m->sid);
    m->h = nh; m->sid = ns; m->cap = nc;
}

static void rmap_put(rmap_t *m, u64 h, u32 sid) {
    if (!h) return;
    if (!m->cap) rmap_grow(m, 1024);
    if ((u64)(m->cnt + 1) * 10 >= (u64)m->cap * 7) rmap_grow(m, m->cap * 2);
    u32 s = (u32)h & (m->cap - 1);
    while (m->h[s] && m->h[s] != h) s = (s + 1) & (m->cap - 1);
    if (!m->h[s]) { m->h[s] = h; m->sid[s] = sid; m->cnt++; }
    /* first-wins on the (vanishing) hash-collision case: the symbol identity
     * IS the hash, so any holder of that hash resolves to a real string */
}

static u32 rmap_get(const rmap_t *m, u64 h) {
    if (!m->cap || !h) return 0;
    u32 s = (u32)h & (m->cap - 1);
    while (m->h[s]) {
        if (m->h[s] == h) return m->sid[s];
        s = (s + 1) & (m->cap - 1);
    }
    return 0;
}

static rmap_t *rmap_build(graph4_t *g) {
    rmap_t *m = (rmap_t *)calloc(1, sizeof *m);
    if (!m) abort();
    u32 nent = g4_entity_count(g);
    u32 *ids = (u32 *)malloc((size_t)(nent ? nent : 1) * 4);
    if (!ids) abort();
    u32 n = g4_list_entities(g, ids, nent);
    for (u32 i = 0; i < n; i++) {
        u32 ec = g4_edge_count(g, ids[i]);
        if (!ec) continue;
        g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
        if (!es) abort();
        g4_edges(g, ids[i], es, ec);
        for (u32 k = 0; k < ec; k++) rmap_put(m, g4_relhash(g, es[k].rel_sid), es[k].rel_sid);
        free(es);
    }
    free(ids);
    return m;
}

static void rmap_free(rmap_t *m) {
    if (!m) return;
    free(m->h); free(m->sid); free(m);
}

/* ---- helpers ---- */

static u32 sym_u32(const u8 *p) { return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24); }
static u64 sym_u64(const u8 *p) {
    u64 v = 0;
    for (int k = 7; k >= 0; k--) v = (v << 8) | p[k];
    return v;
}

/* does `from`'s FORWARD chain already hold (to, rt)? (probe after create=0) */
static int edge_present(graph4_t *g, u32 from, u32 to, const u8 *rt, u16 rl) {
    u32 ec = g4_edge_count(g, from);
    if (!ec) return 0;
    g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
    if (!es) return 0;
    g4_edges(g, from, es, ec);
    int found = 0;
    for (u32 k = 0; k < ec && !found; k++) {
        if (es[k].target_eid != to || es[k].direction != G4_DIR_FORWARD) continue;
        u16 l = 0;
        const u8 *b = g4_str(g, es[k].rel_sid, &l);
        if (b && l == rl && memcmp(b, rt, rl) == 0) found = 1;
    }
    free(es);
    return found;
}

/* vstate row order, public fields only (store-independent, deterministic) */
static int row_cmp(graph4_t *ga, const g4_entity_t *a, graph4_t *gb, const g4_entity_t *b) {
    if (a->mtime != b->mtime) return a->mtime < b->mtime ? -1 : 1;
    if (a->obs_mtime != b->obs_mtime) return a->obs_mtime < b->obs_mtime ? -1 : 1;
    if (a->obs_count != b->obs_count) return a->obs_count < b->obs_count ? -1 : 1;
    {
        u16 la = 0, lb = 0;
        const u8 *ta = g4_str(ga, a->type_sid, &la);
        const u8 *tb = g4_str(gb, b->type_sid, &lb);
        u16 m = la < lb ? la : lb;
        int c = m ? memcmp(ta, tb, m) : 0;
        if (c) return c < 0 ? -1 : 1;
        if (la != lb) return la < lb ? -1 : 1;
    }
    for (int k = 0; k < 2; k++) {
        u32 sa = (k == 0) ? a->obs0_sid : a->obs1_sid;
        u32 sb = (k == 0) ? b->obs0_sid : b->obs1_sid;
        int pa = sa && (u8)k < a->obs_count;
        int pb = sb && (u8)k < b->obs_count;
        if (!pa && !pb) continue;
        u16 la = 0, lb = 0;
        const u8 *oa = pa ? g4_str(ga, sa, &la) : (const u8 *)"";
        const u8 *ob = pb ? g4_str(gb, sb, &lb) : (const u8 *)"";
        u16 m = la < lb ? la : lb;
        int c = m ? memcmp(oa, ob, m) : 0;
        if (c) return c < 0 ? -1 : 1;
        if (la != lb) return la < lb ? -1 : 1;
    }
    if (a->psi != b->psi) return a->psi < b->psi ? -1 : 1;
    return 0;
}

/* ---- the round ---- */

int g4_repl_round(graph4_t *a, graph4_t *b, g4_repl_stats_t *st) {
    g4_repl_stats_t stats;
    memset(&stats, 0, sizeof stats);
    int ok = 1;

    /* 1. extract (deduped canonical sets) — before any apply */
    rsymset_t adjA = {0}, adjB = {0}, vsA = {0}, vsB = {0};
    g4_adj_symbols(a, rcollect, &adjA);
    g4_adj_symbols(b, rcollect, &adjB);
    g4_vstate_symbols(a, rcollect, &vsA);
    g4_vstate_symbols(b, rcollect, &vsB);
    qsort(adjA.v, adjA.n, RIBLT_WIDTH, rsym_cmp); rsym_uniq(&adjA);
    qsort(adjB.v, adjB.n, RIBLT_WIDTH, rsym_cmp); rsym_uniq(&adjB);
    qsort(vsA.v, vsA.n, RIBLT_WIDTH, rsym_cmp); rsym_uniq(&vsA);
    qsort(vsB.v, vsB.n, RIBLT_WIDTH, rsym_cmp); rsym_uniq(&vsB);

    /* 2. reconcile: decoder local = B, remote = A (one decode, both diffs) */
    rsymset_t *famA[2] = { &adjA, &vsA };
    rsymset_t *famB[2] = { &adjB, &vsB };
    u8 *Aonly[2] = { NULL, NULL };
    u8 *Bonly[2] = { NULL, NULL };
    u32 nAonly[2] = { 0, 0 }, nBonly[2] = { 0, 0 };
    for (int f = 0; f < 2 && ok; f++) {
        riblt_enc ea, eb;
        riblt_dec d;
        if (riblt_enc_init(&ea, "kbrepl", 6, famA[f]->v, famA[f]->n) != 0) { ok = 0; break; }
        if (riblt_enc_init(&eb, "kbrepl", 6, famB[f]->v, famB[f]->n) != 0) {
            riblt_enc_free(&ea); ok = 0; break;
        }
        if (riblt_dec_init(&d, &eb, "kbrepl", 6) != 0) {
            riblt_enc_free(&ea); riblt_enc_free(&eb); ok = 0; break;
        }
        int done = 0;
        for (u32 i = 0; i < (1u << 22) && !done; i++)
            done = riblt_dec_feed(&d, i, riblt_enc_cell(&ea, i)) == 1;
        if (!done) ok = 0;
        else {
            nAonly[f] = d.remote_only_n;
            nBonly[f] = d.local_only_n;
            if (nAonly[f]) {
                Aonly[f] = (u8 *)malloc((size_t)nAonly[f] * RIBLT_WIDTH);
                if (!Aonly[f]) abort();
                memcpy(Aonly[f], d.remote_only, (size_t)nAonly[f] * RIBLT_WIDTH);
            }
            if (nBonly[f]) {
                Bonly[f] = (u8 *)malloc((size_t)nBonly[f] * RIBLT_WIDTH);
                if (!Bonly[f]) abort();
                memcpy(Bonly[f], d.local_only, (size_t)nBonly[f] * RIBLT_WIDTH);
            }
        }
        riblt_dec_free(&d); riblt_enc_free(&ea); riblt_enc_free(&eb);
    }
    if (!ok) goto cleanup;

    /* 3. apply — edges. side 0: A-only (A has, B not); side 1: B-only. */
    {
        rmap_t *mapA = NULL, *mapB = NULL;
        for (int side = 0; side < 2; side++) {
            const u8 *list = (side == 0) ? Aonly[0] : Bonly[0];
            u32 n = (side == 0) ? nAonly[0] : nBonly[0];
            if (!n) continue;
            graph4_t *have = (side == 0) ? a : b;   /* holds the edge  */
            graph4_t *lack = (side == 0) ? b : a;   /* missing it      */
            rmap_t **hm = (side == 0) ? &mapA : &mapB;
            if (!*hm) *hm = rmap_build(have);
            for (u32 i = 0; i < n; i++) {
                const u8 *s = list + (size_t)i * RIBLT_WIDTH;
                u32 lo = sym_u32(s);
                u32 hi = sym_u32(s + 4);
                u64 rh = sym_u64(s + 8);
                u8 dlo = s[16];
                u64 mt = sym_u64(s + 17);
                u32 from = (dlo == G4_DIR_FORWARD) ? lo : hi;
                u32 to   = (dlo == G4_DIR_FORWARD) ? hi : lo;
                /* deleted there? either endpoint's watermark covers it */
                u64 wm = g4_chain_wm(lack, lo);
                if (hi != lo) {
                    u64 wm2 = g4_chain_wm(lack, hi);
                    if (wm2 > wm) wm = wm2;
                }
                u32 sid = rmap_get(*hm, rh);
                if (!sid) { stats.edge_skipped++; continue; }
                u16 rl = 0;
                const u8 *rb = g4_str(have, sid, &rl);
                if (!rb) { stats.edge_skipped++; continue; }
                if (wm >= mt) {
                    /* the deletion happened there: push it back — never re-add */
                    if (g4_delete_relation(have, from, to, rb, rl)) {
                        if (side == 0) stats.edges_deleted_from_a++;
                        else stats.edges_deleted_from_b++;
                    } else stats.edge_skipped++;
                    continue;
                }
                int r = g4_create_relation(lack, from, to, rb, rl, mt);
                if (r) {
                    if (side == 0) stats.edges_pulled_b++;
                    else stats.edges_pulled_a++;
                } else if (edge_present(lack, from, to, rb, rl)) {
                    stats.edges_dup++;
                } else {
                    stats.edge_skipped++;
                }
            }
        }
        rmap_free(mapA); rmap_free(mapB);
    }

    /* 4. apply — vstate: whole-row LWW for nodes both stores can read */
    for (int side = 0; side < 2; side++) {
        const u8 *list = (side == 0) ? Aonly[1] : Bonly[1];
        u32 n = (side == 0) ? nAonly[1] : nBonly[1];
        for (u32 i = 0; i < n; i++) {
            const u8 *s = list + (size_t)i * RIBLT_WIDTH;
            u32 node = sym_u32(s);
            g4_entity_t ea, eb;
            int ha = g4_read_entity(a, node, &ea);
            int hb = g4_read_entity(b, node, &eb);
            if (!ha || !hb) { stats.vstate_skipped++; continue; }  /* 5b */
            int c = row_cmp(a, &ea, b, &eb);
            if (c == 0) { stats.vstate_skipped++; continue; }      /* gen-only, 5b */
            graph4_t *wi = (c > 0) ? a : b;
            graph4_t *lo_ = (c > 0) ? b : a;
            g4_entity_t *we = (c > 0) ? &ea : &eb;
            if (g4_vstate_apply(lo_, node, wi, we)) {
                if (lo_ == b) stats.vstate_applied_b++;
                else stats.vstate_applied_a++;
            } else stats.vstate_skipped++;
        }
    }

cleanup:
    free(adjA.v); free(adjB.v); free(vsA.v); free(vsB.v);
    free(Aonly[0]); free(Aonly[1]); free(Bonly[0]); free(Bonly[1]);
    if (st) *st = stats;
    return ok;
}
