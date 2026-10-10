/*
 * seg_repl.c — the pairwise anti-entropy ROUND (docs/shard-seam-design-note.md
 * §8/§5, build step 5a-ii; unified wire form 5b-i). One algorithm, two
 * transports: the round driver owns the LOCAL store and one RIBLT decode per
 * family; everything it needs from the other side goes through repl_peer_t
 * (segstore.h). g4_repl_round(a, b) is the same round with an in-process
 * adapter over `b`; the daemon channel is the wire adapter.
 *
 *   edges  — canonical symbols ([lo][hi][relhash][dlo][mtime]); a symbol the
 *            peer lacks is PULLED (created there via edge_pull; the peer
 *            re-checks ITS remove-watermark at apply time and answers
 *            "covered" instead when it deleted the edge after that version —
 *            then the driver deletes its own copy: never resurrected). A
 *            symbol WE lack is created locally (edge_pull's mirror image
 *            driven from here) unless OUR watermark covers its mtime, in
 *            which case the deletion is PUSHED to the holder (edge_del).
 *   vstate — node rows; whole-row LWW by (mtime, obs_mtime, obs_count, type
 *            bytes, obs bytes, psi) via the row blob codec (g4_vrow_*). The
 *            winner is applied to the loser; the loser side re-checks the
 *            order at apply time (vrow_set / the local adapter), so a stale
 *            push can never clobber a newer row. One-sided nodes MIRROR:
 *            the missing record is created at the exact node id under the
 *            name (gen, node) rule (g4_mirror_apply) — a beaten local
 *            binding's record retires, and a local tombstone or rival
 *            binding pushes its name row instead (g4_mirror_name), so
 *            deletions propagate as name-row wins over resurrected
 *            mirrors. Binding-generation differences on shared rows
 *            reconcile through the same name rule (5b-ii).
 *
 * Snapshot model: each side's symbol set is captured once per family (the
 * driver's just before that family's exchange; the peer's at begin), so the
 * decode is over two consistent snapshots. Concurrent edits on either side
 * between snapshot and apply surface as next-round churn — the standard
 * anti-entropy contract; the no-resurrection invariant is enforced at apply
 * time by the watermark checks, which run at the store that owns the
 * watermark, single-threaded with its writes.
 */
#include "segstore.h"
#include "riblt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define REPL_CELL_BATCH 256u       /* cells per fetch (256*44B ≈ 11 KiB) */
#define REPL_ROW_CAP    G4_REPL_ROW_CAP   /* row blob cap (type + 2 obs + overhead) */
#define REPL_STR_CAP    G4_REPL_STR_CAP   /* reltype string cap */
#define REPL_INDEX_CAP  (1u << 22) /* cell-index bound (parity with 5a-ii) */

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

/* ---- relhash -> local sid map (relname fetches at a local store) ---- */

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
    if (n > nent) n = nent;              /* true-total return: clamp to the buffer */
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
static int u32cmp(const void *a, const void *b) {
    u32 x = *(const u32 *)a, y = *(const u32 *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

/* does `g`'s FORWARD chain from `from` already hold (to, rt)? (create=0 probe) */
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

/* ---- in-process peer adapter (backing g4_repl_round(a, b)) ---- */

typedef struct {
    graph4_t *g;            /* the peer store */
    riblt_enc enc[3];
    int active[3];
    rmap_t *rmap;           /* lazy relhash -> sid over g's edges */
} lp_t;

static int lp_begin(void *ctx, u8 family, u32 *nsym) {
    lp_t *lp = (lp_t *)ctx;
    if (family > 2) return 0;
    if (lp->active[family]) { riblt_enc_free(&lp->enc[family]); lp->active[family] = 0; }
    rsymset_t s = {0};
    if (family == 0) g4_adj_symbols(lp->g, rcollect, &s);
    else if (family == 1) g4_vstate_symbols(lp->g, rcollect, &s);
    else g4_ndir_symbols(lp->g, rcollect, &s);
    qsort(s.v, s.n, RIBLT_WIDTH, rsym_cmp);
    rsym_uniq(&s);
    if (nsym) *nsym = s.n;
    int r = riblt_enc_init(&lp->enc[family], "kbrepl", 6, s.v, s.n);
    free(s.v);
    if (r != 0) return 0;
    lp->active[family] = 1;
    return 1;
}

static int lp_cells(void *ctx, u8 family, u32 from_idx, u32 max, u8 *out, u32 *nout) {
    lp_t *lp = (lp_t *)ctx;
    if (family > 2 || !lp->active[family]) return 0;
    for (u32 i = 0; i < max; i++) {
        const riblt_cell *c = riblt_enc_cell(&lp->enc[family], from_idx + i);
        if (!c) return 0;
        riblt_cell_pack(out + (size_t)i * 44, c);
    }
    *nout = max;
    return 1;
}

static int lp_end(void *ctx, u8 family) {
    lp_t *lp = (lp_t *)ctx;
    if (family > 2) return 0;
    if (lp->active[family]) { riblt_enc_free(&lp->enc[family]); lp->active[family] = 0; }
    return 1;
}

static int lp_relname(void *ctx, u64 relhash, u8 *out, u16 *outlen) {
    lp_t *lp = (lp_t *)ctx;
    if (!lp->rmap) lp->rmap = rmap_build(lp->g);
    u32 sid = rmap_get(lp->rmap, relhash);
    if (!sid) return 0;
    u16 l = 0;
    const u8 *s = g4_str(lp->g, sid, &l);
    if (!s) return 0;
    memcpy(out, s, l);
    *outlen = l;
    return 1;
}

static int lp_vrow(void *ctx, u32 node, u8 *out, u32 *outlen) {
    lp_t *lp = (lp_t *)ctx;
    g4_entity_t e;
    if (!g4_read_entity(lp->g, node, &e)) return 0;
    u32 n = g4_vrow_pack(lp->g, &e, out, REPL_ROW_CAP);
    if (!n) return 0;
    *outlen = n;
    return 1;
}

static int lp_vrow_set(void *ctx, u32 node, const u8 *row, u32 rowlen, u8 *status) {
    lp_t *lp = (lp_t *)ctx;
    g4_entity_t cur;
    *status = 0;
    if (!g4_read_entity(lp->g, node, &cur)) return 1;      /* node gone: lost */
    if (g4_vrow_cmp(lp->g, row, rowlen, &cur) <= 0) return 1;
    if (g4_vrow_apply(lp->g, node, row, rowlen)) *status = 1;
    return 1;
}

static int lp_edge_pull(void *ctx, const u8 *sym, const u8 *rt, u16 rl, u8 *status) {
    lp_t *lp = (lp_t *)ctx;
    u32 lo = sym_u32(sym), hi = sym_u32(sym + 4);
    u8 dlo = sym[16];
    u64 mt = sym_u64(sym + 17);
    u32 from = (dlo == G4_DIR_FORWARD) ? lo : hi;
    u32 to   = (dlo == G4_DIR_FORWARD) ? hi : lo;
    u64 wm = g4_chain_wm(lp->g, lo);
    if (hi != lo) {
        u64 w2 = g4_chain_wm(lp->g, hi);
        if (w2 > wm) wm = w2;
    }
    if (wm >= mt) { *status = 0; return 1; }               /* covered: delete wins */
    int r = g4_create_relation(lp->g, from, to, rt, rl, mt);
    if (r) *status = 1;
    else if (edge_present(lp->g, from, to, rt, rl)) *status = 2;
    else *status = 3;
    return 1;
}

static int lp_edge_del(void *ctx, const u8 *sym, u8 *status) {
    lp_t *lp = (lp_t *)ctx;
    u32 lo = sym_u32(sym), hi = sym_u32(sym + 4);
    u8 dlo = sym[16];
    u64 rh = sym_u64(sym + 8);
    *status = (u8)(g4_edge_del_hashed(lp->g, lo, hi, dlo, rh) ? 1 : 0);
    return 1;
}

static int lp_name_get(void *ctx, const u8 *name, u16 nl, u32 *node, u32 *gen) {
    lp_t *lp = (lp_t *)ctx;
    return g4_name_get(lp->g, name, nl, node, gen);
}

static int lp_name_by_hash(void *ctx, u64 nh, u8 *name, u16 *nl, u32 *node, u32 *gen) {
    lp_t *lp = (lp_t *)ctx;
    return g4_ndir_find(lp->g, nh, name, nl, node, gen);
}

static int lp_name_set(void *ctx, const u8 *name, u16 nl, u32 node, u32 gen, u8 *status) {
    lp_t *lp = (lp_t *)ctx;
    *status = (u8)(g4_mirror_name(lp->g, name, nl, node, gen) ? 1 : 0);
    return 1;
}

static int lp_entity_get(void *ctx, u32 node, u8 *name, u16 *nl, u32 *gen,
                         u8 *blob, u32 *blen) {
    lp_t *lp = (lp_t *)ctx;
    g4_entity_t e;
    if (!g4_read_entity(lp->g, node, &e)) return 0;
    u16 l = 0;
    const u8 *nm = g4_str(lp->g, e.name_sid, &l);
    if (!nm || l > G4_REPL_STR_CAP) return 0;
    u32 n2 = 0, g2 = 0;
    if (!g4_name_get(lp->g, nm, l, &n2, &g2) || n2 != node) return 0;
    u32 bl = g4_vrow_pack(lp->g, &e, blob, REPL_ROW_CAP);
    if (!bl) return 0;
    memcpy(name, nm, l);
    *nl = l;
    *gen = g2;
    *blen = bl;
    return 1;
}

static int lp_entity_set(void *ctx, u32 node, const u8 *name, u16 nl, u32 gen,
                         const u8 *blob, u32 blen, u8 *status) {
    lp_t *lp = (lp_t *)ctx;
    *status = (u8)g4_mirror_apply(lp->g, node, name, nl, gen, blob, blen);
    return 1;
}

/* ---- the round (driver side) ---- */

int g4_repl_round_wire(graph4_t *local, repl_peer_t *peer, g4_repl_stats_t *st) {
    g4_repl_stats_t stats;
    memset(&stats, 0, sizeof stats);
    int ok = 1;

    u8 *local_only[3] = { NULL, NULL, NULL };
    u8 *remote_only[3] = { NULL, NULL, NULL };
    u32 nlocal[3] = { 0, 0, 0 }, nremote[3] = { 0, 0, 0 };

    /* 1. per family: snapshot LOCAL set, exchange cells, decode both diffs.
     * 0 = adjacency, 1 = vertex-state, 2 = name directory (per §3/§9 — the
     * only family that can see tombstones and ghost rows). */
    for (int f = 0; f < 3 && ok; f++) {
        rsymset_t loc = {0};
        if (f == 0) g4_adj_symbols(local, rcollect, &loc);
        else if (f == 1) g4_vstate_symbols(local, rcollect, &loc);
        else g4_ndir_symbols(local, rcollect, &loc);
        qsort(loc.v, loc.n, RIBLT_WIDTH, rsym_cmp);
        rsym_uniq(&loc);

        riblt_enc le;
        if (riblt_enc_init(&le, "kbrepl", 6, loc.v, loc.n) != 0) {
            free(loc.v); ok = 0; break;
        }
        free(loc.v);

        u32 nsym = 0;
        if (!peer->begin(peer->ctx, (u8)f, &nsym)) { riblt_enc_free(&le); ok = 0; break; }
        riblt_dec d;
        if (riblt_dec_init(&d, &le, "kbrepl", 6) != 0) {
            riblt_enc_free(&le);
            peer->end(peer->ctx, (u8)f);
            ok = 0;
            break;
        }
        u8 *buf = (u8 *)malloc(REPL_CELL_BATCH * 44u);
        if (!buf) {
            riblt_dec_free(&d); riblt_enc_free(&le);
            peer->end(peer->ctx, (u8)f);
            ok = 0;
            break;
        }
        u32 idx = 0;
        int done = 0;
        while (!done && idx < REPL_INDEX_CAP) {
            u32 n = 0;
            if (!peer->cells(peer->ctx, (u8)f, idx, REPL_CELL_BATCH, buf, &n) || n == 0) break;
            for (u32 i = 0; i < n && !done; i++) {
                riblt_cell c;
                riblt_cell_unpack(&c, buf + (size_t)i * 44);
                int r = riblt_dec_feed(&d, idx + i, &c);
                if (r < 0) { ok = 0; break; }
                done = r == 1;
            }
            idx += n;
        }
        if (getenv("G4_REPL_DEBUG"))
            fprintf(stderr, "[repl][%d] fam=%d loc=%u cells=%u done=%d\n",
                    (int)getpid(), f, nsym, idx, done);
        free(buf);
        peer->end(peer->ctx, (u8)f);
        if (!ok || !done) { ok = 0; riblt_dec_free(&d); riblt_enc_free(&le); break; }

        nlocal[f] = d.local_only_n;
        nremote[f] = d.remote_only_n;
        if (nlocal[f]) {
            local_only[f] = (u8 *)malloc((size_t)nlocal[f] * RIBLT_WIDTH);
            if (!local_only[f]) abort();
            memcpy(local_only[f], d.local_only, (size_t)nlocal[f] * RIBLT_WIDTH);
        }
        if (nremote[f]) {
            remote_only[f] = (u8 *)malloc((size_t)nremote[f] * RIBLT_WIDTH);
            if (!remote_only[f]) abort();
            memcpy(remote_only[f], d.remote_only, (size_t)nremote[f] * RIBLT_WIDTH);
        }
        riblt_dec_free(&d);
        riblt_enc_free(&le);
    }
    if (!ok) goto cleanup;

    /* 2. apply — edges. local_only: we hold it, peer pulls (or its wm covers
     * and WE delete). remote_only: we lack it — our wm covers => push the
     * delete to the holder; else fetch the reltype and create locally. */
    {
        u8 *rtbuf = NULL;
        for (int list = 0; list < 2; list++) {
            const u8 *v = (list == 0) ? local_only[0] : remote_only[0];
            u32 n = (list == 0) ? nlocal[0] : nremote[0];
            for (u32 i = 0; i < n; i++) {
                const u8 *s = v + (size_t)i * RIBLT_WIDTH;
                u32 lo = sym_u32(s), hi = sym_u32(s + 4);
                u64 rh = sym_u64(s + 8);
                u8 dlo = s[16];
                u64 mt = sym_u64(s + 17);
                u32 from = (dlo == G4_DIR_FORWARD) ? lo : hi;
                u32 to   = (dlo == G4_DIR_FORWARD) ? hi : lo;
                if (list == 0) {
                    u16 rl = REPL_STR_CAP;
                    if (!rtbuf) { rtbuf = (u8 *)malloc(REPL_STR_CAP); if (!rtbuf) abort(); }
                    if (!g4_edge_find_hashed(local, from, to, rh, rtbuf, &rl)) {
                        stats.edge_skipped++;
                        continue;
                    }
                    u8 status = 3;
                    if (!peer->edge_pull(peer->ctx, s, rtbuf, rl, &status)) {
                        stats.edge_skipped++;
                        continue;
                    }
                    if (status == 0) {
                        /* peer's watermark covers it: the delete wins */
                        if (g4_delete_relation(local, from, to, rtbuf, rl)) stats.edges_deleted_from_a++;
                        else stats.edge_skipped++;
                    } else if (status == 1) stats.edges_pulled_b++;
                    else if (status == 2) stats.edges_dup++;
                    else stats.edge_skipped++;
                    continue;
                }
                /* remote_only */
                u64 wm = g4_chain_wm(local, lo);
                if (hi != lo) {
                    u64 w2 = g4_chain_wm(local, hi);
                    if (w2 > wm) wm = w2;
                }
                if (wm >= mt) {
                    u8 status = 0;
                    if (peer->edge_del(peer->ctx, s, &status) && status) stats.edges_deleted_from_b++;
                    else stats.edge_skipped++;
                    continue;
                }
                if (!rtbuf) { rtbuf = (u8 *)malloc(REPL_STR_CAP); if (!rtbuf) abort(); }
                u16 rl = 0;
                if (!peer->relname(peer->ctx, rh, rtbuf, &rl)) { stats.edge_skipped++; continue; }
                int r = g4_create_relation(local, from, to, rtbuf, rl, mt);
                if (r) stats.edges_pulled_a++;
                else if (edge_present(local, from, to, rtbuf, rl)) stats.edges_dup++;
                else stats.edge_skipped++;
            }
        }
        free(rtbuf);
    }

    /* 3. apply — vstate + entity mirrors. The union of diff nodes: shared
     * nodes take whole-row LWW (then the binding generation reconciles);
     * one-sided nodes mirror — the record is created at the EXACT node id
     * under the name (gen, node) rule, and a beaten local binding's record
     * retires (seam §4/§9). */
    {
        u32 nn = nlocal[1] + nremote[1];
        if (nn) {
            u32 *nodes = (u32 *)malloc((size_t)nn * 4);
            u8 *blob = (u8 *)malloc(REPL_ROW_CAP);
            u8 *pb = (u8 *)malloc(REPL_ROW_CAP);
            u8 *pnm = (u8 *)malloc(REPL_STR_CAP);
            if (!nodes || !blob || !pb || !pnm) abort();
            nn = 0;
            for (int list = 0; list < 2; list++) {
                const u8 *v = (list == 0) ? local_only[1] : remote_only[1];
                u32 n = (list == 0) ? nlocal[1] : nremote[1];
                for (u32 i = 0; i < n; i++) nodes[nn++] = sym_u32(v + (size_t)i * RIBLT_WIDTH);
            }
            qsort(nodes, nn, 4, u32cmp);
            for (u32 i = 0; i < nn; i++) {
                if (i && nodes[i] == nodes[i - 1]) continue;
                u32 node = nodes[i];
                g4_entity_t le;
                int ha = g4_read_entity(local, node, &le);
                u32 bl = 0;
                int hb = peer->vrow(peer->ctx, node, blob, &bl);
                if (!ha && !hb) { stats.vstate_skipped++; continue; }

                if (ha && hb) {
                    int c = g4_vrow_cmp(local, blob, bl, &le);
                    if (getenv("G4_REPL_DEBUG") && c != 0)
                        fprintf(stderr, "[replA] n=%u c=%d mt=%llu omt=%llu psi=%.12g bl=%u\n",
                                node, c, (unsigned long long)le.mtime,
                                (unsigned long long)le.obs_mtime, le.psi, bl);
                    if (c > 0) {
                        if (g4_vrow_apply(local, node, blob, bl)) stats.vstate_applied_a++;
                        else stats.vstate_skipped++;
                    } else if (c < 0) {
                        u32 pbl = g4_vrow_pack(local, &le, pb, REPL_ROW_CAP);
                        u8 st = 0;
                        if (pbl && peer->vrow_set(peer->ctx, node, pb, pbl, &st) && st)
                            stats.vstate_applied_b++;
                        else stats.vstate_skipped++;
                    } else stats.vstate_skipped++;
                    /* binding generation (gen-only differences) */
                    {
                        u16 nml = 0;
                        const u8 *nm = g4_str(local, le.name_sid, &nml);
                        u32 lnode = 0, lgen = 0;
                        if (nm && g4_name_get(local, nm, nml, &lnode, &lgen) && lnode == node) {
                            u32 pn = 0, pg = 0;
                            if (peer->name_get(peer->ctx, nm, nml, &pn, &pg) && pg != lgen) {
                                if (pg > lgen) {
                                    if (g4_mirror_name(local, nm, nml, pn, pg)) stats.name_applied_a++;
                                    else stats.vstate_skipped++;
                                } else {
                                    u8 st = 0;
                                    if (peer->name_set(peer->ctx, nm, nml, node, lgen, &st) && st)
                                        stats.name_applied_b++;
                                    else stats.vstate_skipped++;
                                }
                            }
                        }
                    }
                    continue;
                }

                if (ha && !hb) {
                    /* we hold it, the peer lacks it: push the mirror */
                    u16 nml = 0;
                    const u8 *nm = g4_str(local, le.name_sid, &nml);
                    u32 lnode = 0, lgen = 0;
                    u32 pbl = g4_vrow_pack(local, &le, pb, REPL_ROW_CAP);
                    if (!nm || !pbl || !g4_name_get(local, nm, nml, &lnode, &lgen) || lnode != node) {
                        stats.vstate_skipped++;
                        continue;
                    }
                    u8 st = 0;
                    if (!peer->entity_set(peer->ctx, node, nm, nml, lgen, pb, pbl, &st)) {
                        stats.vstate_skipped++;
                        continue;
                    }
                    if (st == 1) { stats.entity_pushed_b++; continue; }
                    if (st == 2) { stats.vstate_skipped++; continue; }   /* already equal */
                    /* lost at the peer: its binding beats ours — adopt it */
                    u32 pn = 0, pg = 0;
                    if (peer->name_get(peer->ctx, nm, nml, &pn, &pg) &&
                        g4_mirror_name(local, nm, nml, pn, pg))
                        stats.name_applied_a++;
                    else stats.vstate_skipped++;
                    continue;
                }

                /* !ha && hb: the peer holds it, we lack it: pull — unless a
                 * local binding (tombstone or rival node) beats it */
                {
                    u16 pnl = 0;
                    u32 pg = 0, pbl = 0;
                    if (!peer->entity_get(peer->ctx, node, pnm, &pnl, &pg, blob, &pbl)) {
                        stats.vstate_skipped++;
                        continue;
                    }
                    u32 lnode = 0, lgen = 0;
                    int lhave = g4_name_get(local, pnm, pnl, &lnode, &lgen);
                    if (lhave && (lgen > pg || (lgen == pg && lnode > node))) {
                        u8 st = 0;
                        if (peer->name_set(peer->ctx, pnm, pnl, lnode, lgen, &st) && st)
                            stats.name_applied_b++;
                        else stats.vstate_skipped++;
                        continue;
                    }
                    int mr = g4_mirror_apply(local, node, pnm, pnl, pg, blob, pbl);
                    if (mr == 1) stats.entity_pulled_a++;
                    else stats.vstate_skipped++;
                    continue;
                }
            }
            free(nodes); free(blob); free(pb); free(pnm);
        }
    }

    /* 4. apply — name directory. Row-only states (tombstones, ghost rows)
     * are visible ONLY here: resolve each differing symbol's hash to the
     * local and peer rows and run the (gen, node) rule. A winning live
     * node's RECORD arrives via family 1 (same round or the next). */
    {
        u32 n = nlocal[2] + nremote[2];
        if (n) {
            u8 *nm = (u8 *)malloc(REPL_STR_CAP);
            if (!nm) abort();
            for (int list = 0; list < 2; list++) {
                const u8 *v = (list == 0) ? local_only[2] : remote_only[2];
                u32 cnt = (list == 0) ? nlocal[2] : nremote[2];
                for (u32 i = 0; i < cnt; i++) {
                    const u8 *s = v + (size_t)i * RIBLT_WIDTH;
                    u64 nh = sym_u64(s);
                    u32 lnode = 0, lgen = 0, pn = 0, pg = 0;
                    u16 nml = 0;
                    if (!g4_ndir_find(local, nh, nm, &nml, &lnode, &lgen)) {
                        /* no local row at all: fetch the name from the peer */
                        if (!peer->name_by_hash(peer->ctx, nh, nm, &nml, &pn, &pg)) {
                            stats.vstate_skipped++;
                            continue;
                        }
                    } else if (!peer->name_get(peer->ctx, nm, nml, &pn, &pg)) {
                        pn = 0;
                        pg = 0;
                    }
                    if (pn == lnode && pg == lgen) { stats.vstate_skipped++; continue; }
                    if (lgen > pg || (lgen == pg && lnode > pn)) {
                        u8 st = 0;
                        if (peer->name_set(peer->ctx, nm, nml, lnode, lgen, &st) && st)
                            stats.name_applied_b++;
                        else stats.vstate_skipped++;
                    } else {
                        if (g4_mirror_name(local, nm, nml, pn, pg)) stats.name_applied_a++;
                        else stats.vstate_skipped++;
                    }
                }
            }
            free(nm);
        }
    }

cleanup:
    free(local_only[0]); free(local_only[1]); free(local_only[2]);
    free(remote_only[0]); free(remote_only[1]); free(remote_only[2]);
    if (st) *st = stats;
    return ok;
}

/* The in-process round: the same algorithm with `b` behind the adapter. */
int g4_repl_round(graph4_t *a, graph4_t *b, g4_repl_stats_t *st) {
    lp_t lp;
    memset(&lp, 0, sizeof lp);
    lp.g = b;
    repl_peer_t peer;
    peer.ctx = &lp;
    peer.begin = lp_begin;
    peer.cells = lp_cells;
    peer.end = lp_end;
    peer.relname = lp_relname;
    peer.vrow = lp_vrow;
    peer.vrow_set = lp_vrow_set;
    peer.edge_pull = lp_edge_pull;
    peer.edge_del = lp_edge_del;
    peer.name_get = lp_name_get;
    peer.name_set = lp_name_set;
    peer.name_by_hash = lp_name_by_hash;
    peer.entity_get = lp_entity_get;
    peer.entity_set = lp_entity_set;
    int ok = g4_repl_round_wire(a, &peer, st);
    for (int f = 0; f < 3; f++)
        if (lp.active[f]) riblt_enc_free(&lp.enc[f]);
    rmap_free(lp.rmap);
    return ok;
}
