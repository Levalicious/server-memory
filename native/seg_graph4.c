/*
 * seg_graph4.c — entities + persisted name index over segstores.
 *
 * See segstore.h graph4 block (Design_Graph4Entities_2026_08_24).
 * Adjacency lands in the next module; adj_ref is carried but unused here.
 */
#include "segstore.h"
#include "regex.h"
#include "re_dfa.h"
#include "re_trigram.h"

#include <stdlib.h>
#include <string.h>

#define EID_MAKE(lpg, slot) ((((u32)(lpg) << 12) | (u32)(slot)) + 1u)
#define EID_LPG(eid)  (((eid) - 1u) >> 12)
#define EID_SLOT(eid) (((eid) - 1u) & 0xFFFu)
#define G4_MAX_LPG    (1u << 20)

#define ENT_SIZE      76u    /* +u64 adj_wm at 68 (chain remove-watermark, seam §5) */

struct graph4 {
    mstore_t   *ms;
    segstore_t *gs;         /* graph segment */
    st4_t      *st;         /* strings layer (owned) */
    u32 last_adj_page;      /* ADJ insertion affinity; SEG_PT_NONE = none */
    /* index layer (seg_tree; docs/v4-index-design-note.md): catalog + the
     * name, trigram-posting and type-posting tables. Immediate in-txn. */
    seg_tree_t cat, namet, trit, typet;
    u32 ent_count;          /* live entities (persisted in META) */
    u64 structural_total, walker_total;   /* persisted in META (ruling 4-i) */
    u32 last_ent_page;      /* insertion affinity; SEG_PT_NONE = none */
    /* node-id indirection (docs/shard-seam-design-note.md §3, build step 2;
     * spec §6.4 r3.3): every id that crosses this module's API is a LOGICAL
     * node id, resolved here to the physical EID_MAKE(lpg, slot). Dir page
     * holds one record [u32 npages][u32 lpg x npages]; data page p holds one
     * record of IND_PAGE_SLOTS u32 eids for nodes [p*S, (p+1)*S). Dir
     * capacity 1018 pages ≈ 1.03M nodes — the named level-1 cliff (a
     * second-level dir or a tree replaces it if the live KB ever crosses).
     * ind_root lives in the catalog (K_IND); next_node in META v2. */
    u32 ind_root;           /* dir page lpg; 0 = not yet created */
    u32 next_node;          /* next logical id to hand out (>= 1) */
};

/* ---------- LE helpers ---------- */
static u32  g4ld32(const u8 *p) { return (u32)p[0]|((u32)p[1]<<8)|((u32)p[2]<<16)|((u32)p[3]<<24); }
static void g4st32(u8 *p, u32 v) { p[0]=(u8)v; p[1]=(u8)(v>>8); p[2]=(u8)(v>>16); p[3]=(u8)(v>>24); }
static u64  g4ld64(const u8 *p) { return (u64)g4ld32(p) | ((u64)g4ld32(p+4) << 32); }
static void g4st64(u8 *p, u64 v) { g4st32(p, (u32)v); g4st32(p+4, (u32)(v>>32)); }

/* ---------- entity record encode/decode ---------- */

static void ent_encode(u8 *r, const g4_entity_t *e) {
    g4st32(r + 0,  1u);                    /* version */
    g4st32(r + 4,  e->name_sid);
    g4st32(r + 8,  e->type_sid);
    g4st32(r + 12, e->adj_ref);
    g4st64(r + 16, e->mtime);
    g4st64(r + 24, e->obs_mtime);
    g4st32(r + 32, e->obs0_sid);
    g4st32(r + 36, e->obs1_sid);
    r[40] = e->obs_count; r[41] = r[42] = r[43] = 0;
    g4st64(r + 44, e->structural_visits);
    g4st64(r + 52, e->walker_visits);
    memcpy(r + 60, &e->psi, 8);
    g4st64(r + 68, 0);                     /* adj_wm: fresh entities start clean */
}

static int ent_decode(const u8 *r, u16 sz, g4_entity_t *e) {
    if (sz != ENT_SIZE || g4ld32(r) != 1u) return 0;
    e->name_sid = g4ld32(r + 4);
    e->type_sid = g4ld32(r + 8);
    e->adj_ref  = g4ld32(r + 12);
    e->mtime     = g4ld64(r + 16);
    e->obs_mtime = g4ld64(r + 24);
    e->obs0_sid = g4ld32(r + 32);
    e->obs1_sid = g4ld32(r + 36);
    e->obs_count = r[40];
    e->structural_visits = g4ld64(r + 44);
    e->walker_visits     = g4ld64(r + 52);
    memcpy(&e->psi, r + 60, 8);
    e->adj_wm = g4ld64(r + 68);
    return 1;
}

static const u8 *ent_rec(graph4_t *g, u32 node, u16 *sz_out);

/* ---------- node-id indirection (build step 2) ----------
 * Logical node ids are the ONLY ids this module hands out or accepts; the
 * EID_* macros appear exclusively at the chokepoints below and in
 * g4_create_entity/g4_delete_entity. A killed id resolves to 0 — reading
 * through a stale node id fails cleanly instead of aliasing a recycled
 * physical slot (the v3 reuse-aliasing failure class). */

#define IND_PAGE_SLOTS (SEG_PAGE_MAX_REC / 4u)        /* 1019 eids per data page */
#define IND_DIR_MAX    ((SEG_PAGE_MAX_REC - 4u) / 4u) /* 1018 lpg entries in the dir */

/* catalog key for the indirection dir page (same namespace as the K_* set
 * defined below with the other tables) */
static const u8 K_IND[2] = { 0x01, 'i' };

static void cat_put_root(graph4_t *g, const u8 *key, u16 kl, u32 root);

static u32 ind_lookup(graph4_t *g, u32 node) {
    if (!node || !g->ind_root) return 0;
    u32 idx = node - 1u;
    u32 page = idx / IND_PAGE_SLOTS, slot = idx % IND_PAGE_SLOTS;
    const u8 *dpg = seg_txn_view(g->gs, g->ind_root);
    if (!dpg) return 0;
    u16 dsz = 0;
    const u8 *d = seg_page_read(dpg, 0, &dsz);
    if (!d || dsz < 4) return 0;
    if (page >= g4ld32(d)) return 0;
    u32 dlpg = g4ld32(d + 4 + page * 4u);
    const u8 *ppg = seg_txn_view(g->gs, dlpg);
    if (!ppg) return 0;
    u16 psz = 0;
    const u8 *p = seg_page_read(ppg, 0, &psz);
    if (!p || (u32)psz < (slot + 1u) * 4u) return 0;
    return g4ld32(p + slot * 4u);
}

/* Make `eid` reachable under a fresh logical id. 0 = failure (dir cliff or
 * OOM — a loud create failure, never a silent wrong id). */
static u32 ind_append(graph4_t *g, u32 eid) {
    u32 node = g->next_node;
    if (!node || !eid) return 0;
    u32 idx = node - 1u;
    u32 page = idx / IND_PAGE_SLOTS, slot = idx % IND_PAGE_SLOTS;
    /* dir page: create at FULL size once, then poke npages + the new lpg in
     * place. (A growing record would relocate on every append —
     * seg_page_update's grow path consumes fresh space per copy and dies
     * around 43 pages; the import gate caught exactly that at 43,817 nodes.) */
    if (!g->ind_root) {
        u32 dlpg = 0;
        u8 *dpg = seg_txn_alloc(g->gs, SEG_KIND_INDIRECT, &dlpg);
        if (!dpg) return 0;
        u8 *drec = (u8 *)calloc(1, 4u + IND_DIR_MAX * 4u);
        if (!drec) return 0;
        u16 s = 0;
        int ok = seg_page_insert(dpg, drec, (u16)(4u + IND_DIR_MAX * 4u), &s);
        free(drec);
        if (!ok) return 0;
        g->ind_root = dlpg;
        cat_put_root(g, K_IND, 2, dlpg);
    }
    if (page >= IND_DIR_MAX) return 0;                /* level-1 dir cliff */
    u8 *dpg = seg_txn_touch(g->gs, g->ind_root);
    if (!dpg) return 0;
    u16 dsz = 0;
    const u8 *dr = seg_page_read(dpg, 0, &dsz);
    if (!dr || dsz < 4) return 0;
    u8 *dw = (u8 *)(dpg + (dr - dpg));
    u32 npages = g4ld32(dw);
    while (npages <= page) {          /* holes are legal (sparse ids: a
                                       * mirror or an id-base jump lands far
                                       * above the dense prefix) */
        u32 plpg = 0;
        u8 *dp = seg_txn_alloc(g->gs, SEG_KIND_INDIRECT, &plpg);
        if (!dp) return 0;
        u8 *zrec = (u8 *)calloc(1, IND_PAGE_SLOTS * 4u);
        if (!zrec) return 0;
        u16 s = 0;
        int ok = seg_page_insert(dp, zrec, (u16)(IND_PAGE_SLOTS * 4u), &s);
        free(zrec);
        if (!ok) return 0;
        g4st32(dw + 4 + npages * 4u, plpg);           /* in-place: never moves */
        npages++;
        g4st32(dw, npages);
    }
    u32 dlpg2 = g4ld32(dw + 4 + page * 4u);           /* same image: still valid */
    u8 *ppg = seg_txn_touch(g->gs, dlpg2);
    if (!ppg) return 0;
    u16 psz = 0;
    const u8 *p = seg_page_read(ppg, 0, &psz);
    if (!p || (u32)psz < (slot + 1u) * 4u) return 0;
    g4st32(ppg + (p - ppg) + slot * 4u, eid);
    g->next_node = node + 1u;
    return node;
}

/* Retire a logical id (entity delete): it can never alias a later entity. */
static void ind_kill(graph4_t *g, u32 node) {
    if (!node || !g->ind_root) return;
    u32 idx = node - 1u;
    u32 page = idx / IND_PAGE_SLOTS, slot = idx % IND_PAGE_SLOTS;
    const u8 *dpg = seg_txn_view(g->gs, g->ind_root);
    if (!dpg) return;
    u16 dsz = 0;
    const u8 *d = seg_page_read(dpg, 0, &dsz);
    if (!d || dsz < 4 || page >= g4ld32(d)) return;
    u32 dlpg = g4ld32(d + 4 + page * 4u);
    u8 *ppg = seg_txn_touch(g->gs, dlpg);
    if (!ppg) return;
    u16 psz = 0;
    const u8 *p = seg_page_read(ppg, 0, &psz);
    if (!p || (u32)psz < (slot + 1u) * 4u) return;
    g4st32(ppg + (p - ppg) + slot * 4u, 0);
}

static const u8 *ent_rec(graph4_t *g, u32 node, u16 *sz_out) {
    u32 eid = ind_lookup(g, node);
    if (!eid) return NULL;
    const u8 *pg = seg_txn_view(g->gs, EID_LPG(eid));
    if (!pg) return NULL;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, (u16)EID_SLOT(eid), &sz);
    if (!r || sz != ENT_SIZE) return NULL;
    if (sz_out) *sz_out = sz;
    return r;
}

/* write-through: touch page, update record in place (same size) */
static u8 *ent_rec_w(graph4_t *g, u32 node) {
    u32 eid = ind_lookup(g, node);
    if (!eid) return NULL;
    u8 *pg = seg_txn_touch(g->gs, EID_LPG(eid));
    if (!pg) return NULL;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, (u16)EID_SLOT(eid), &sz);
    if (!r || sz != ENT_SIZE) return NULL;
    return (u8 *)(pg + (r - pg));
}

/* ---------- index layer (seg_tree; docs/v4-index-design-note.md) ----------
 * Catalog + three tables, replacing the old hash name-index / in-memory
 * trigram / in-memory type postings:
 *   cat   : key "\x00META" (totals, counts) | "\x01n"/"\x01t"/"\x01y" (table
 *           roots, u32) — the catalog's own root lives in the segment root
 *           slot (segstore_nameindex_root).
 *   name  : var key (name bytes) -> u32 eid.
 *   tri   : fixed 7B [tri3][eid BE] postings (prefix range scan per trigram;
 *           eid BIG-endian so scan order is ascending by eid).
 *   type  : fixed 8B [type_sid LE][eid BE] postings.
 * All updates are immediate, in the caller's txn (one commit, one
 * consistency story); every table op re-puts its root into the catalog and
 * re-stages the segment roots. */

static const u8 K_META[5] = { 0x00, 'M', 'E', 'T', 'A' };
static const u8 K_NAME[2] = { 0x01, 'n' };
static const u8 K_TRI[2]  = { 0x01, 't' };
static const u8 K_TYPE[2] = { 0x01, 'y' };
static const st_codec_t CD_CAT  = { 0, 2 };
static const st_codec_t CD_NAME = { 0, 2 };   /* name -> {node u32, gen u32} (step 3) */
static const st_codec_t CD_TRI  = { 7, 0 };
static const st_codec_t CD_TYPE = { 8, 0 };

static void cat_sync_roots(graph4_t *g) {
    if (g->cat.root) seg_txn_set_roots(g->gs, g->cat.root, 0);
}

static void cat_put_root(graph4_t *g, const u8 *key, u16 kl, u32 root) {
    u8 v[4];
    g4st32(v, root);
    seg_tree_insert(&g->cat, key, kl, v, 4);
    cat_sync_roots(g);
}

static void meta_store(graph4_t *g) {
    u8 v[28];
    g4st32(v, 2);                                  /* format version (v2: + next_node) */
    g4st64(v + 4, g->structural_total);
    g4st64(v + 12, g->walker_total);
    g4st32(v + 20, g->ent_count);
    g4st32(v + 24, g->next_node);
    seg_tree_insert(&g->cat, K_META, 5, v, 28);
    cat_sync_roots(g);
}

/* read a table root value from the catalog (0 = empty/absent) */
static u32 cat_get_root(graph4_t *g, const u8 *key, u16 kl) {
    u8 v[4];
    u16 vl = 4;
    return (seg_tree_lookup(&g->cat, key, kl, v, &vl) == 1 && vl == 4) ? g4ld32(v) : 0;
}

/* ---- name table ---- */

/* ---- the name directory (build step 3; docs/shard-seam-design-note.md §3) ----
 * Rows bind a name to {node-id, generation}:
 *   node == 0  — a tombstone: the name is unbound. The row persists for the
 *                in-flight window (§9's directory-tombstone contract) so a
 *                later re-creation advances the generation; skipping
 *                tombstones is the job of every consumer (ni_walk_scan_cb).
 *   gen        — advances on EVERY transition (bind or unbind): any cache
 *                holding (name, gen) detects a rebinding, and the sharded
 *                era's per-name RIBLT symbol is exactly this pair.
 * Routing (sharded era) is a pure function of the name bytes:
 * shard = fnv1a32(name) % N (N=1 today); no per-store state is read to
 * decide where a binding lives, so the row shape is the whole commitment. */

static int name_row(graph4_t *g, const u8 *nm, u16 nl, u32 *node, u32 *gen) {
    u8 v[8];
    u16 vl = 8;
    if (seg_tree_lookup(&g->namet, nm, nl, v, &vl) != 1 || vl != 8) {
        *node = 0; *gen = 0;
        return 0;
    }
    *node = g4ld32(v);
    *gen = g4ld32(v + 4);
    return 1;
}

static int name_bind_raw(graph4_t *g, const u8 *nm, u16 nl, u32 node, u32 gen) {
    u8 v[8];
    g4st32(v, node);
    g4st32(v + 4, gen);
    int r = seg_tree_insert(&g->namet, nm, nl, v, 8);
    cat_put_root(g, K_NAME, 2, g->namet.root);
    return r >= 0;              /* 1 = fresh row, 0 = replaced (tombstone/live) */
}

/* bind a fresh node to an unbound-or-tombstoned name (callers have already
 * verified lookup == 0); returns 0 if a live binding exists */
static int name_bind(graph4_t *g, const u8 *nm, u16 nl, u32 node) {
    u32 cur = 0, gen = 0;
    (void)name_row(g, nm, nl, &cur, &gen);
    if (cur) return 0;
    return name_bind_raw(g, nm, nl, node, gen + 1u);
}

/* unbind: tombstone the row, advancing the generation */
static int name_unbind(graph4_t *g, const u8 *nm, u16 nl) {
    u32 cur = 0, gen = 0;
    if (!name_row(g, nm, nl, &cur, &gen) || !cur) return 0;
    return name_bind_raw(g, nm, nl, 0, gen + 1u);
}

/* ---- trigram posting table ---- */

static void tri_key(u32 tri, u32 eid, u8 k[7]) {
    k[0] = (u8)(tri >> 16); k[1] = (u8)(tri >> 8); k[2] = (u8)tri;
    k[3] = (u8)(eid >> 24); k[4] = (u8)(eid >> 16); k[5] = (u8)(eid >> 8); k[6] = (u8)eid;
}

/* growable u32 set (raw trigram pack values) */
typedef struct { u32 *v; u32 n, cap; } u32set_t;
static int u32set_push(u32set_t *s, u32 x) {
    if (s->n == s->cap) {
        u32 nc = s->cap ? s->cap * 2 : 64;
        u32 *nv = (u32 *)realloc(s->v, (size_t)nc * 4);
        if (!nv) return 0;
        s->v = nv; s->cap = nc;
    }
    s->v[s->n++] = x;
    return 1;
}
static int cmp_u32v(const void *a, const void *b) {
    u32 x = *(const u32 *)a, y = *(const u32 *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

typedef struct { u32set_t *s; } tri_collect_ctx;
static void tri_collect_cb(void *ctx, u32 tri) {
    u32set_t *s = ((tri_collect_ctx *)ctx)->s;
    (void)u32set_push(s, tri);
}

/* The entity's field-trigram UNION (sorted, deduped): name + type + obs. */
static u32 tri_collect_entity(graph4_t *g, const g4_entity_t *e, u32set_t *out) {
    const u32 sids[4] = { e->name_sid, e->type_sid, e->obs0_sid, e->obs1_sid };
    int nf = 2 + (e->obs_count >= 1 ? 1 : 0) + (e->obs_count >= 2 ? 1 : 0);
    tri_collect_ctx c = { out };
    for (int f = 0; f < nf; f++) {
        if (!sids[f]) continue;
        u16 l = 0;
        const u8 *b = st4_get(g->st, sids[f], &l);
        if (b) re_trigram_foreach((const char *)b, l, tri_collect_cb, &c);
    }
    if (out->n > 1) qsort(out->v, out->n, 4, cmp_u32v);
    u32 u = 0;
    for (u32 i = 0; i < out->n; i++)
        if (i == 0 || out->v[i] != out->v[i - 1]) out->v[u++] = out->v[i];
    out->n = u;
    return u;
}

/* Apply the delta between the old trigram set and the entity's current one:
 * old-only => delete posting, new-only => insert posting (O(1) tree ops). */
static void tri_apply(graph4_t *g, u32 eid, const u32 *old, u32 oldn) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return;
    u32set_t cur = {0};
    tri_collect_entity(g, &e, &cur);
    u32 i = 0, j = 0;
    int changed = 0;
    while (i < oldn || j < cur.n) {
        u32 ov = i < oldn ? old[i] : 0xFFFFFFFFu;
        u32 nv = j < cur.n ? cur.v[j] : 0xFFFFFFFFu;
        if (i < oldn && (j >= cur.n || ov < nv)) {
            u8 k[7]; tri_key(ov, eid, k);
            seg_tree_delete(&g->trit, k, 7);
            i++; changed = 1;
        } else if (j < cur.n && (i >= oldn || nv < ov)) {
            u8 k[7]; tri_key(nv, eid, k);
            seg_tree_insert(&g->trit, k, 7, NULL, 0);
            j++; changed = 1;
        } else { i++; j++; }
    }
    free(cur.v);
    if (changed) cat_put_root(g, K_TRI, 2, g->trit.root);
}

/* ---- type posting table ---- */

static void type_key(u32 sid, u32 eid, u8 k[8]) {
    k[0] = (u8)sid; k[1] = (u8)(sid >> 8); k[2] = (u8)(sid >> 16); k[3] = (u8)(sid >> 24);
    k[4] = (u8)(eid >> 24); k[5] = (u8)(eid >> 16); k[6] = (u8)(eid >> 8); k[7] = (u8)eid;
}
static void type_put(graph4_t *g, u32 sid, u32 eid) {
    u8 k[8]; type_key(sid, eid, k);
    seg_tree_insert(&g->typet, k, 8, NULL, 0);
    cat_put_root(g, K_TYPE, 2, g->typet.root);
}
static void type_del(graph4_t *g, u32 sid, u32 eid) {
    u8 k[8]; type_key(sid, eid, k);
    if (seg_tree_delete(&g->typet, k, 8) == 1) cat_put_root(g, K_TYPE, 2, g->typet.root);
}

typedef struct { u32 *out; u32 n, max; } eid_scan_t;
static int eid_scan_cb(void *ctx, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)k; (void)v; (void)vl;
    eid_scan_t *s = (eid_scan_t *)ctx;
    if (kl >= 4) {
        u32 eid = ((u32)k[kl - 4] << 24) | ((u32)k[kl - 3] << 16) | ((u32)k[kl - 2] << 8) | (u32)k[kl - 1];
        if (s->n < s->max) s->out[s->n] = eid;
        s->n++;
    }
    return 1;
}

/* ---- candidate evaluation over the tri tree (provider for re_trigram) ---- */

typedef struct { graph4_t *g; } g4_tri_ctx;

typedef struct { u64 *v; u32 n, cap; int oom; } c64_t;
static int c64_cb(void *ctx, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)v; (void)vl;
    c64_t *c = (c64_t *)ctx;
    if (kl >= 4) {
        u32 eid = ((u32)k[kl - 4] << 24) | ((u32)k[kl - 3] << 16) | ((u32)k[kl - 2] << 8) | (u32)k[kl - 1];
        if (c->n == c->cap) {
            u32 nc = c->cap ? c->cap * 2 : 256;
            u64 *nv = (u64 *)realloc(c->v, (size_t)nc * 8);
            if (!nv) { c->oom = 1; return 0; }
            c->v = nv; c->cap = nc;
        }
        c->v[c->n++] = eid;
    }
    return 1;
}

static u64 *g4_tri_leaf_impl(graph4_t *g, const u8 *lo, const u8 *hi, u32 *n_out) {
    c64_t c = { NULL, 0, 0, 0 };
    seg_tree_scan(&g->trit, lo, 7, 1, hi, 7, 1, c64_cb, &c);
    if (c.oom) { free(c.v); *n_out = 0; return NULL; }
    *n_out = c.n;
    return c.v;
}

static u64 *g4_tri_leaf(void *ctx, u32 tri, u32 *n_out) {
    graph4_t *g = ((g4_tri_ctx *)ctx)->g;
    u8 lo[7], hi[7];
    tri_key(tri, 0, lo);
    hi[0] = lo[0]; hi[1] = lo[1]; hi[2] = lo[2];
    hi[3] = 0xFF; hi[4] = 0xFF; hi[5] = 0xFF; hi[6] = 0xFF;
    return g4_tri_leaf_impl(g, lo, hi, n_out);
}

static ReCandidates64 g4_candidates(graph4_t *g, const ReTrigramQuery *q) {
    g4_tri_ctx c = { g };
    ReTriProvider p;
    p.ctx = &c;
    p.leaf = g4_tri_leaf;
    return re_trigram_eval_provider(q, &p);
}

/* ---- entity enumeration (the name table is the registry) ---- */

typedef struct { int (*cb)(void *ctx, u32 eid); void *ctx; } ni_walk_t;
static int ni_walk_scan_cb(void *c, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)k; (void)kl;
    ni_walk_t *w = (ni_walk_t *)c;
    if (!v || vl != 8) return 1;
    u32 node = g4ld32(v);
    if (!node) return 1;                       /* tombstone row: unbound name */
    return w->cb(w->ctx, node) ? 1 : 0;
}
static int ni_walk(graph4_t *g, int (*cb)(void *ctx, u32 eid), void *ctx) {
    ni_walk_t w = { cb, ctx };
    return seg_tree_scan(&g->namet, NULL, 0, 1, NULL, 0, 1, ni_walk_scan_cb, &w);
}


/* ---------- lifecycle ---------- */

graph4_t *graph4_open(mstore_t *ms) {
    if (mstore_nsegs(ms) < 2) return NULL;
    graph4_t *g = (graph4_t *)calloc(1, sizeof *g);
    if (!g) return NULL;
    g->ms = ms;
    g->gs = mstore_seg(ms, G4_SEG_GRAPH);
    g->st = st4_open(mstore_seg(ms, G4_SEG_STRINGS));
    if (!g->gs || !g->st) { graph4_close(g); return NULL; }
    g->last_ent_page = SEG_PT_NONE;
    g->last_adj_page = SEG_PT_NONE;

    /* Index layer from the committed roots: catalog (segment root slot) holds
     * META (totals, counts — ruling 4-i) and each table's root. O(1) open:
     * one catalog read; no scans, no rebuilds. */
    g->cat.root = segstore_nameindex_root(g->gs);
    seg_tree_open(&g->cat, g->gs, CD_CAT, g->cat.root);
    g->namet.root = cat_get_root(g, K_NAME, 2);
    g->trit.root  = cat_get_root(g, K_TRI, 2);
    g->typet.root = cat_get_root(g, K_TYPE, 2);
    g->ind_root   = cat_get_root(g, K_IND, 2);
    seg_tree_open(&g->namet, g->gs, CD_NAME, g->namet.root);
    seg_tree_open(&g->trit,  g->gs, CD_TRI,  g->trit.root);
    seg_tree_open(&g->typet, g->gs, CD_TYPE, g->typet.root);
    {
        u8 v[28];
        u16 vl = 28;
        if (seg_tree_lookup(&g->cat, K_META, 5, v, &vl) == 1) {
            /* pre-release: META v2 only (v1 stores carry no next_node and
             * would mint colliding logical ids — refuse, never guess) */
            if (vl != 28 || g4ld32(v) != 2) { graph4_close(g); return NULL; }
            g->structural_total = g4ld64(v + 4);
            g->walker_total     = g4ld64(v + 12);
            g->ent_count        = g4ld32(v + 20);
            g->next_node        = g4ld32(v + 24);
        }
    }
    if (!g->next_node) g->next_node = 1;
    return g;
}

void graph4_close(graph4_t *g) {
    if (!g) return;
    st4_close(g->st);
    free(g);
}

u32 g4_entity_count(graph4_t *g) { return g->ent_count; }
const u8 *g4_str(graph4_t *g, u32 sid, u16 *len_out) { return st4_get(g->st, sid, len_out); }

/* ---------- ops ---------- */

u32 g4_lookup_ex(graph4_t *g, const u8 *name, u16 nlen, u32 *gen_out) {
    u32 node = 0, gen = 0;
    (void)name_row(g, name, nlen, &node, &gen);
    if (gen_out) *gen_out = gen;
    return node;
}

u32 g4_lookup(graph4_t *g, const u8 *name, u16 nlen) {
    return g4_lookup_ex(g, name, nlen, NULL);
}

u32 g4_create_entity(graph4_t *g, const u8 *name, u16 nlen,
                     const u8 *type, u16 tlen, u64 mtime) {
    u32 existing = g4_lookup(g, name, nlen);
    if (existing) return existing;

    u32 name_sid = st4_intern(g->st, name, nlen);
    u32 type_sid = st4_intern(g->st, type, tlen);
    if (!name_sid || !type_sid) return 0;

    g4_entity_t e;
    memset(&e, 0, sizeof e);
    e.name_sid = name_sid; e.type_sid = type_sid;
    e.mtime = mtime; e.obs_mtime = 0;
    u8 rec[ENT_SIZE];
    ent_encode(rec, &e);

    u32 lpg = g->last_ent_page; u16 slot = 0;
    u8 *pg = (lpg != (u32)SEG_PT_NONE) ? seg_txn_touch(g->gs, lpg) : NULL;
    if (!pg || !seg_page_insert(pg, rec, ENT_SIZE, &slot)) {
        pg = seg_txn_alloc(g->gs, SEG_KIND_ENTITY, &lpg);
        if (!pg || lpg >= G4_MAX_LPG || !seg_page_insert(pg, rec, ENT_SIZE, &slot))
            return 0;
        g->last_ent_page = lpg;
    }
    u32 eid = EID_MAKE(lpg, slot);                 /* physical location */
    u32 node = ind_append(g, eid);                 /* logical id (the API's) */
    if (!node) return 0;
    if (!name_bind(g, name, nlen, node)) return 0;
    type_put(g, type_sid, node);
    g->ent_count++;
    meta_store(g);                                 /* persists next_node (META v2) */
    { u32set_t old = {0}; tri_apply(g, node, old.v, 0); free(old.v); }  /* index current fields */
    return node;
}

int g4_read_entity(graph4_t *g, u32 eid, g4_entity_t *out) {
    u16 sz = 0;
    const u8 *r = ent_rec(g, eid, &sz);
    if (!r) return 0;
    if (!ent_decode(r, sz, out)) return 0;
    out->eid = eid;
    return 1;
}

static int adj_clear_all(graph4_t *g, u32 eid, const g4_entity_t *e);

/* remove an entity's record + incident edges + its id. unbind_name=1 is the
 * public g4_delete_entity (the name row is tombstoned, advancing the
 * generation); unbind_name=0 is the mirror path's RETIRE — the name row is
 * being overwritten by the winning binding instead, so no generation churn. */
static int ent_remove(graph4_t *g, u32 node, int unbind_name) {
    g4_entity_t e;
    if (!g4_read_entity(g, node, &e)) return 0;
    u32 eid = ind_lookup(g, node);                 /* physical location */
    if (!eid) return 0;
    /* the trigram set must be collected BEFORE the strings are released */
    u32set_t old = {0};
    tri_collect_entity(g, &e, &old);
    u16 nl = 0;
    u8 *nmc = NULL;
    if (unbind_name) {
        const u8 *nmp = st4_get(g->st, e.name_sid, &nl);
        if (nmp) { nmc = (u8 *)malloc(nl ? nl : 1); if (nmc) memcpy(nmc, nmp, nl); }
    }
    /* remove every incident edge (mirrors on peers + own chain) first */
    if (!adj_clear_all(g, node, &e)) { free(old.v); free(nmc); return 0; }
    /* release string refs */
    st4_decref(g->st, e.name_sid);
    st4_decref(g->st, e.type_sid);
    if (e.obs_count >= 1 && e.obs0_sid) st4_decref(g->st, e.obs0_sid);
    if (e.obs_count >= 2 && e.obs1_sid) st4_decref(g->st, e.obs1_sid);
    if (unbind_name && (!nmc || !name_unbind(g, nmc, nl))) { free(old.v); free(nmc); return 0; }
    free(nmc);
    type_del(g, e.type_sid, node);
    u8 *pg = seg_txn_touch(g->gs, EID_LPG(eid));
    if (!pg || !seg_page_delete(pg, (u16)EID_SLOT(eid))) { free(old.v); return 0; }
    g->ent_count--;
    ind_kill(g, node);                             /* id retired: never reused,
                                                    * stale refs resolve dead  */
    meta_store(g);
    for (u32 i = 0; i < old.n; i++) {
        u8 k[7];
        tri_key(old.v[i], node, k);
        seg_tree_delete(&g->trit, k, 7);
    }
    if (old.n) cat_put_root(g, K_TRI, 2, g->trit.root);
    free(old.v);
    return 1;
}

int g4_delete_entity(graph4_t *g, u32 node) { return ent_remove(g, node, 1); }
int g4_entity_retire(graph4_t *g, u32 node) { return ent_remove(g, node, 0); }

typedef struct { u32 *out; u32 n, max; } list_ctx_t;
static int list_cb(void *c, u32 eid) {
    list_ctx_t *l = (list_ctx_t *)c;
    if (l->n < l->max) l->out[l->n] = eid;
    l->n++;
    return 1;
}
u32 g4_list_entities(graph4_t *g, u32 *out, u32 max) {
    list_ctx_t l = { out, 0, max };
    ni_walk(g, list_cb, &l);
    return l.n;                            /* true count (may exceed max) */
}

int g4_add_observation(graph4_t *g, u32 eid, const u8 *obs, u16 len, u64 mtime) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    if (e.obs_count >= 2) return 0;                       /* KB constraint */
    u32set_t old = {0};
    tri_collect_entity(g, &e, &old);
    u32 sid = st4_intern(g->st, obs, len);
    if (!sid) { free(old.v); return 0; }
    /* v3 semantics: NO dup check — the same obs may occupy both slots */
    u8 *r = ent_rec_w(g, eid);
    if (!r) { st4_decref(g->st, sid); free(old.v); return 0; }
    if (e.obs_count == 0) g4st32(r + 32, sid);
    else                  g4st32(r + 36, sid);
    r[40] = (u8)(e.obs_count + 1);
    g4st64(r + 24, mtime);                               /* obs_mtime */
    g4st64(r + 16, mtime);                               /* mtime too (v3) */
    tri_apply(g, eid, old.v, old.n);
    free(old.v);
    return 1;
}

int g4_remove_observation(graph4_t *g, u32 eid, const u8 *obs, u16 len, u64 mtime) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u32 sid = st4_find(g->st, obs, len);
    if (!sid) return 0;
    int which = -1;
    if (e.obs_count >= 1 && e.obs0_sid == sid) which = 0;
    else if (e.obs_count >= 2 && e.obs1_sid == sid) which = 1;
    if (which < 0) return 0;
    u32set_t old = {0};
    tri_collect_entity(g, &e, &old);
    u8 *r = ent_rec_w(g, eid);
    if (!r) { free(old.v); return 0; }
    if (which == 0) {                                    /* shift obs1 down */
        g4st32(r + 32, e.obs_count == 2 ? e.obs1_sid : 0);
        g4st32(r + 36, 0);
    } else {
        g4st32(r + 36, 0);
    }
    r[40] = (u8)(e.obs_count - 1);
    g4st64(r + 24, mtime);
    g4st64(r + 16, mtime);                               /* mtime too (v3) */
    st4_decref(g->st, sid);
    tri_apply(g, eid, old.v, old.n);
    free(old.v);
    return 1;
}

/* ================= adjacency ================= */

#define ADJ_HDR       8u                    /* [u32 count][u32 next_ref] */
#define ADJ_ENT       20u                   /* target, rel_sid, mtime, dir */
#define ADJ_MAX_ENT   ((SEG_PAGE_MAX_REC - ADJ_HDR) / ADJ_ENT)   /* 203 */
#define ADJ_SPLIT     128u                  /* new head past this many */

#define AREF_LPG(r)   (((r) - 1u) >> 12)
#define AREF_SLOT(r)  (((r) - 1u) & 0xFFFu)
#define AREF_MAKE(l, s) ((((u32)(l) << 12) | (u32)(s)) + 1u)

typedef struct { u32 count, next; const u8 *ents; } adj_view_t;

static int adj_view(graph4_t *g, u32 aref, adj_view_t *v) {
    if (aref == 0) return 0;
    const u8 *pg = seg_txn_view(g->gs, AREF_LPG(aref));
    if (!pg) return 0;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, (u16)AREF_SLOT(aref), &sz);
    if (!r || sz < ADJ_HDR) return 0;
    v->count = g4ld32(r);
    v->next  = g4ld32(r + 4);
    if ((u32)sz != ADJ_HDR + v->count * ADJ_ENT) return 0;
    v->ents = r + ADJ_HDR;
    return 1;
}

static void adj_ent_decode(const u8 *p, g4_edge_t *e) {
    e->target_eid = g4ld32(p);
    e->rel_sid    = g4ld32(p + 4);
    e->mtime      = g4ld64(p + 8);
    e->direction  = g4ld32(p + 16);
}

static void adj_ent_encode(u8 *p, const g4_edge_t *e) {
    g4st32(p, e->target_eid);
    g4st32(p + 4, e->rel_sid);
    g4st64(p + 8, e->mtime);
    g4st32(p + 16, e->direction);
}

static u32 adj_new(graph4_t *g, const u8 *img, u16 sz);

static int ent_set_adj_ref(graph4_t *g, u32 eid, u32 aref) {
    u8 *r = ent_rec_w(g, eid);
    if (!r) return 0;
    g4st32(r + 12, aref);
    return 1;
}

/* rewrite record `aref` with a new image (grow/shrink via update; on grow
 * failure relocate to another page and return the NEW aref, else same). */
static u32 adj_write(graph4_t *g, u32 aref, const u8 *img, u16 sz) {
    u8 *pg = seg_txn_touch(g->gs, AREF_LPG(aref));
    if (!pg) return 0;
    if (seg_page_update(pg, (u16)AREF_SLOT(aref), img, sz))
        return aref;
    /* relocate: delete here, insert with affinity */
    if (!seg_page_delete(pg, (u16)AREF_SLOT(aref))) return 0;
    return adj_new(g, img, sz);
}

/* fresh record with page affinity; returns aref or 0 */
static u32 adj_new(graph4_t *g, const u8 *img, u16 sz) {
    u32 lpg = g->last_adj_page; u16 slot;
    u8 *pg = (lpg != (u32)SEG_PT_NONE) ? seg_txn_touch(g->gs, lpg) : NULL;
    if (pg && seg_page_insert(pg, img, sz, &slot))
        return AREF_MAKE(lpg, slot);
    pg = seg_txn_alloc(g->gs, SEG_KIND_ADJ, &lpg);
    if (!pg || lpg >= G4_MAX_LPG || !seg_page_insert(pg, img, sz, &slot)) return 0;
    g->last_adj_page = lpg;
    return AREF_MAKE(lpg, slot);
}

/* add an entry at the HEAD record of eid's chain (new head on overflow) */
static int adj_add(graph4_t *g, u32 eid, const g4_edge_t *edge) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u8 img[ADJ_HDR + (ADJ_SPLIT + 1) * ADJ_ENT];
    adj_view_t v;
    if (e.adj_ref && adj_view(g, e.adj_ref, &v) && v.count < ADJ_SPLIT) {
        /* extend head in place (possibly relocating) */
        g4st32(img, v.count + 1);
        g4st32(img + 4, v.next);
        memcpy(img + ADJ_HDR, v.ents, v.count * ADJ_ENT);
        adj_ent_encode(img + ADJ_HDR + v.count * ADJ_ENT, edge);
        u32 nref = adj_write(g, e.adj_ref, img, (u16)(ADJ_HDR + (v.count + 1) * ADJ_ENT));
        if (!nref) return 0;
        if (nref != e.adj_ref && !ent_set_adj_ref(g, eid, nref)) return 0;
        return 1;
    }
    /* new head (first record, or head full): [1 entry][next = old head] */
    g4st32(img, 1);
    g4st32(img + 4, e.adj_ref);
    adj_ent_encode(img + ADJ_HDR, edge);
    u32 nref = adj_new(g, img, ADJ_HDR + ADJ_ENT);
    if (!nref) return 0;
    return ent_set_adj_ref(g, eid, nref);
}

/* remove first entry matching (target, rel_sid, dir); 1 = removed */
static int adj_remove(graph4_t *g, u32 eid, u32 target, u32 rel_sid, u32 dir) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u32 prev = 0, aref = e.adj_ref;
    u8 img[ADJ_HDR + ADJ_MAX_ENT * ADJ_ENT];
    while (aref) {
        adj_view_t v;
        if (!adj_view(g, aref, &v)) return 0;
        for (u32 i = 0; i < v.count; i++) {
            g4_edge_t ed;
            adj_ent_decode(v.ents + i * ADJ_ENT, &ed);
            if (ed.target_eid != target || ed.rel_sid != rel_sid || ed.direction != dir)
                continue;
            /* shard-seam note §5: record the deletion in the chain's
             * remove-watermark (max deleted mtime) BEFORE removing the row —
             * replication propagates the delete from this, and a re-applied
             * older add can never resurrect it. */
            {
                u8 *ww = ent_rec_w(g, eid);
                if (ww) {
                    u64 wm = g4ld64(ww + 68);
                    if (ed.mtime > wm) g4st64(ww + 68, ed.mtime);
                }
            }
            /* swap-remove within this record */
            u32 nc = v.count - 1;
            g4st32(img, nc);
            g4st32(img + 4, v.next);
            memcpy(img + ADJ_HDR, v.ents, v.count * ADJ_ENT);
            if (i != nc)
                memcpy(img + ADJ_HDR + i * ADJ_ENT,
                       img + ADJ_HDR + nc * ADJ_ENT, ADJ_ENT);
            if (nc == 0) {
                /* record empty: unlink it */
                u8 *pg = seg_txn_touch(g->gs, AREF_LPG(aref));
                if (!pg || !seg_page_delete(pg, (u16)AREF_SLOT(aref))) return 0;
                if (prev) {
                    adj_view_t pv;
                    if (!adj_view(g, prev, &pv)) return 0;
                    u8 pimg[ADJ_HDR + ADJ_MAX_ENT * ADJ_ENT];
                    g4st32(pimg, pv.count);
                    g4st32(pimg + 4, v.next);              /* skip over */
                    memcpy(pimg + ADJ_HDR, pv.ents, pv.count * ADJ_ENT);
                    u32 nref = adj_write(g, prev, pimg,
                                         (u16)(ADJ_HDR + pv.count * ADJ_ENT));
                    if (!nref) return 0;
                    if (nref != prev) {
                        /* prev relocated: fix ITS referrer (entity or prev-prev).
                         * prev is always the head's predecessor path — simplest
                         * sound approach: re-walk from entity is overkill; the
                         * same-size update NEVER relocates (no grow), so this
                         * branch is unreachable. Guard anyway. */
                        return 0;
                    }
                } else if (!ent_set_adj_ref(g, eid, v.next)) return 0;
                return 1;
            }
            u32 nref = adj_write(g, aref, img, (u16)(ADJ_HDR + nc * ADJ_ENT));
            if (!nref) return 0;                           /* shrink: no relocate */
            if (nref != aref) return 0;                    /* unreachable */
            return 1;
        }
        prev = aref;
        aref = v.next;
    }
    return 0;
}

static u32 adj_find(graph4_t *g, u32 eid, u32 target, u32 rel_sid, u32 dir,
                    g4_edge_t *out) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u32 aref = e.adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(g, aref, &v)) return 0;
        for (u32 i = 0; i < v.count; i++) {
            g4_edge_t ed;
            adj_ent_decode(v.ents + i * ADJ_ENT, &ed);
            if (ed.target_eid == target && ed.rel_sid == rel_sid && ed.direction == dir) {
                if (out) *out = ed;
                return 1;
            }
        }
        aref = v.next;
    }
    return 0;
}

u32 g4_edge_count(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u32 n = 0, aref = e.adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(g, aref, &v)) return n;
        n += v.count;
        aref = v.next;
    }
    return n;
}

u32 g4_edges(graph4_t *g, u32 eid, g4_edge_t *out, u32 max) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u32 n = 0, aref = e.adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(g, aref, &v)) break;
        for (u32 i = 0; i < v.count; i++, n++)
            if (n < max) adj_ent_decode(v.ents + i * ADJ_ENT, &out[n]);
        aref = v.next;
    }
    return n;
}

int g4_create_relation(graph4_t *g, u32 from, u32 to,
                       const u8 *rt, u16 rtlen, u64 mtime) {
    g4_entity_t ef, et;
    if (!g4_read_entity(g, from, &ef) || !g4_read_entity(g, to, &et)) return 0;
    u32 existing_sid = st4_find(g->st, rt, rtlen);
    if (existing_sid && adj_find(g, from, to, existing_sid, G4_DIR_FORWARD, NULL))
        return 0;                                          /* duplicate edge */
    u32 sid = st4_intern(g->st, rt, rtlen);                /* ref for fwd entry */
    if (!sid) return 0;
    if (!st4_incref(g->st, sid)) { st4_decref(g->st, sid); return 0; }  /* ref for mirror */
    g4_edge_t fwd = { to,   sid, mtime, G4_DIR_FORWARD };
    g4_edge_t bwd = { from, sid, mtime, G4_DIR_BACKWARD };
    if (!adj_add(g, from, &fwd)) { st4_decref(g->st, sid); st4_decref(g->st, sid); return 0; }
    if (!adj_add(g, to, &bwd))   { adj_remove(g, from, to, sid, G4_DIR_FORWARD);
                                   st4_decref(g->st, sid); st4_decref(g->st, sid); return 0; }
    {   /* v3: a new relation marks the SOURCE entity modified */
        u8 *r = ent_rec_w(g, from);
        if (r) g4st64(r + 16, mtime);
    }
    return 1;
}

int g4_delete_relation(graph4_t *g, u32 from, u32 to, const u8 *rt, u16 rtlen) {
    u32 sid = st4_find(g->st, rt, rtlen);
    if (!sid) return 0;
    if (!adj_remove(g, from, to, sid, G4_DIR_FORWARD)) return 0;
    if (!adj_remove(g, to, from, sid, G4_DIR_BACKWARD)) return 0;
    st4_decref(g->st, sid);
    st4_decref(g->st, sid);
    return 1;
}

/* delete-entity support: drop every incident edge. For each own entry,
 * remove the mirror on the peer and release both rel refs; then free the
 * whole own chain. */
static int adj_clear_all(graph4_t *g, u32 eid, const g4_entity_t *e) {
    u32 aref = e->adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(g, aref, &v)) return 0;
        /* copy entries out: mirror removal mutates pages under us */
        u32 cnt = v.count, next = v.next;
        g4_edge_t *eds = (g4_edge_t *)malloc((size_t)(cnt ? cnt : 1) * sizeof *eds);
        if (!eds) return 0;
        for (u32 i = 0; i < cnt; i++) adj_ent_decode(v.ents + i * ADJ_ENT, &eds[i]);
        for (u32 i = 0; i < cnt; i++) {
            u32 mdir = eds[i].direction == G4_DIR_FORWARD ? G4_DIR_BACKWARD
                                                          : G4_DIR_FORWARD;
            if (eds[i].target_eid != eid)                  /* self-loop mirror lives here */
                adj_remove(g, eds[i].target_eid, eid, eds[i].rel_sid, mdir);
            st4_decref(g->st, eds[i].rel_sid);
        }
        free(eds);
        u8 *pg = seg_txn_touch(g->gs, AREF_LPG(aref));
        if (!pg || !seg_page_delete(pg, (u16)AREF_SLOT(aref))) return 0;
        aref = next;
    }
    return ent_set_adj_ref(g, eid, 0);
}

/* ================= traversal ================= */

static int g4push(u32 **a, u32 *n, u32 *cap, u32 v) {
    if (*n == *cap) {
        u32 nc = *cap ? *cap * 2 : 64;
        u32 *na = (u32 *)realloc(*a, (size_t)nc * 4);
        if (!na) return 0;
        *a = na; *cap = nc;
    }
    (*a)[(*n)++] = v;
    return 1;
}

static inline int g4_dir_match(u32 want, u32 have) {
    return want == G4_DIR_ANY || have == want;
}

/* open-addressing eid -> parent map (parent 0 = root/none; eid never 0) */
typedef struct { u32 *keys, *par; u32 cap, n; } pmap_t;

static int pmap_init(pmap_t *m, u32 cap0) {
    m->cap = 64; while (m->cap < cap0 * 2) m->cap *= 2;
    m->keys = (u32 *)calloc(m->cap, 4);
    m->par  = (u32 *)calloc(m->cap, 4);
    m->n = 0;
    return m->keys && m->par;
}
static void pmap_free(pmap_t *m) { free(m->keys); free(m->par); }
static u32 pmap_slot(const pmap_t *m, u32 eid) {
    u32 i = (u32)(((u64)eid * 0x9E3779B97F4A7C15ull) >> 32) & (m->cap - 1);
    while (m->keys[i] && m->keys[i] != eid) i = (i + 1) & (m->cap - 1);
    return i;
}
static int pmap_has(const pmap_t *m, u32 eid) { return m->keys[pmap_slot(m, eid)] == eid; }
static u32 pmap_get(const pmap_t *m, u32 eid) { return m->par[pmap_slot(m, eid)]; }
static int pmap_grow(pmap_t *m);
static int pmap_put(pmap_t *m, u32 eid, u32 parent) {
    if ((m->n + 1) * 4 >= m->cap * 3) if (!pmap_grow(m)) return 0;
    u32 i = pmap_slot(m, eid);
    if (m->keys[i] == eid) return 1;          /* keep first parent (BFS) */
    m->keys[i] = eid; m->par[i] = parent; m->n++;
    return 1;
}
static int pmap_grow(pmap_t *m) {
    pmap_t nm;
    nm.cap = m->cap * 2;
    nm.keys = (u32 *)calloc(nm.cap, 4);
    nm.par  = (u32 *)calloc(nm.cap, 4);
    if (!nm.keys || !nm.par) { free(nm.keys); free(nm.par); return 0; }
    nm.n = 0;
    for (u32 i = 0; i < m->cap; i++)
        if (m->keys[i]) {
            u32 j = pmap_slot(&nm, m->keys[i]);
            nm.keys[j] = m->keys[i]; nm.par[j] = m->par[i]; nm.n++;
        }
    pmap_free(m);
    *m = nm;
    return 1;
}

/* expand one node's edges through the filter, calling visit(target) */
#define G4_EXPAND(g, eid, want, TARGET, BODY) do {                           \
    g4_entity_t _e;                                                          \
    if (g4_read_entity((g), (eid), &_e)) {                                   \
        u32 _aref = _e.adj_ref;                                              \
        while (_aref) {                                                      \
            adj_view_t _v;                                                   \
            if (!adj_view((g), _aref, &_v)) break;                           \
            for (u32 _i = 0; _i < _v.count; _i++) {                          \
                g4_edge_t _ed;                                               \
                adj_ent_decode(_v.ents + _i * ADJ_ENT, &_ed);                \
                if (!g4_dir_match((want), _ed.direction)) continue;          \
                u32 TARGET = _ed.target_eid;                                 \
                BODY                                                         \
            }                                                                \
            _aref = _v.next;                                                 \
        }                                                                    \
    }                                                                        \
} while (0)

u32 g4_neighbors(graph4_t *g, u32 start, u32 depth, u32 direction,
                 u32 *out, u32 max) {
    g4_entity_t e;
    if (depth == 0 || !g4_read_entity(g, start, &e)) return 0;

    if (depth == 1) {                          /* d0-public fast path */
        /* small-degree: stack dedup; larger falls through to BFS below */
        enum { FAST = 128 };
        u32 buf[FAST]; u32 n = 0; int spill = 0;
        G4_EXPAND(g, start, direction, t, {
            if (t == start) continue;
            int dup = 0;
            for (u32 k = 0; k < n; k++) if (buf[k] == t) { dup = 1; break; }
            if (dup) continue;
            if (n == FAST) { spill = 1; break; }
            buf[n++] = t;
        });
        if (!spill) {
            for (u32 k = 0; k < n && k < max; k++) out[k] = buf[k];
            return n;
        }
    }

    pmap_t seen;
    if (!pmap_init(&seen, 256)) return 0;
    u32 *frontier = (u32 *)malloc(4), fcnt = 1, fcap = 1;
    if (!frontier) { pmap_free(&seen); return 0; }
    frontier[0] = start;
    pmap_put(&seen, start, 0);
    u32 total = 0;
    for (u32 d = 0; d < depth && fcnt; d++) {
        u32 *next = NULL, ncnt = 0, ncap = 0;
        for (u32 f = 0; f < fcnt; f++) {
            G4_EXPAND(g, frontier[f], direction, t, {
                if (pmap_has(&seen, t)) continue;
                if (!pmap_put(&seen, t, frontier[f])) goto oom;
                if (total < max) out[total] = t;
                total++;
                if (!g4push(&next, &ncnt, &ncap, t)) goto oom;
            });
        }
        free(frontier);
        frontier = next; fcnt = ncnt; fcap = ncap;
        continue;
    oom:
        free(next); free(frontier); pmap_free(&seen);
        return total;
    }
    (void)fcap;
    free(frontier);
    pmap_free(&seen);
    return total;
}

/* bidirectional level-sync BFS (Finding_BidirectionalBFS): expand the
 * smaller frontier; reverse side uses the inverted filter. */
u32 g4_find_path(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                 u32 *out_path, u32 max_path) {
    g4_entity_t e;
    if (!g4_read_entity(g, from, &e) || !g4_read_entity(g, to, &e)) return 0;
    if (from == to) {
        if (max_path >= 1) out_path[0] = from;
        return 1;
    }
    u32 rev_dir = direction == G4_DIR_ANY ? G4_DIR_ANY
                : direction == G4_DIR_FORWARD ? G4_DIR_BACKWARD : G4_DIR_FORWARD;

    pmap_t sf, sb;                              /* seen-from / seen-back */
    if (!pmap_init(&sf, 64)) return 0;
    if (!pmap_init(&sb, 64)) { pmap_free(&sf); return 0; }
    pmap_put(&sf, from, 0);
    pmap_put(&sb, to, 0);

    u32 *ff = (u32 *)malloc(4), ffn = 1, ffc = 1;
    u32 *bf = (u32 *)malloc(4), bfn = 1, bfc = 1;
    u32 result = 0, meet = 0;
    if (!ff || !bf) goto done;
    ff[0] = from; bf[0] = to;

    for (u32 lvl = 0; lvl < max_depth && ffn && bfn && !meet; lvl++) {
        int fwd_side = ffn <= bfn;              /* expand the smaller side */
        u32 *cur = fwd_side ? ff : bf;
        u32 cn = fwd_side ? ffn : bfn;
        pmap_t *own = fwd_side ? &sf : &sb;
        pmap_t *other = fwd_side ? &sb : &sf;
        u32 want = fwd_side ? direction : rev_dir;
        u32 *next = NULL, ncnt = 0, ncap = 0;
        for (u32 f = 0; f < cn && !meet; f++) {
            G4_EXPAND(g, cur[f], want, t, {
                if (pmap_has(own, t)) continue;
                if (!pmap_put(own, t, cur[f])) goto pdone;
                if (pmap_has(other, t)) { meet = t; break; }
                if (!g4push(&next, &ncnt, &ncap, t)) goto pdone;
            });
        }
        free(cur);
        if (fwd_side) { ff = next; ffn = ncnt; ffc = ncap; }
        else          { bf = next; bfn = ncnt; bfc = ncap; }
        continue;
    pdone:
        free(next);
        if (fwd_side) { ff = NULL; ffn = 0; } else { bf = NULL; bfn = 0; }
        goto done;
    }
    (void)ffc; (void)bfc;

    if (meet) {
        /* reconstruct: from ... meet via sf parents, meet ... to via sb */
        u32 tmp[512]; u32 nfrom = 0;
        for (u32 x = meet; x; x = pmap_get(&sf, x)) {
            if (nfrom >= 512) goto done;
            tmp[nfrom++] = x;
        }
        u32 n = 0;
        for (u32 i = nfrom; i-- > 0; ) {        /* from..meet in order */
            if (n < max_path) out_path[n] = tmp[i];
            n++;
        }
        for (u32 x = pmap_get(&sb, meet); x; x = pmap_get(&sb, x)) {
            if (n < max_path) out_path[n] = x;
            n++;
        }
        result = n;
    }
done:
    free(ff); free(bf);
    pmap_free(&sf); pmap_free(&sb);
    return result;
}

/* Unidirectional BFS shortest path with a best-effort fallback + byte budget —
 * the β-contract (Decision_FindPathBetaContractInC), ported from v3's
 * graph_find_path_ex; the Jest find_path suite is the behavioral contract.
 *
 *  - target_reached: BFS arrived at `to`; out_path = from..to.
 *  - else best-effort: out_path = from..farthest (the LAST node discovered,
 *    BFS order) so the caller has a retry anchor; farthest = 0 if no edge
 *    was expanded at all.
 *  - budget_exhausted: stopped because per-node bytes crossed the stop
 *    threshold. The target check fires BEFORE the budget check, so a
 *    discovery that IS the target succeeds even at budget 0. (u64)-1 =
 *    untracked.
 *  - Per-node cost = name bytes + rel-type bytes + 28 (C BFS bookkeeping) —
 *    the same mechanism/sizing as v3, not V8 layout constants.
 *
 * g4_find_path_ex2 is the core engine shared with continuations
 * (v1.5/leases): `replay_until` = the cumulative byte value at which a PRIOR
 * run stopped (0 = fresh). Replay is exact because a validated store txid
 * freezes the graph AND the discovery order; the stop threshold becomes
 * `replay_until + budget` and every node below it is adopted — including
 * prior cut nodes, which a resumed run must expand (progress). `cut_out`
 * reports the trip value so it can become the next continuation's
 * replay_until. Payloads stay O(1): a descriptor, not a serialized frontier.
 *
 * Returns the emitted path's node count.
 */
u32 g4_find_path_ex2(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                     u64 budget_bytes, u64 replay_until,
                     u32 *out_path, u32 max_path,
                     int *target_reached, int *budget_exhausted, u32 *farthest,
                     u64 *cut_out) {
    *target_reached = 0; *budget_exhausted = 0; *farthest = 0;
    if (cut_out) *cut_out = 0;
    g4_entity_t e;
    if (!g4_read_entity(g, from, &e) || !g4_read_entity(g, to, &e)) return 0;
    if (from == to) { if (max_path >= 1) out_path[0] = from; *target_reached = 1; return 1; }

    pmap_t parent;
    if (!pmap_init(&parent, 256)) return 0;
    if (!pmap_put(&parent, from, 0)) { pmap_free(&parent); return 0; }  /* 0 = root sentinel */

    u32 qcap = 256, head = 0, tail = 0;
    u32 *q  = (u32 *)malloc((size_t)qcap * 4);
    u32 *qd = (u32 *)malloc((size_t)qcap * 4);
    if (!q || !qd) { free(q); free(qd); pmap_free(&parent); return 0; }
    q[tail] = from; qd[tail] = 0; tail++;

    int track = (budget_bytes != (u64)-1);
    u64 stop_at = 0;
    if (track)
        stop_at = (replay_until > (u64)-1 - budget_bytes)
                ? (u64)-1 : replay_until + budget_bytes;
    u64 bytes_used = 0;
    if (track) {
        u16 fl = 0;
        const u8 *fb = st4_get(g->st, e.name_sid, &fl);
        bytes_used = (u64)(fb ? fl : 0) + 28;
    }
    int found = 0, exhausted = 0;

    while (head < tail && !found && !exhausted) {
        u32 f = q[head]; u32 d = qd[head]; head++;
        if (d >= max_depth) continue;
        u32 ec = g4_edge_count(g, f);
        if (!ec) continue;
        g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
        if (!es) break;
        g4_edges(g, f, es, ec);
        for (u32 k = 0; k < ec; k++) {
            if (!g4_dir_match(direction, es[k].direction)) continue;
            u32 t = es[k].target_eid;
            if (pmap_has(&parent, t)) continue;
            if (!pmap_put(&parent, t, f)) { free(es); goto done; }
            *farthest = t;
            if (t == to) { found = 1; break; }              /* target check first */
            if (track) {                                    /* then the budget */
                u16 nl = 0, rl = 0;
                g4_entity_t te;
                if (g4_read_entity(g, t, &te)) { const u8 *b = st4_get(g->st, te.name_sid, &nl); (void)b; }
                const u8 *rb = st4_get(g->st, es[k].rel_sid, &rl); (void)rb;
                bytes_used += (u64)nl + (u64)rl + 28;
                if (bytes_used >= stop_at) {
                    exhausted = 1;
                    if (cut_out) *cut_out = bytes_used;
                    break;
                }
            }
            if (tail == qcap) {
                u32 nc = qcap * 2;
                u32 *nq = (u32 *)realloc(q, (size_t)nc * 4);
                if (!nq) { free(es); free(q); free(qd); q = qd = NULL; goto done; }
                q = nq;
                u32 *nqd = (u32 *)realloc(qd, (size_t)nc * 4);
                if (!nqd) { free(es); free(q); free(qd); q = qd = NULL; goto done; }
                qd = nqd; qcap = nc;
            }
            q[tail] = t; qd[tail] = d + 1; tail++;
        }
        free(es);
    }

done:
    {
        u32 n = 0;
        u32 endp = found ? to : *farthest;
        if (endp) {
            u32 *rev = (u32 *)malloc((size_t)max_path * 4);
            if (rev) {
                for (u32 cur = endp; cur != from && n < max_path; cur = pmap_get(&parent, cur))
                    rev[n++] = cur;
                if (n < max_path) rev[n++] = from;
                for (u32 i = 0; i < n; i++) out_path[i] = rev[n - 1 - i];
                free(rev);
            }
        }
        free(q); free(qd); pmap_free(&parent);
        *target_reached = found; *budget_exhausted = exhausted;
        return n;
    }
}

/* Fresh search (no continuation): ex2 with replay_until = 0. */
u32 g4_find_path_ex(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                    u64 budget_bytes, u32 *out_path, u32 max_path,
                    int *target_reached, int *budget_exhausted, u32 *farthest) {
    return g4_find_path_ex2(g, from, to, max_depth, direction, budget_bytes, 0,
                            out_path, max_path, target_reached, budget_exhausted,
                            farthest, NULL);
}

/* ================= indexes + search ================= */

#define G4_DFA_MIN_VERIFY 128u

/* The trigram/type/name indexes are PERSISTENT seg_trees maintained
 * immediately, in-txn (docs/v4-index-design-note.md). There is nothing to
 * lazily sync; this stub remains for benchmark/tooling compatibility. */
void g4_index_sync(graph4_t *g) { (void)g; }

u32 g4_entities_by_type(graph4_t *g, const u8 *type, u16 tlen, u32 *out, u32 max) {
    u32 sid = st4_find(g->st, type, tlen);
    if (!sid) return 0;
    u8 lo[8], hi[8];
    type_key(sid, 0, lo);
    hi[0] = lo[0]; hi[1] = lo[1]; hi[2] = lo[2]; hi[3] = lo[3];
    hi[4] = 0xFF; hi[5] = 0xFF; hi[6] = 0xFF; hi[7] = 0xFF;
    eid_scan_t s = { out, 0, max };
    seg_tree_scan(&g->typet, lo, 8, 1, hi, 8, 1, eid_scan_cb, &s);
    return s.n;
}

typedef struct { u32 *out; u32 n, max; u32 last; int have; } tsid_ctx_t;
static int tsid_cb(void *c, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)v; (void)vl;
    tsid_ctx_t *s = (tsid_ctx_t *)c;
    if (kl != 8) return 1;
    u32 sid = (u32)k[0] | ((u32)k[1] << 8) | ((u32)k[2] << 16) | ((u32)k[3] << 24);
    if (!s->have || sid != s->last) {
        if (s->n < s->max) s->out[s->n] = sid;
        s->n++;
        s->last = sid;
        s->have = 1;
    }
    return 1;
}
u32 g4_entity_types(graph4_t *g, u32 *out_sids, u32 max) {
    /* distinct prefixes of the type table, in key (sid-bytes) order */
    tsid_ctx_t s = { out_sids, 0, max, 0, 0 };
    seg_tree_scan(&g->typet, NULL, 0, 1, NULL, 0, 1, tsid_cb, &s);
    return s.n;
}

typedef struct { graph4_t *g; u32 *set; u32 setcap, setn; } rts_ctx_t;
static int rts_cb(void *c, u32 eid) {
    rts_ctx_t *s = (rts_ctx_t *)c;
    g4_entity_t e;
    if (!g4_read_entity(s->g, eid, &e)) return 1;
    u32 aref = e.adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(s->g, aref, &v)) break;
        for (u32 k = 0; k < v.count; k++) {
            g4_edge_t ed;
            adj_ent_decode(v.ents + k * ADJ_ENT, &ed);
            if (ed.direction != G4_DIR_FORWARD) continue;
            if ((s->setn + 1) * 4 >= s->setcap * 3) {
                u32 nc = s->setcap * 2;
                u32 *nset = (u32 *)calloc(nc, 4);
                if (!nset) return 0;
                for (u32 x = 0; x < s->setcap; x++) if (s->set[x]) {
                    u32 ss = (u32)(((u64)s->set[x] * 0x9E3779B97F4A7C15ull) >> 32) & (nc - 1);
                    while (nset[ss]) ss = (ss + 1) & (nc - 1);
                    nset[ss] = s->set[x];
                }
                free(s->set); s->set = nset; s->setcap = nc;
            }
            u32 ss = (u32)(((u64)ed.rel_sid * 0x9E3779B97F4A7C15ull) >> 32) & (s->setcap - 1);
            while (s->set[ss] && s->set[ss] != ed.rel_sid) ss = (ss + 1) & (s->setcap - 1);
            if (!s->set[ss]) { s->set[ss] = ed.rel_sid; s->setn++; }
        }
        aref = v.next;
    }
    return 1;
}
u32 g4_relation_types(graph4_t *g, u32 *out_sids, u32 max) {
    /* O(E) sweep of FORWARD entries with dedup via a small open set */
    rts_ctx_t s;
    s.g = g; s.setn = 0; s.setcap = 256;
    s.set = (u32 *)calloc(s.setcap, 4);
    if (!s.set) return 0;
    ni_walk(g, rts_cb, &s);
    u32 n = 0;
    for (u32 i = 0; i < s.setcap; i++)
        if (s.set[i]) { if (n < max) out_sids[n] = s.set[i]; n++; }
    free(s.set);
    return n;
}

typedef struct { graph4_t *g; u32 *out; u32 n, max; } orph_ctx_t;
static int orph_cb(void *c, u32 eid) {
    orph_ctx_t *o = (orph_ctx_t *)c;
    g4_entity_t e;
    if (g4_read_entity(o->g, eid, &e) && e.adj_ref == 0) {
        if (o->n < o->max) o->out[o->n] = eid;
        o->n++;
    }
    return 1;
}
u32 g4_orphaned(graph4_t *g, u32 *out, u32 max) {
    orph_ctx_t o = { g, out, 0, max };
    ni_walk(g, orph_cb, &o);
    return o.n;
}

/* ---- search ---- */

int g4_regex_valid(const char *pattern) {
    const char *err = NULL;
    ReNode *ast = re_parse(pattern, &err);
    if (!ast) return 0;
    re_ast_free(ast);
    return 1;
}

static int g4_match_sid(graph4_t *g, const ReDfa *d, const Regex *re, u32 sid) {
    if (!sid) return 0;
    u16 len = 0;
    const u8 *b = st4_get(g->st, sid, &len);
    if (!b) return 0;
    return d ? re_dfa_search(d, (const char *)b, len)
             : re_nfa_search(re, (const char *)b, len);
}

static int g4_entity_matches(graph4_t *g, const ReDfa *d, const Regex *re, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    return g4_match_sid(g, d, re, e.name_sid)
        || g4_match_sid(g, d, re, e.type_sid)
        || (e.obs_count >= 1 && g4_match_sid(g, d, re, e.obs0_sid))
        || (e.obs_count >= 2 && g4_match_sid(g, d, re, e.obs1_sid));
}

typedef struct { graph4_t *g; ReDfa *d; const Regex *re; u32 *out; u32 max; u32 found; } search_ctx_t;
static int search_cb(void *c, u32 eid) {
    search_ctx_t *s = (search_ctx_t *)c;
    if (g4_entity_matches(s->g, s->d, s->re, eid)) {
        if (s->found < s->max) s->out[s->found] = eid;
        s->found++;
    }
    return 1;
}

u32 g4_search(graph4_t *g, const char *pattern, u32 *out, u32 max) {
    const char *err = NULL;
    ReNode *ast = re_parse(pattern, &err);
    if (!ast) return 0;
    Regex *re = re_compile_ast(ast);
    if (!re) { re_ast_free(ast); return 0; }

    /* Persistent trigram table: candidates straight from the tree; the query
     * never needs a lazily-built index (spec §7-era decoupling is retired). */
    ReTrigramQuery *q = re_trigram_build(ast);
    ReCandidates64 cand;
    cand.ids = NULL; cand.n = 0; cand.all = 1;
    if (q) cand = g4_candidates(g, q);

    u32 nverify = cand.all ? g->ent_count : cand.n;
    ReDfa *d = (nverify >= G4_DFA_MIN_VERIFY) ? re_dfa_build(re) : NULL;

    u32 found = 0;
    if (cand.all) {
        search_ctx_t sc = { g, d, re, out, max, 0 };
        ni_walk(g, search_cb, &sc);
        found = sc.found;
    } else {
        for (u32 i = 0; i < cand.n; i++) {
            u32 ei = (u32)cand.ids[i];
            if (g4_entity_matches(g, d, re, ei)) { if (found < max) out[found] = ei; found++; }
        }
    }
    re_candidates64_free(&cand);
    re_trigram_free(q);
    if (d) re_dfa_free(d);
    re_free(re);
    re_ast_free(ast);
    return found;
}

/* ================= rank + walks (v3-verbatim, eid-keyed) ================= */

static u64 g4_rng_state = 0x9e3779b97f4a7c15ull;
void g4_seed_rng(u64 seed) { g4_rng_state = seed ? seed : 0x9e3779b97f4a7c15ull; }
static inline u64 g4_rng_u64(u64 *s) { u64 x = *s; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return *s = x; }
static inline double g4_rng_d(u64 *s) { return (double)(g4_rng_u64(s) >> 11) * (1.0 / 9007199254740992.0); }

void g4_inc_structural_visit(graph4_t *g, u32 eid) {
    u8 *r = ent_rec_w(g, eid);
    if (!r) return;
    g4st64(r + 44, g4ld64(r + 44) + 1);
    g->structural_total++;
    meta_store(g);                       /* called inside write txns only */
}
void g4_inc_walker_visit(graph4_t *g, u32 eid) {
    u8 *r = ent_rec_w(g, eid);
    if (!r) return;
    g4st64(r + 52, g4ld64(r + 52) + 1);
    g->walker_total++;
    meta_store(g);
}
u64 g4_structural_total(graph4_t *g) { return g->structural_total; }
u64 g4_walker_total(graph4_t *g)     { return g->walker_total; }
double g4_structural_rank(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e) || !g->structural_total) return 0.0;
    return (double)e.structural_visits / (double)g->structural_total;
}
double g4_walker_rank(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e) || !g->walker_total) return 0.0;
    return (double)e.walker_visits / (double)g->walker_total;
}
double g4_get_psi(graph4_t *g, u32 eid) {
    g4_entity_t e;
    return g4_read_entity(g, eid, &e) ? e.psi : 0.0;
}

int g4_set_entity_fields(graph4_t *g, u32 eid, u64 mtime, u64 obs_mtime,
                         u64 svis, u64 wvis, double psi) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    u8 *r = ent_rec_w(g, eid);
    if (!r) return 0;
    g->structural_total = g->structural_total - e.structural_visits + svis;
    g->walker_total     = g->walker_total     - e.walker_visits     + wvis;
    g4st64(r + 16, mtime);
    g4st64(r + 24, obs_mtime);
    g4st64(r + 44, svis);
    g4st64(r + 52, wvis);
    memcpy(r + 60, &psi, 8);
    meta_store(g);                       /* totals are store state (ruling 4-i) */
    return 1;
}

/* Restore global totals verbatim (v3->v4 import): persisted in META, so the
 * source's all-time history — including orphaned visits of deleted entities —
 * carries exactly. */
void g4_set_totals(graph4_t *g, u64 structural_total, u64 walker_total) {
    g->structural_total = structural_total;
    g->walker_total = walker_total;
    meta_store(g);
}

typedef struct { graph4_t *g; u32 n; } rc_ctx_t;
static int rc_cb(void *c, u32 eid) {
    rc_ctx_t *s = (rc_ctx_t *)c;
    g4_entity_t e;
    if (!g4_read_entity(s->g, eid, &e)) return 1;
    u32 aref = e.adj_ref;
    while (aref) {
        adj_view_t v;
        if (!adj_view(s->g, aref, &v)) break;
        for (u32 k = 0; k < v.count; k++) {
            g4_edge_t ed;
            adj_ent_decode(v.ents + k * ADJ_ENT, &ed);
            if (ed.direction == G4_DIR_FORWARD) s->n++;
        }
        aref = v.next;
    }
    return 1;
}
u32 g4_relation_count(graph4_t *g) {
    rc_ctx_t s = { g, 0 };
    ni_walk(g, rc_cb, &s);
    return s.n;
}

static u32 g4_structural_walk(graph4_t *g, u32 start, double damping) {
    u32 cur = start, visits = 0;
    for (;;) {
        g4_inc_structural_visit(g, cur); visits++;
        u32 ec = g4_edge_count(g, cur);
        if (!ec) break;
        g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
        if (!es) break;
        g4_edges(g, cur, es, ec);
        u32 fwd = 0;
        for (u32 k = 0; k < ec; k++) if (es[k].direction == G4_DIR_FORWARD) fwd++;
        if (fwd == 0 || g4_rng_d(&g4_rng_state) >= damping) { free(es); break; }
        u32 pick = (u32)(g4_rng_d(&g4_rng_state) * fwd); if (pick >= fwd) pick = fwd - 1;
        u32 seen = 0, next = cur;
        for (u32 k = 0; k < ec; k++) if (es[k].direction == G4_DIR_FORWARD) {
            if (seen == pick) { next = es[k].target_eid; break; }
            seen++;
        }
        free(es);
        cur = next;
    }
    return visits;
}

u32 g4_structural_sample(graph4_t *g, u32 iterations, double damping) {
    u32 n = g->ent_count;
    if (n == 0) return 0;
    u32 *eids = (u32 *)malloc((size_t)n * 4);
    if (!eids) return 0;
    u32 got = g4_list_entities(g, eids, n);
    if (got > n) got = n;                  /* true-total return: clamp to the buffer */
    u32 total = 0;
    for (u32 it = 0; it < iterations; it++)
        for (u32 i = 0; i < got; i++) total += g4_structural_walk(g, eids[i], damping);
    free(eids);
    return total;
}

u32 g4_random_walk(graph4_t *g, u32 start, u32 depth, u32 direction,
                   int merw_mode, u64 seed, int avoid_cycles, u32 *out_path, u32 max_path,
                   u32 *out_uniform_steps) {
    g4_entity_t e0;
    if (!g4_read_entity(g, start, &e0)) return 0;
    u64 st = seed ? seed : g4_rng_state;
    u32 plen = 0;
    u32 uniform_steps = 0;
    if (max_path >= 1) out_path[plen] = start;
    plen = 1;
    u32 cur = start;
    for (u32 i = 0; i < depth; i++) {
        u32 ec = g4_edge_count(g, cur);
        if (!ec) break;
        g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
        if (!es) break;
        g4_edges(g, cur, es, ec);
        u32 *cand = (u32 *)malloc((size_t)ec * 4);
        double *cpsi = (double *)malloc((size_t)ec * 8);
        u32 nc = 0;
        if (!cand || !cpsi) { free(es); free(cand); free(cpsi); break; }
        for (u32 k = 0; k < ec; k++) {
            if (!g4_dir_match(direction, es[k].direction)) continue;
            u32 t = es[k].target_eid; if (t == cur) continue;
            if (avoid_cycles) {
                /* Self-avoiding walk: skip any node already on the path (the
                 * path prefix doubles as the visited set; max_path >= depth+1
                 * from callers, so the prefix is complete). All visited ->
                 * nc == 0 below -> stops early. */
                u32 vmax = plen < max_path ? plen : max_path;
                int seen = 0;
                for (u32 j = 0; j < vmax; j++) if (out_path[j] == t) { seen = 1; break; }
                if (seen) continue;
            }
            double p = g4_get_psi(g, t);
            int found = 0;
            for (u32 j = 0; j < nc; j++) if (cand[j] == t) { if (p > cpsi[j]) cpsi[j] = p; found = 1; break; }
            if (!found) { cand[nc] = t; cpsi[nc] = p; nc++; }
        }
        free(es);
        if (nc == 0) { free(cand); free(cpsi); break; }
        double total_psi = 0; for (u32 j = 0; j < nc; j++) total_psi += cpsi[j];
        u32 chosen;
        if (merw_mode && total_psi > 0) {
            double r = g4_rng_d(&st) * total_psi, cum = 0; chosen = cand[nc - 1];
            for (u32 j = 0; j < nc; j++) { cum += cpsi[j]; if (r <= cum) { chosen = cand[j]; break; } }
        } else {
            if (merw_mode) uniform_steps++;   /* psi unavailable at this step: fallback */
            u32 ix = (u32)(g4_rng_d(&st) * nc); if (ix >= nc) ix = nc - 1; chosen = cand[ix];
        }
        free(cand); free(cpsi);
        cur = chosen;
        if (plen < max_path) out_path[plen] = cur;
        plen++;
    }
    if (!seed) g4_rng_state = st;
    if (out_uniform_steps) *out_uniform_steps = uniform_steps;
    return plen;
}

/* eid -> dense-index map for the psi solver */
typedef struct { u32 *k; u32 *v; u32 cap; } e2i_t;
static u32 e2i_get(const e2i_t *m, u32 eid) {
    u32 i = (u32)(((u64)eid * 0x9E3779B97F4A7C15ull) >> 32) & (m->cap - 1);
    while (m->k[i] && m->k[i] != eid) i = (i + 1) & (m->cap - 1);
    return m->k[i] == eid ? m->v[i] : 0;
}
static void e2i_put(e2i_t *m, u32 eid, u32 val) {
    u32 i = (u32)(((u64)eid * 0x9E3779B97F4A7C15ull) >> 32) & (m->cap - 1);
    while (m->k[i] && m->k[i] != eid) i = (i + 1) & (m->cap - 1);
    m->k[i] = eid; m->v[i] = val;
}

u32 g4_compute_merw_psi(graph4_t *g, double alpha, u32 max_iter, double tol) {
    u32 n = g->ent_count;
    if (n == 0) return 0;
    u32 *eids = (u32 *)malloc((size_t)n * 4);
    if (!eids) return 0;
    u32 got = g4_list_entities(g, eids, n);
    if (got > n) got = n;                  /* true-total return: clamp to the buffer */
    n = got;

    e2i_t idx;
    idx.cap = 256; while (idx.cap < n * 2) idx.cap *= 2;
    idx.k = (u32 *)calloc(idx.cap, 4); idx.v = (u32 *)calloc(idx.cap, 4);
    if (!idx.k || !idx.v) { free(eids); free(idx.k); free(idx.v); return 0; }
    for (u32 i = 0; i < n; i++) e2i_put(&idx, eids[i], i + 1);

    /* CSR forward adjacency */
    u32 *rowoff = (u32 *)malloc((size_t)(n + 1) * 4);
    if (!rowoff) goto fail0;
    rowoff[0] = 0;
    for (u32 i = 0; i < n; i++) {
        u32 ec = g4_edge_count(g, eids[i]), d = 0;
        if (ec) {
            g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
            if (!es) { free(rowoff); goto fail0; }
            g4_edges(g, eids[i], es, ec);
            for (u32 k = 0; k < ec; k++)
                if (es[k].direction == G4_DIR_FORWARD && e2i_get(&idx, es[k].target_eid)) d++;
            free(es);
        }
        rowoff[i + 1] = rowoff[i] + d;
    }
    {
        u32 nnz = rowoff[n];
        u32 *col = (u32 *)malloc((size_t)(nnz ? nnz : 1) * 4);
        double *psi = (double *)malloc((size_t)n * 8);
        double *nx = (double *)malloc((size_t)n * 8);
        if (!col || !psi || !nx) { free(col); free(psi); free(nx); free(rowoff); goto fail0; }
        for (u32 i = 0; i < n; i++) {
            u32 ec = g4_edge_count(g, eids[i]);
            if (!ec) continue;
            g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
            if (!es) { free(col); free(psi); free(nx); free(rowoff); goto fail0; }
            g4_edges(g, eids[i], es, ec);
            u32 w = rowoff[i];
            for (u32 k = 0; k < ec; k++) if (es[k].direction == G4_DIR_FORWARD) {
                u32 j = e2i_get(&idx, es[k].target_eid);
                if (j) col[w++] = j - 1;
            }
            free(es);
        }
        double warm_sum = 0; u32 warm_cnt = 0;
        for (u32 i = 0; i < n; i++) {
            double v = g4_get_psi(g, eids[i]); psi[i] = v;
            if (v > 0) { warm_sum += v; warm_cnt++; }
        }
        if (warm_cnt) { double m = warm_sum / warm_cnt; for (u32 i = 0; i < n; i++) if (psi[i] <= 0) psi[i] = m; }
        else { double u = 1.0 / __builtin_sqrt((double)n); for (u32 i = 0; i < n; i++) psi[i] = u; }
        double nrm = 0; for (u32 i = 0; i < n; i++) nrm += psi[i] * psi[i]; nrm = __builtin_sqrt(nrm);
        if (nrm > 0) for (u32 i = 0; i < n; i++) psi[i] /= nrm;

        double teleport = (1.0 - alpha) / (double)n;
        u32 iter = 0;
        for (iter = 0; iter < max_iter; iter++) {
            for (u32 i = 0; i < n; i++) nx[i] = 0;
            double psi_sum = 0; for (u32 i = 0; i < n; i++) psi_sum += psi[i];
            double tc = teleport * psi_sum;
            for (u32 i = 0; i < n; i++) { double val = alpha * psi[i]; for (u32 p = rowoff[i]; p < rowoff[i + 1]; p++) nx[col[p]] += val; }
            for (u32 i = 0; i < n; i++) nx[i] += tc;
            double norm = 0; for (u32 i = 0; i < n; i++) norm += nx[i] * nx[i]; norm = __builtin_sqrt(norm);
            if (norm > 0) for (u32 i = 0; i < n; i++) nx[i] /= norm;
            double diff = 0; for (u32 i = 0; i < n; i++) { double d = nx[i] - psi[i]; diff += d * d; } diff = __builtin_sqrt(diff);
            double *t = psi; psi = nx; nx = t;
            if (diff < tol) { iter++; break; }
        }
        for (u32 i = 0; i < n; i++) {
            if (psi[i] < 0) psi[i] = 0;
            u8 *r = ent_rec_w(g, eids[i]);
            if (r) memcpy(r + 60, &psi[i], 8);
        }
        free(col); free(psi); free(nx); free(rowoff);
        free(eids); free(idx.k); free(idx.v);
        return iter;
    }
fail0:
    free(eids); free(idx.k); free(idx.v);
    return 0;
}

/* ================= anti-entropy symbol extraction =================
 * (docs/shard-seam-design-note.md §8; consumers: the repl test and, later,
 * the daemon's ANTI_ENTROPY op class). Must run inside a txn — the scan and
 * the record reads share one consistent view. */

typedef struct { graph4_t *g; void (*cb)(void *, const u8 *); void *ctx; u32 n; } sym_walk_t;

/* 64-bit content mix for the vertex-state symbol. Diff DETECTION only —
 * RIBLT's own checksum stays blake2s-keyed, so peeling correctness does not
 * depend on this hash. */
static u64 sym_mix64(const u8 *p, u32 n) {
    u64 h = 0x9E3779B97F4A7C15ull ^ ((u64)n << 32);
    for (u32 i = 0; i < n; i++) { h ^= p[i]; h *= 0x100000001B3ull; h ^= h >> 29; }
    h ^= h >> 30; h *= 0xBF58476D1CE4E5B9ull;
    h ^= h >> 27; h *= 0x94D049BB133111EBull;
    return h ^ (h >> 31);
}

/* continued mix over one more piece (order matters), for content hashes that
 * span several strings — BYTES only, never store-local sids, so two stores
 * with equal content emit equal symbols */
static u64 sym_mix64_acc(u64 h, const u8 *p, u32 n) {
    h ^= (u64)n << 32;
    for (u32 i = 0; i < n; i++) { h ^= p[i]; h *= 0x100000001B3ull; h ^= h >> 29; }
    h ^= h >> 30; h *= 0xBF58476D1CE4E5B9ull;
    h ^= h >> 27; h *= 0x94D049BB133111EBull;
    return h ^ (h >> 31);
}

static int sym_adj_cb(void *c, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)k; (void)kl;
    sym_walk_t *w = (sym_walk_t *)c;
    if (!v || vl != 8) return 1;
    u32 host = g4ld32(v);
    if (!host) return 1;                                /* tombstone */
    g4_entity_t e;
    if (!g4_read_entity(w->g, host, &e)) return 1;
    u32 aref = e.adj_ref;
    while (aref) {
        adj_view_t av;
        if (!adj_view(w->g, aref, &av)) break;
        for (u32 i = 0; i < av.count; i++) {
            g4_edge_t ed;
            adj_ent_decode(av.ents + (size_t)i * ADJ_ENT, &ed);
            /* CANONICAL edge symbol: both halves (host-side row and peer-side
             * mirror) collapse to one form, so a pair reconciliation can
             * detect and repair unpaired halves. A self-loop's BWD row is
             * represented by its FWD row (skip the duplicate). */
            if (ed.target_eid == host && ed.direction != G4_DIR_FORWARD) continue;
            u32 lo = host < ed.target_eid ? host : ed.target_eid;
            u32 hi = host < ed.target_eid ? ed.target_eid : host;
            u8 dlo = (host == lo) ? (u8)ed.direction : (u8)(1u - ed.direction);
            u16 rl = 0;
            const u8 *rb = st4_get(w->g->st, ed.rel_sid, &rl);
            if (!rb) continue;                          /* dead sid: not a fact */
            u8 s[G4_SYM_LEN];
            memset(s, 0, sizeof s);
            g4st32(s, lo);                              /* [lo u32]     */
            g4st32(s + 4, hi);                          /* [hi u32]     */
            g4st64(s + 8, sym_mix64(rb, rl));           /* [relhash u64]*/
            s[16] = dlo;                                /* [dlo u8]     */
            g4st64(s + 17, ed.mtime);                   /* [mtime u64]  */
            w->cb(w->ctx, s);
            w->n++;
        }
        aref = av.next;
    }
    return 1;
}

static int sym_vstate_cb(void *c, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    (void)k; (void)kl;
    sym_walk_t *w = (sym_walk_t *)c;
    if (!v || vl != 8) return 1;
    u32 node = g4ld32(v);
    if (!node) return 1;
    u32 gen = g4ld32(v + 4);
    g4_entity_t e;
    if (!g4_read_entity(w->g, node, &e)) return 1;
    /* content hash over STRING BYTES (sids are store-local; hashing them
     * made equal rows hash differently across stores — churn and a
     * never-zero fixpoint), then count/mtimes/psi; fixed order. */
    u16 lt = 0, l0 = 0, l1 = 0;
    const u8 *tb = st4_get(w->g->st, e.type_sid, &lt);
    if (!tb) return 1;
    const u8 *o0 = (e.obs_count >= 1 && e.obs0_sid) ? st4_get(w->g->st, e.obs0_sid, &l0) : NULL;
    const u8 *o1 = (e.obs_count >= 2 && e.obs1_sid) ? st4_get(w->g->st, e.obs1_sid, &l1) : NULL;
    u64 h = 0x9E3779B97F4A7C15ull;
    h = sym_mix64_acc(h, tb, lt);
    h = sym_mix64_acc(h, o0 ? o0 : (const u8 *)"", o0 ? l0 : 0);
    h = sym_mix64_acc(h, o1 ? o1 : (const u8 *)"", o1 ? l1 : 0);
    u8 tail[25];
    tail[0] = e.obs_count;
    g4st64(tail + 1, e.mtime);
    g4st64(tail + 9, e.obs_mtime);
    u64 psi_bits; memcpy(&psi_bits, &e.psi, 8);
    memcpy(tail + 17, &psi_bits, 8);
    h = sym_mix64_acc(h, tail, sizeof tail);
    u8 s[G4_SYM_LEN];
    memset(s, 0, sizeof s);
    g4st32(s, node);
    g4st32(s + 4, gen);
    g4st64(s + 8, h);
    w->cb(w->ctx, s);
    w->n++;
    return 1;
}

u32 g4_adj_symbols(graph4_t *g, void (*cb)(void *ctx, const u8 *sym), void *ctx) {
    sym_walk_t w = { g, cb, ctx, 0 };
    (void)seg_tree_scan(&g->namet, NULL, 0, 1, NULL, 0, 1, sym_adj_cb, &w);
    return w.n;
}

u32 g4_vstate_symbols(graph4_t *g, void (*cb)(void *ctx, const u8 *sym), void *ctx) {
    sym_walk_t w = { g, cb, ctx, 0 };
    (void)seg_tree_scan(&g->namet, NULL, 0, 1, NULL, 0, 1, sym_vstate_cb, &w);
    return w.n;
}

/* ================= replication primitives (seam §5, step 5a) ================= */

/* chain remove-watermark (0 = none recorded) */
u64 g4_chain_wm(graph4_t *g, u32 node) {
    g4_entity_t e;
    return g4_read_entity(g, node, &e) ? e.adj_wm : 0;
}

/* string-stable reltype hash — the adjacency symbol's identity for rel types
 * (sids are per-store; the hash is over the bytes and comparable everywhere) */
u64 g4_relhash(graph4_t *g, u32 rel_sid) {
    u16 l = 0;
    const u8 *s = st4_get(g->st, rel_sid, &l);
    return s ? sym_mix64(s, l) : 0;
}

/* one half of an edge, written idempotently (half-repair's apply path) */
int g4_half_put(graph4_t *g, u32 host, u32 peer, u32 rel_sid, u64 mtime, u32 dir_stored) {
    g4_entity_t he;
    if (!g4_read_entity(g, host, &he)) return 0;
    if (adj_find(g, host, peer, rel_sid, dir_stored, NULL)) return 1;   /* already there */
    if (!st4_incref(g->st, rel_sid)) return 0;
    g4_edge_t ed = { peer, rel_sid, mtime, dir_stored };
    if (!adj_add(g, host, &ed)) { st4_decref(g->st, rel_sid); return 0; }
    return 1;
}

/* one half removed; the chain's remove-watermark is captured inside
 * adj_remove, so this deletion can propagate (never resurrect) */
int g4_half_del(graph4_t *g, u32 host, u32 peer, u32 rel_sid, u32 dir_stored) {
    if (!adj_remove(g, host, peer, rel_sid, dir_stored)) return 0;
    st4_decref(g->st, rel_sid);
    return 1;
}

/* resolve a reltype hash on `from`'s FORWARD chain to its string (the holder
 * side of a wire fetch — no rmap needed); 1 = found; *outcap in/out. */
int g4_edge_find_hashed(graph4_t *g, u32 from, u32 to, u64 relhash, u8 *out, u16 *outcap) {
    u32 ec = g4_edge_count(g, from);
    if (!ec) return 0;
    g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
    if (!es) return 0;
    g4_edges(g, from, es, ec);
    int found = 0;
    for (u32 k = 0; k < ec && !found; k++) {
        if (es[k].target_eid != to || es[k].direction != G4_DIR_FORWARD) continue;
        if (g4_relhash(g, es[k].rel_sid) != relhash) continue;
        u16 l = 0;
        const u8 *s = g4_str(g, es[k].rel_sid, &l);
        if (s && l <= *outcap) { memcpy(out, s, l); *outcap = l; found = 1; }
    }
    free(es);
    return found;
}

/* delete the (from,to) edge whose reltype hashes to relhash (a wire delete at
 * the holder; the reltype resolves from the chain itself); 1 = deleted. */
int g4_edge_del_hashed(graph4_t *g, u32 lo, u32 hi, u8 dlo, u64 relhash) {
    u32 from = (dlo == G4_DIR_FORWARD) ? lo : hi;
    u32 to   = (dlo == G4_DIR_FORWARD) ? hi : lo;
    u32 ec = g4_edge_count(g, from);
    if (!ec) return 0;
    g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
    if (!es) return 0;
    g4_edges(g, from, es, ec);
    int del = 0;
    for (u32 k = 0; k < ec && !del; k++) {
        if (es[k].target_eid != to || es[k].direction != G4_DIR_FORWARD) continue;
        if (g4_relhash(g, es[k].rel_sid) != relhash) continue;
        u16 l = 0;
        const u8 *s = g4_str(g, es[k].rel_sid, &l);
        if (s) del = g4_delete_relation(g, from, to, s, l);
    }
    free(es);
    return del;
}

/* whole-row apply, raw strings (the wire form; seam §5/§8). Same body as the
 * store-to-store apply below; the winner's fields arrive as bytes. */
int g4_vstate_apply_raw(graph4_t *g, u32 node,
                        const u8 *type, u16 tl,
                        u64 mtime, u64 obs_mtime,
                        const u8 *o0, u16 o0l, const u8 *o1, u16 o1l,
                        u8 ocount, double psi) {
    g4_entity_t e;
    if (!g4_read_entity(g, node, &e)) return 0;
    u32 ntype = st4_intern(g->st, type, tl);
    if (!ntype) return 0;
    u32 nobs[2] = { 0, 0 };
    if (ocount >= 1 && o0l) {
        nobs[0] = st4_intern(g->st, o0, o0l);
        if (!nobs[0]) { st4_decref(g->st, ntype); return 0; }
    }
    if (ocount >= 2 && o1l) {
        nobs[1] = st4_intern(g->st, o1, o1l);
        if (!nobs[1]) { st4_decref(g->st, ntype); if (nobs[0]) st4_decref(g->st, nobs[0]); return 0; }
    }
    u32set_t old = {0};
    tri_collect_entity(g, &e, &old);
    u8 *r = ent_rec_w(g, node);
    if (!r) {
        free(old.v);
        st4_decref(g->st, ntype);
        if (nobs[0]) st4_decref(g->st, nobs[0]);
        if (nobs[1]) st4_decref(g->st, nobs[1]);
        return 0;
    }
    type_del(g, e.type_sid, node);                 /* index follows the apply */
    type_put(g, ntype, node);
    st4_decref(g->st, e.type_sid);
    g4st32(r + 8, ntype);
    if (e.obs_count >= 1 && e.obs0_sid) st4_decref(g->st, e.obs0_sid);
    if (e.obs_count >= 2 && e.obs1_sid) st4_decref(g->st, e.obs1_sid);
    g4st32(r + 32, nobs[0]);
    g4st32(r + 36, nobs[1]);
    r[40] = ocount;
    g4st64(r + 16, mtime);
    g4st64(r + 24, obs_mtime);
    u64 psi_bits; memcpy(&psi_bits, &psi, 8);
    memcpy(r + 60, &psi_bits, 8);       /* psi is stored native, like every writer */
    tri_apply(g, node, old.v, old.n);
    free(old.v);
    return 1;
}

/* whole-row apply from a source store (LWW winner side): strings translate
 * across stores; visits stay local (relaxed-counter class); type index and
 * trigram postings follow. */
int g4_vstate_apply(graph4_t *g, u32 node, graph4_t *src_g, const g4_entity_t *se) {
    u16 tl = 0, l0 = 0, l1 = 0;
    const u8 *ts = st4_get(src_g->st, se->type_sid, &tl);
    if (!ts) return 0;
    const u8 *o0 = (se->obs_count >= 1 && se->obs0_sid) ? st4_get(src_g->st, se->obs0_sid, &l0) : NULL;
    if (se->obs_count >= 1 && se->obs0_sid && !o0) return 0;
    const u8 *o1 = (se->obs_count >= 2 && se->obs1_sid) ? st4_get(src_g->st, se->obs1_sid, &l1) : NULL;
    if (se->obs_count >= 2 && se->obs1_sid && !o1) return 0;
    return g4_vstate_apply_raw(g, node, ts, tl, se->mtime, se->obs_mtime,
                               o0, l0, o1, l1, se->obs_count, se->psi);
}

/* ================= vstate row blob codec (repl wire; seam §8) ============= */

static void blob_put_str(u8 *out, u32 *off, const u8 *s, u16 l) {
    out[*off] = (u8)l; out[*off + 1] = (u8)(l >> 8); *off += 2;   /* u16 LE */
    if (l) memcpy(out + *off, s, l);
    *off += l;
}

static int blob_get_str(const u8 *b, u32 len, u32 *off, const u8 **s, u16 *l) {
    if (*off + 2 > len) return 0;
    u16 n = (u16)(b[*off] | (b[*off + 1] << 8)); *off += 2;
    if (*off + n > len) return 0;
    *s = b + *off; *l = n; *off += n;
    return 1;
}

u32 g4_vrow_pack(graph4_t *g, const g4_entity_t *e, u8 *out, u32 cap) {
    u32 off = 0;
    u16 tl = 0; const u8 *ts = g4_str(g, e->type_sid, &tl);
    if (!ts) return 0;
    if (cap < (u32)2 + tl + 8 + 8 + 1 + 8) return 0;
    blob_put_str(out, &off, ts, tl);
    g4st64(out + off, e->mtime); off += 8;
    g4st64(out + off, e->obs_mtime); off += 8;
    out[off++] = e->obs_count;
    for (int k = 0; k < 2; k++) {
        if ((u8)(k + 1) > e->obs_count) break;
        u32 sid = (k == 0) ? e->obs0_sid : e->obs1_sid;
        u16 l = 0;
        const u8 *s = sid ? g4_str(g, sid, &l) : NULL;
        if (s && cap < off + 2 + l) return 0;
        blob_put_str(out, &off, s, s ? l : 0);
    }
    if (cap < off + 8) return 0;
    u64 psi_bits; memcpy(&psi_bits, &e->psi, 8);
    g4st64(out + off, psi_bits); off += 8;
    return off;
}

/* parsed row-blob fields (one parser feeds cmp/apply/mirror) */
typedef struct {
    const u8 *type; u16 tl;
    u64 mtime, obs_mtime;
    u8 ocount;
    const u8 *obs[2]; u16 obslen[2];
    double psi;
} vf_t;

static int vrow_parse(const u8 *blob, u32 len, vf_t *f) {
    u32 off = 0;
    if (!blob_get_str(blob, len, &off, &f->type, &f->tl)) return 0;
    if (off + 8 + 8 + 1 > len) return 0;
    f->mtime = g4ld64(blob + off); off += 8;
    f->obs_mtime = g4ld64(blob + off); off += 8;
    f->ocount = blob[off++];
    f->obs[0] = f->obs[1] = NULL;
    f->obslen[0] = f->obslen[1] = 0;
    for (int k = 0; k < 2; k++) {
        if ((u8)(k + 1) > f->ocount) break;
        if (!blob_get_str(blob, len, &off, &f->obs[k], &f->obslen[k])) return 0;
    }
    if (off + 8 > len) return 0;
    u64 pb = g4ld64(blob + off);
    memcpy(&f->psi, &pb, 8);
    return 1;
}

int g4_vrow_cmp(graph4_t *g, const u8 *blob, u32 len, const g4_entity_t *e) {
    /* blob row ("a") vs live row ("b"): exact row_cmp order. Malformed
     * blob = NULL row: caller skips. */
    vf_t f;
    if (!vrow_parse(blob, len, &f)) return 0;
    if (f.mtime != e->mtime) return f.mtime < e->mtime ? -1 : 1;
    if (f.obs_mtime != e->obs_mtime) return f.obs_mtime < e->obs_mtime ? -1 : 1;
    if (f.ocount != e->obs_count) return f.ocount < e->obs_count ? -1 : 1;
    {
        u16 ll = 0;
        const u8 *tlive = g4_str(g, e->type_sid, &ll);
        if (!tlive) return 0;
        u16 m = f.tl < ll ? f.tl : ll;
        int c = m ? memcmp(f.type, tlive, m) : 0;
        if (c) return c < 0 ? -1 : 1;
        if (f.tl != ll) return f.tl < ll ? -1 : 1;
    }
    for (int k = 0; k < 2; k++) {
        u32 sid = (k == 0) ? e->obs0_sid : e->obs1_sid;
        int pb = sid && (u8)k < e->obs_count;
        int pa = (u8)(k + 1) <= f.ocount && f.obslen[k] > 0;
        if (!pa && !pb) continue;
        u16 ll = 0;
        const u8 *lb = pb ? g4_str(g, sid, &ll) : NULL;
        if (pb && !lb) return 0;
        const u8 *la = pa ? f.obs[k] : (const u8 *)"";
        u16 al = pa ? f.obslen[k] : 0;
        u16 lv = pb ? ll : 0;
        u16 m = al < lv ? al : lv;
        int c = m ? memcmp(la, lb, m) : 0;
        if (c) return c < 0 ? -1 : 1;
        if (al != lv) return al < lv ? -1 : 1;
    }
    u64 psi_b; memcpy(&psi_b, &e->psi, 8);
    u64 psi_a; memcpy(&psi_a, &f.psi, 8);
    if (psi_a != psi_b) return psi_a < psi_b ? -1 : 1;
    return 0;
}

int g4_vrow_apply(graph4_t *g, u32 node, const u8 *blob, u32 len) {
    vf_t f;
    if (!vrow_parse(blob, len, &f)) return 0;
    return g4_vstate_apply_raw(g, node, f.type, f.tl, f.mtime, f.obs_mtime,
                               f.obs[0], f.obslen[0], f.obs[1], f.obslen[1],
                               f.ocount, f.psi);
}

/* ================= mirroring (seam §4/§9, step 5b-ii) ================= */

u32 g4_set_next_node(graph4_t *g, u32 base) {
    if (base > g->next_node) { g->next_node = base; meta_store(g); }
    return g->next_node;
}

int g4_name_get(graph4_t *g, const u8 *name, u16 nl, u32 *node, u32 *gen) {
    return name_row(g, name, nl, node, gen);
}

/* the (gen, node) rule, one voice for every caller: incoming beats the local
 * row iff strictly greater lexicographically; ties pass for nothing here
 * (record-ensure is decided by g4_mirror_apply). On a replace that retires a
 * live local binding, that binding's record is RETIRED (no gen churn — the
 * incoming row IS the new state). node 0 = tombstone. 1 = applied. */
int g4_mirror_name(graph4_t *g, const u8 *name, u16 nl, u32 node, u32 gen) {
    u32 lnode = 0, lgen = 0;
    int have = name_row(g, name, nl, &lnode, &lgen);
    if (have) {
        if (gen < lgen || (gen == lgen && node <= lnode)) return 0;   /* lost/equal */
        if (lnode && lnode != node && !g4_entity_retire(g, lnode)) return 0;
    }
    return name_bind_raw(g, name, nl, node, gen);
}

/* indirection entry at an EXPLICIT id: materializes dir pages up to the id
 * (holes zero-filled — mirror ids arrive sparse), then pokes the eid. The
 * mint counter is bumped separately by the mirror path. */
static int ind_set(graph4_t *g, u32 node, u32 eid) {
    if (!node || !eid) return 0;
    u32 idx = node - 1u;
    u32 page = idx / IND_PAGE_SLOTS, slot = idx % IND_PAGE_SLOTS;
    if (!g->ind_root) {
        u32 dlpg = 0;
        u8 *dpg = seg_txn_alloc(g->gs, SEG_KIND_INDIRECT, &dlpg);
        if (!dpg) return 0;
        u8 *drec = (u8 *)calloc(1, 4u + IND_DIR_MAX * 4u);
        if (!drec) return 0;
        u16 s = 0;
        int ok = seg_page_insert(dpg, drec, (u16)(4u + IND_DIR_MAX * 4u), &s);
        free(drec);
        if (!ok) return 0;
        g->ind_root = dlpg;
        cat_put_root(g, K_IND, 2, dlpg);
    }
    if (page >= IND_DIR_MAX) return 0;
    u8 *dpg = seg_txn_touch(g->gs, g->ind_root);
    if (!dpg) return 0;
    u16 dsz = 0;
    const u8 *dr = seg_page_read(dpg, 0, &dsz);
    if (!dr || dsz < 4) return 0;
    u8 *dw = (u8 *)(dpg + (dr - dpg));
    u32 npages = g4ld32(dw);
    while (npages <= page) {
        u32 plpg = 0;
        u8 *dp = seg_txn_alloc(g->gs, SEG_KIND_INDIRECT, &plpg);
        if (!dp) return 0;
        u8 *zrec = (u8 *)calloc(1, IND_PAGE_SLOTS * 4u);
        if (!zrec) return 0;
        u16 s = 0;
        int ok = seg_page_insert(dp, zrec, (u16)(IND_PAGE_SLOTS * 4u), &s);
        free(zrec);
        if (!ok) return 0;
        g4st32(dw + 4 + npages * 4u, plpg);
        npages++;
        g4st32(dw, npages);
    }
    u32 dlpg2 = g4ld32(dw + 4 + page * 4u);
    u8 *ppg = seg_txn_touch(g->gs, dlpg2);
    if (!ppg) return 0;
    u16 psz = 0;
    const u8 *p = seg_page_read(ppg, 0, &psz);
    if (!p || (u32)psz < (slot + 1u) * 4u) return 0;
    g4st32(ppg + (p - ppg) + slot * 4u, eid);
    return 1;
}

/* create an entity record at an EXPLICIT node id from parsed blob fields
 * (the mirror path — g4_create_entity mints ids, this implants one). */
static int ent_create_at(graph4_t *g, u32 node, u32 name_sid, const vf_t *f) {
    u32 type_sid = st4_intern(g->st, f->type, f->tl);
    if (!type_sid) return 0;
    u32 obs[2] = { 0, 0 };
    if (f->ocount >= 1 && f->obslen[0]) {
        obs[0] = st4_intern(g->st, f->obs[0], f->obslen[0]);
        if (!obs[0]) { st4_decref(g->st, type_sid); return 0; }
    }
    if (f->ocount >= 2 && f->obslen[1]) {
        obs[1] = st4_intern(g->st, f->obs[1], f->obslen[1]);
        if (!obs[1]) { st4_decref(g->st, type_sid); if (obs[0]) st4_decref(g->st, obs[0]); return 0; }
    }
    g4_entity_t e;
    memset(&e, 0, sizeof e);
    e.name_sid = name_sid;
    e.type_sid = type_sid;
    e.mtime = f->mtime;
    e.obs_mtime = f->obs_mtime;
    e.obs0_sid = obs[0];
    e.obs1_sid = obs[1];
    e.obs_count = f->ocount;
    e.psi = f->psi;
    u8 rec[ENT_SIZE];
    ent_encode(rec, &e);
    u32 lpg = g->last_ent_page; u16 slot = 0;
    u8 *pg = (lpg != (u32)SEG_PT_NONE) ? seg_txn_touch(g->gs, lpg) : NULL;
    if (!pg || !seg_page_insert(pg, rec, ENT_SIZE, &slot)) {
        pg = seg_txn_alloc(g->gs, SEG_KIND_ENTITY, &lpg);
        if (!pg || lpg >= G4_MAX_LPG || !seg_page_insert(pg, rec, ENT_SIZE, &slot)) {
            st4_decref(g->st, type_sid);
            if (obs[0]) st4_decref(g->st, obs[0]);
            if (obs[1]) st4_decref(g->st, obs[1]);
            return 0;
        }
        g->last_ent_page = lpg;
    }
    u32 eid = EID_MAKE(lpg, slot);
    if (!ind_set(g, node, eid)) return 0;
    type_put(g, type_sid, node);
    g->ent_count++;
    meta_store(g);                         /* persists next_node + ent_count */
    { u32set_t old = {0}; tri_apply(g, node, old.v, 0); free(old.v); }
    return 1;
}

int g4_mirror_apply(graph4_t *g, u32 node, const u8 *name, u16 nl, u32 gen,
                    const u8 *blob, u32 blen) {
    if (!node) return 0;
    vf_t f;
    if (!vrow_parse(blob, blen, &f)) return 0;
    u32 lnode = 0, lgen = 0;
    int have = name_row(g, name, nl, &lnode, &lgen);
    if (have) {
        int cmp = (gen > lgen) - (gen < lgen);
        if (cmp == 0) cmp = (node > lnode) - (node < lnode);
        if (cmp < 0) return 0;                       /* incoming lost */
        if (cmp > 0 || (lnode && lnode != node)) {
            if (!g4_mirror_name(g, name, nl, node, gen)) return 0;
        }
        /* cmp == 0 && lnode == node: the binding is already ours — just
         * ensure the record exists (transient: row set before record) */
    } else {
        if (!g4_mirror_name(g, name, nl, node, gen)) return 0;
    }
    g4_entity_t cur;
    if (!g4_read_entity(g, node, &cur)) {
        u32 name_sid = st4_intern(g->st, name, nl);
        if (!name_sid) return 0;
        if (!ent_create_at(g, node, name_sid, &f)) { st4_decref(g->st, name_sid); return 0; }
    } else {
        int c = g4_vrow_cmp(g, blob, blen, &cur);
        if (c == 0) return 2;                    /* already equal: no-op */
        if (c > 0) return 0;                     /* local content newer: never clobbered */
        if (!g4_vrow_apply(g, node, blob, blen)) return 0;
    }
    u32 nn = node + 1u;
    if (nn > g->next_node) { g->next_node = nn; meta_store(g); }
    return 1;
}
