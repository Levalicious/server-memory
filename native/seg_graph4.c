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

#define ENT_SIZE      68u
#define NI_REC_SIZE   4072u                 /* slot-0 record of a NAMEIDX page */
#define NI_PER_PAGE   (NI_REC_SIZE / 8u)    /* 509 buckets */
#define NI_DIR_MAX    ((NI_REC_SIZE - 4u) / 4u)   /* 1017 index pages max */

struct graph4 {
    mstore_t   *ms;
    segstore_t *gs;         /* graph segment */
    st4_t      *st;         /* strings layer (owned) */
    u32 last_adj_page;      /* ADJ insertion affinity; SEG_PT_NONE = none */
    /* trigram prefilter (lazy; dirty-set decoupled from writes) */
    ReTrigramLive *tri; int tri_built;
    u32 *tri_dirty; u8 *tri_dirty_op; u32 tri_dcap, tri_dcnt;
    /* type index: open-addr type_sid -> eid postings (lazy, O(1) maint) */
    struct tpost { u32 type_sid; u32 *eids; u32 n, cap; } *tidx;
    u32 tidx_cap, tidx_n; int tidx_built;
    u32 ni_dir_lpg;         /* directory page lpg + 1; 0 = none (mirror of meta) */
    u32 ni_npages;          /* cached from directory */
    u32 ent_count;          /* live entities (rebuilt at open) */
    u64 structural_total, walker_total;   /* recomputed at open; memory-held */
    u32 last_ent_page;      /* insertion affinity; SEG_PT_NONE = none */
};

#define TRI_OP_REINDEX 1u
#define TRI_OP_REMOVE  2u

static void tri_mark(graph4_t *g, u32 eid, u8 op);
static int  tidx_add(graph4_t *g, u32 type_sid, u32 eid);
static void tidx_remove(graph4_t *g, u32 type_sid, u32 eid);

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
    return 1;
}

static const u8 *ent_rec(graph4_t *g, u32 eid, u16 *sz_out) {
    if (eid == 0) return NULL;
    const u8 *pg = seg_txn_view(g->gs, EID_LPG(eid));
    if (!pg) return NULL;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, (u16)EID_SLOT(eid), &sz);
    if (!r || sz != ENT_SIZE) return NULL;
    if (sz_out) *sz_out = sz;
    return r;
}

/* write-through: touch page, update record in place (same size) */
static u8 *ent_rec_w(graph4_t *g, u32 eid) {
    u8 *pg = seg_txn_touch(g->gs, EID_LPG(eid));
    if (!pg) return NULL;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, (u16)EID_SLOT(eid), &sz);
    if (!r || sz != ENT_SIZE) return NULL;
    return (u8 *)(pg + (r - pg));
}

/* ---------- name index ---------- */

/* directory record layout: [u32 npages][u32 lpg x npages] (in slot 0) */

static const u8 *ni_dir(graph4_t *g) {
    if (g->ni_dir_lpg == 0) return NULL;
    const u8 *pg = seg_txn_view(g->gs, g->ni_dir_lpg - 1);
    if (!pg) return NULL;
    u16 sz = 0;
    return seg_page_read(pg, 0, &sz);
}

static u32 ni_page_lpg(graph4_t *g, u32 t) {
    const u8 *d = ni_dir(g);
    return d ? g4ld32(d + 4 + 4u * t) : 0;
}

/* bucket accessors: global index i -> page i/NI_PER_PAGE, entry i%NI_PER_PAGE */
static int ni_get(graph4_t *g, u32 i, u32 *name_sid, u32 *eid) {
    const u8 *pg = seg_txn_view(g->gs, ni_page_lpg(g, i / NI_PER_PAGE));
    if (!pg) return 0;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, 0, &sz);
    if (!r || sz != NI_REC_SIZE) return 0;
    u32 off = (i % NI_PER_PAGE) * 8u;
    *name_sid = g4ld32(r + off);
    *eid      = g4ld32(r + off + 4);
    return 1;
}

static int ni_set(graph4_t *g, u32 i, u32 name_sid, u32 eid) {
    u8 *pg = seg_txn_touch(g->gs, ni_page_lpg(g, i / NI_PER_PAGE));
    if (!pg) return 0;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, 0, &sz);
    if (!r || sz != NI_REC_SIZE) return 0;
    u8 *w = (u8 *)(pg + (r - pg));
    u32 off = (i % NI_PER_PAGE) * 8u;
    g4st32(w + off, name_sid);
    g4st32(w + off + 4, eid);
    return 1;
}

static u32 ni_capacity(const graph4_t *g) { return g->ni_npages * NI_PER_PAGE; }

static u64 ni_hash(u32 name_sid) {         /* splitmix-style scramble */
    u64 z = (u64)name_sid * 0x9E3779B97F4A7C15ull;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    return z ^ (z >> 27);
}

/* allocate a fresh empty index of npages; returns dir lpg+1 or 0 */
static u32 ni_build_empty(graph4_t *g, u32 npages, u32 *lpgs_out) {
    static u8 zero[NI_REC_SIZE];           /* zeroed bucket block */
    memset(zero, 0, sizeof zero);
    for (u32 t = 0; t < npages; t++) {
        u32 lpg; u16 slot;
        u8 *pg = seg_txn_alloc(g->gs, SEG_KIND_NAMEIDX, &lpg);
        if (!pg || !seg_page_insert(pg, zero, NI_REC_SIZE, &slot) || slot != 0)
            return 0;
        lpgs_out[t] = lpg;
    }
    u8 dir[NI_REC_SIZE];
    memset(dir, 0, sizeof dir);
    g4st32(dir, npages);
    for (u32 t = 0; t < npages; t++) g4st32(dir + 4 + 4u * t, lpgs_out[t]);
    u32 dlpg; u16 slot;
    u8 *dp = seg_txn_alloc(g->gs, SEG_KIND_NAMEIDX, &dlpg);
    if (!dp || !seg_page_insert(dp, dir, NI_REC_SIZE, &slot) || slot != 0)
        return 0;
    return dlpg + 1;
}

static int ni_insert(graph4_t *g, u32 name_sid, u32 eid);

/* rehash into a doubled index; frees old pages */
static int ni_rehash(graph4_t *g) {
    u32 old_npages = g->ni_npages;
    u32 old_dir = g->ni_dir_lpg;
    u32 new_npages = old_npages ? old_npages * 2 : 1;
    if (new_npages > NI_DIR_MAX) return 0;

    /* collect live buckets first (old index still readable) */
    u32 cap = ni_capacity(g);
    u32 *live = NULL; u32 nlive = 0;
    if (cap) {
        live = (u32 *)malloc((size_t)cap * 8);
        if (!live) return 0;
        for (u32 i = 0; i < cap; i++) {
            u32 ns, ei;
            if (!ni_get(g, i, &ns, &ei)) { free(live); return 0; }
            if (ns) { live[nlive * 2] = ns; live[nlive * 2 + 1] = ei; nlive++; }
        }
    }
    u32 *old_lpgs = NULL;
    if (old_npages) {
        old_lpgs = (u32 *)malloc((size_t)old_npages * 4);
        if (!old_lpgs) { free(live); return 0; }
        for (u32 t = 0; t < old_npages; t++) old_lpgs[t] = ni_page_lpg(g, t);
    }

    u32 *lpgs = (u32 *)malloc((size_t)new_npages * 4);
    if (!lpgs) { free(live); free(old_lpgs); return 0; }
    u32 ndir = ni_build_empty(g, new_npages, lpgs);
    free(lpgs);
    if (!ndir) { free(live); free(old_lpgs); return 0; }
    g->ni_dir_lpg = ndir;
    g->ni_npages = new_npages;

    for (u32 k = 0; k < nlive; k++)
        if (!ni_insert(g, live[k * 2], live[k * 2 + 1])) { free(live); free(old_lpgs); return 0; }
    free(live);

    /* free the old pages + old directory */
    if (old_npages) {
        for (u32 t = 0; t < old_npages; t++) seg_txn_free(g->gs, old_lpgs[t]);
        free(old_lpgs);
    }
    if (old_dir) seg_txn_free(g->gs, old_dir - 1);
    seg_txn_set_roots(g->gs, g->ni_dir_lpg, 0);
    return 1;
}

static int ni_insert(graph4_t *g, u32 name_sid, u32 eid) {
    u32 cap = ni_capacity(g);
    if (cap == 0 || (u64)(g->ent_count + 1) * 10 >= (u64)cap * 7) {
        if (!ni_rehash(g)) return 0;
        cap = ni_capacity(g);
    }
    for (u32 i = (u32)(ni_hash(name_sid) % cap); ; i = (i + 1) % cap) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) return 0;
        if (ns == 0) return ni_set(g, i, name_sid, eid);
        if (ns == name_sid) return 0;      /* duplicate name = caller bug */
    }
}

static u32 ni_find(graph4_t *g, u32 name_sid) {
    u32 cap = ni_capacity(g);
    if (!cap) return 0;
    for (u32 i = (u32)(ni_hash(name_sid) % cap); ; i = (i + 1) % cap) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) return 0;
        if (ns == 0) return 0;
        if (ns == name_sid) return ei;
    }
}

static int ni_remove(graph4_t *g, u32 name_sid) {
    u32 cap = ni_capacity(g);
    if (!cap) return 0;
    u32 i = (u32)(ni_hash(name_sid) % cap);
    for (;;) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) return 0;
        if (ns == 0) return 0;
        if (ns == name_sid) break;
        i = (i + 1) % cap;
    }
    if (!ni_set(g, i, 0, 0)) return 0;
    /* backward-shift */
    u32 j = i;
    for (;;) {
        j = (j + 1) % cap;
        u32 ns, ei;
        if (!ni_get(g, j, &ns, &ei)) return 0;
        if (ns == 0) break;
        u32 home = (u32)(ni_hash(ns) % cap);
        int between = (i < j) ? (home > i && home <= j)
                              : (home > i || home <= j);
        if (!between) {
            if (!ni_set(g, i, ns, ei) || !ni_set(g, j, 0, 0)) return 0;
            i = j;
        }
    }
    return 1;
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

    /* name index root from meta (committed) */
    /* note: nameindex_root is maintained via seg_txn_set_roots at rehash */
    g->ni_dir_lpg = 0; g->ni_npages = 0; g->ent_count = 0;
    g->ni_dir_lpg = segstore_nameindex_root(g->gs);
    if (g->ni_dir_lpg) {
        const u8 *d = ni_dir(g);
        if (!d) { graph4_close(g); return NULL; }
        g->ni_npages = g4ld32(d);
        /* count live entities by scanning the index */
        u32 cap = ni_capacity(g);
        for (u32 i = 0; i < cap; i++) {
            u32 ns, ei;
            if (!ni_get(g, i, &ns, &ei)) { graph4_close(g); return NULL; }
            if (ns) {
                g->ent_count++;
                if (EID_LPG(ei) != (u32)SEG_PT_NONE) g->last_ent_page = EID_LPG(ei);
                g4_entity_t e2;
                if (g4_read_entity(g, ei, &e2)) {
                    g->structural_total += e2.structural_visits;
                    g->walker_total     += e2.walker_visits;
                }
            }
        }
    }
    return g;
}

void graph4_close(graph4_t *g) {
    if (!g) return;
    if (g->tri) re_trigram_live_free(g->tri);
    free(g->tri_dirty); free(g->tri_dirty_op);
    if (g->tidx) {
        for (u32 i = 0; i < g->tidx_cap; i++) free(g->tidx[i].eids);
        free(g->tidx);
    }
    st4_close(g->st);
    free(g);
}

u32 g4_entity_count(graph4_t *g) { return g->ent_count; }
const u8 *g4_str(graph4_t *g, u32 sid, u16 *len_out) { return st4_get(g->st, sid, len_out); }

/* ---------- ops ---------- */

u32 g4_lookup(graph4_t *g, const u8 *name, u16 nlen) {
    u32 sid = st4_find(g->st, name, nlen);
    return sid ? ni_find(g, sid) : 0;
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
    u32 eid = EID_MAKE(lpg, slot);
    if (!ni_insert(g, name_sid, eid)) return 0;
    g->ent_count++;
    tri_mark(g, eid, TRI_OP_REINDEX);
    tidx_add(g, type_sid, eid);
    return eid;
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

int g4_delete_entity(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    /* remove every incident edge (mirrors on peers + own chain) first */
    if (!adj_clear_all(g, eid, &e)) return 0;
    /* release string refs */
    st4_decref(g->st, e.name_sid);
    st4_decref(g->st, e.type_sid);
    if (e.obs_count >= 1 && e.obs0_sid) st4_decref(g->st, e.obs0_sid);
    if (e.obs_count >= 2 && e.obs1_sid) st4_decref(g->st, e.obs1_sid);
    if (!ni_remove(g, e.name_sid)) return 0;
    u8 *pg = seg_txn_touch(g->gs, EID_LPG(eid));
    if (!pg || !seg_page_delete(pg, (u16)EID_SLOT(eid))) return 0;
    g->ent_count--;
    tri_mark(g, eid, TRI_OP_REMOVE);
    tidx_remove(g, e.type_sid, eid);
    return 1;
}

u32 g4_list_entities(graph4_t *g, u32 *out, u32 max) {
    u32 n = 0, cap = ni_capacity(g);
    for (u32 i = 0; i < cap && n < max; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (ns) out[n++] = ei;
    }
    return n;
}

int g4_add_observation(graph4_t *g, u32 eid, const u8 *obs, u16 len, u64 mtime) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    if (e.obs_count >= 2) return 0;                       /* KB constraint */
    u32 sid = st4_intern(g->st, obs, len);
    if (!sid) return 0;
    /* v3 semantics: NO dup check — the same obs may occupy both slots */
    u8 *r = ent_rec_w(g, eid);
    if (!r) { st4_decref(g->st, sid); return 0; }
    if (e.obs_count == 0) g4st32(r + 32, sid);
    else                  g4st32(r + 36, sid);
    r[40] = (u8)(e.obs_count + 1);
    g4st64(r + 24, mtime);                               /* obs_mtime */
    g4st64(r + 16, mtime);                               /* mtime too (v3) */
    tri_mark(g, eid, TRI_OP_REINDEX);
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
    u8 *r = ent_rec_w(g, eid);
    if (!r) return 0;
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
    tri_mark(g, eid, TRI_OP_REINDEX);
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

/* ================= indexes + search ================= */

#define G4_DFA_MIN_VERIFY 128u

/* ---- dirty set (eid -> op, last-wins; open addressing) ---- */

static void tri_mark(graph4_t *g, u32 eid, u8 op) {
    if (!g->tri_built) return;                /* index not built: nothing to catch up */
    if ((u64)(g->tri_dcnt + 1) * 10 >= (u64)g->tri_dcap * 7) {
        u32 ncap = g->tri_dcap ? g->tri_dcap * 2 : 256;
        u32 *nk = (u32 *)calloc(ncap, 4);
        u8  *no = (u8 *)calloc(ncap, 1);
        if (!nk || !no) { free(nk); free(no); g->tri_built = 0; return; }  /* degrade: full rebuild */
        for (u32 i = 0; i < g->tri_dcap; i++) if (g->tri_dirty[i]) {
            u32 s = (u32)(((u64)g->tri_dirty[i] * 0x9E3779B97F4A7C15ull) >> 32) & (ncap - 1);
            while (nk[s]) s = (s + 1) & (ncap - 1);
            nk[s] = g->tri_dirty[i]; no[s] = g->tri_dirty_op[i];
        }
        free(g->tri_dirty); free(g->tri_dirty_op);
        g->tri_dirty = nk; g->tri_dirty_op = no; g->tri_dcap = ncap;
    }
    u32 s = (u32)(((u64)eid * 0x9E3779B97F4A7C15ull) >> 32) & (g->tri_dcap - 1);
    while (g->tri_dirty[s] && g->tri_dirty[s] != eid) s = (s + 1) & (g->tri_dcap - 1);
    if (!g->tri_dirty[s]) { g->tri_dirty[s] = eid; g->tri_dcnt++; }
    g->tri_dirty_op[s] = op;                  /* last write wins */
}

/* index one entity's field union into the live trigram index */
static void tri_index_entity(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) { re_trigram_live_remove(g->tri, eid); return; }
    const char *fields[4]; size_t lens[4]; int nf = 0;
    u16 len;
    const u8 *b;
    if ((b = st4_get(g->st, e.name_sid, &len))) { fields[nf] = (const char *)b; lens[nf++] = len; }
    if ((b = st4_get(g->st, e.type_sid, &len))) { fields[nf] = (const char *)b; lens[nf++] = len; }
    if (e.obs_count >= 1 && (b = st4_get(g->st, e.obs0_sid, &len))) { fields[nf] = (const char *)b; lens[nf++] = len; }
    if (e.obs_count >= 2 && (b = st4_get(g->st, e.obs1_sid, &len))) { fields[nf] = (const char *)b; lens[nf++] = len; }
    re_trigram_live_set(g->tri, eid, fields, lens, nf);
}

void g4_index_sync(graph4_t *g) {
    if (!g->tri_built) {
        if (g->tri) re_trigram_live_free(g->tri);
        g->tri = re_trigram_live_new();
        if (!g->tri) return;
        u32 cap = ni_capacity(g);
        for (u32 i = 0; i < cap; i++) {
            u32 ns, ei;
            if (!ni_get(g, i, &ns, &ei)) break;
            if (ns) tri_index_entity(g, ei);
        }
        g->tri_built = 1;
        g->tri_dcnt = 0;
        if (g->tri_dirty) memset(g->tri_dirty, 0, (size_t)g->tri_dcap * 4);
        return;
    }
    if (!g->tri_dcnt) return;
    for (u32 i = 0; i < g->tri_dcap; i++) {
        if (!g->tri_dirty[i]) continue;
        u32 eid = g->tri_dirty[i];
        if (g->tri_dirty_op[i] == TRI_OP_REMOVE) re_trigram_live_remove(g->tri, eid);
        else tri_index_entity(g, eid);
    }
    memset(g->tri_dirty, 0, (size_t)g->tri_dcap * 4);
    memset(g->tri_dirty_op, 0, g->tri_dcap);
    g->tri_dcnt = 0;
}

/* ---- type index ---- */

static struct tpost *tidx_slot(graph4_t *g, u32 type_sid) {
    u32 i = (u32)(((u64)type_sid * 0x9E3779B97F4A7C15ull) >> 32) & (g->tidx_cap - 1);
    while (g->tidx[i].type_sid && g->tidx[i].type_sid != type_sid)
        i = (i + 1) & (g->tidx_cap - 1);
    return &g->tidx[i];
}

static int tidx_add(graph4_t *g, u32 type_sid, u32 eid) {
    if (!g->tidx_built) return 1;
    if ((g->tidx_n + 1) * 4 >= g->tidx_cap * 3) {
        u32 ncap = g->tidx_cap ? g->tidx_cap * 2 : 64;
        struct tpost *nt = (struct tpost *)calloc(ncap, sizeof *nt);
        if (!nt) { g->tidx_built = 0; return 1; }          /* degrade */
        struct tpost *old = g->tidx; u32 ocap = g->tidx_cap;
        g->tidx = nt; g->tidx_cap = ncap;
        for (u32 i = 0; i < ocap; i++) if (old[i].type_sid) {
            struct tpost *s = tidx_slot(g, old[i].type_sid);
            *s = old[i];
        }
        free(old);
    }
    struct tpost *p = tidx_slot(g, type_sid);
    if (!p->type_sid) { p->type_sid = type_sid; g->tidx_n++; }
    return g4push(&p->eids, &p->n, &p->cap, eid);
}

static void tidx_remove(graph4_t *g, u32 type_sid, u32 eid) {
    if (!g->tidx_built || !g->tidx_cap) return;
    struct tpost *p = tidx_slot(g, type_sid);
    if (!p->type_sid) return;
    for (u32 i = 0; i < p->n; i++)
        if (p->eids[i] == eid) { p->eids[i] = p->eids[--p->n]; return; }
}

static void tidx_build(graph4_t *g) {
    if (g->tidx_built) return;
    g->tidx_built = 1;                        /* set first: tidx_add is gated on it */
    u32 cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (g4_read_entity(g, ei, &e))
            if (!tidx_add(g, e.type_sid, ei)) { g->tidx_built = 0; return; }
    }
}

u32 g4_entities_by_type(graph4_t *g, const u8 *type, u16 tlen, u32 *out, u32 max) {
    u32 sid = st4_find(g->st, type, tlen);
    if (!sid) return 0;
    tidx_build(g);
    if (g->tidx_built && g->tidx_cap) {
        struct tpost *p = tidx_slot(g, sid);
        if (!p->type_sid) return 0;
        for (u32 i = 0; i < p->n && i < max; i++) out[i] = p->eids[i];
        return p->n;
    }
    /* sound fallback: O(N) scan */
    u32 n = 0, cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (g4_read_entity(g, ei, &e) && e.type_sid == sid) {
            if (n < max) out[n] = ei;
            n++;
        }
    }
    return n;
}

u32 g4_entity_types(graph4_t *g, u32 *out_sids, u32 max) {
    tidx_build(g);
    u32 n = 0;
    if (g->tidx_built) {
        for (u32 i = 0; i < g->tidx_cap; i++)
            if (g->tidx[i].type_sid && g->tidx[i].n > 0) {
                if (n < max) out_sids[n] = g->tidx[i].type_sid;
                n++;
            }
        return n;
    }
    /* fallback: scan with local dedup */
    u32 cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (!g4_read_entity(g, ei, &e)) continue;
        int dup = 0;
        for (u32 k = 0; k < n && k < max; k++) if (out_sids[k] == e.type_sid) { dup = 1; break; }
        if (dup) continue;
        if (n < max) out_sids[n] = e.type_sid;
        n++;
    }
    return n;
}

u32 g4_relation_types(graph4_t *g, u32 *out_sids, u32 max) {
    /* O(E) sweep of FORWARD entries with dedup via a small open set */
    u32 setcap = 256, setn = 0;
    u32 *set = (u32 *)calloc(setcap, 4);
    if (!set) return 0;
    u32 cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (!g4_read_entity(g, ei, &e)) continue;
        u32 aref = e.adj_ref;
        while (aref) {
            adj_view_t v;
            if (!adj_view(g, aref, &v)) break;
            for (u32 k = 0; k < v.count; k++) {
                g4_edge_t ed;
                adj_ent_decode(v.ents + k * ADJ_ENT, &ed);
                if (ed.direction != G4_DIR_FORWARD) continue;
                if ((setn + 1) * 4 >= setcap * 3) {
                    u32 nc = setcap * 2;
                    u32 *nset = (u32 *)calloc(nc, 4);
                    if (!nset) { free(set); return setn; }
                    for (u32 x = 0; x < setcap; x++) if (set[x]) {
                        u32 s = (u32)(((u64)set[x] * 0x9E3779B97F4A7C15ull) >> 32) & (nc - 1);
                        while (nset[s]) s = (s + 1) & (nc - 1);
                        nset[s] = set[x];
                    }
                    free(set); set = nset; setcap = nc;
                }
                u32 s = (u32)(((u64)ed.rel_sid * 0x9E3779B97F4A7C15ull) >> 32) & (setcap - 1);
                while (set[s] && set[s] != ed.rel_sid) s = (s + 1) & (setcap - 1);
                if (!set[s]) { set[s] = ed.rel_sid; setn++; }
            }
            aref = v.next;
        }
    }
    u32 n = 0;
    for (u32 i = 0; i < setcap; i++)
        if (set[i]) { if (n < max) out_sids[n] = set[i]; n++; }
    free(set);
    return n;
}

u32 g4_orphaned(graph4_t *g, u32 *out, u32 max) {
    u32 n = 0, cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (g4_read_entity(g, ei, &e) && e.adj_ref == 0) {
            if (n < max) out[n] = ei;
            n++;
        }
    }
    return n;
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

u32 g4_search(graph4_t *g, const char *pattern, u32 *out, u32 max) {
    const char *err = NULL;
    ReNode *ast = re_parse(pattern, &err);
    if (!ast) return 0;
    Regex *re = re_compile_ast(ast);
    if (!re) { re_ast_free(ast); return 0; }

    g4_index_sync(g);

    ReTrigramQuery *q = re_trigram_build(ast);
    ReCandidates64 cand;
    cand.ids = NULL; cand.n = 0; cand.all = 1;
    if (g->tri_built) cand = re_trigram_live_eval(q, g->tri);

    u32 nverify = cand.all ? g->ent_count : cand.n;
    ReDfa *d = (nverify >= G4_DFA_MIN_VERIFY) ? re_dfa_build(re) : NULL;

    u32 found = 0;
    if (cand.all) {
        u32 cap = ni_capacity(g);
        for (u32 i = 0; i < cap; i++) {
            u32 ns, ei;
            if (!ni_get(g, i, &ns, &ei)) break;
            if (!ns) continue;
            if (g4_entity_matches(g, d, re, ei)) { if (found < max) out[found] = ei; found++; }
        }
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
}
void g4_inc_walker_visit(graph4_t *g, u32 eid) {
    u8 *r = ent_rec_w(g, eid);
    if (!r) return;
    g4st64(r + 52, g4ld64(r + 52) + 1);
    g->walker_total++;
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
    return 1;
}

u32 g4_relation_count(graph4_t *g) {
    u32 n = 0, cap = ni_capacity(g);
    for (u32 i = 0; i < cap; i++) {
        u32 ns, ei;
        if (!ni_get(g, i, &ns, &ei)) break;
        if (!ns) continue;
        g4_entity_t e;
        if (!g4_read_entity(g, ei, &e)) continue;
        u32 aref = e.adj_ref;
        while (aref) {
            adj_view_t v;
            if (!adj_view(g, aref, &v)) break;
            for (u32 k = 0; k < v.count; k++) {
                g4_edge_t ed;
                adj_ent_decode(v.ents + k * ADJ_ENT, &ed);
                if (ed.direction == G4_DIR_FORWARD) n++;
            }
            aref = v.next;
        }
    }
    return n;
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
