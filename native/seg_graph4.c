/*
 * seg_graph4.c — entities + persisted name index over segstores.
 *
 * See segstore.h graph4 block (Design_Graph4Entities_2026_08_24).
 * Adjacency lands in the next module; adj_ref is carried but unused here.
 */
#include "segstore.h"

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
    u32 ni_dir_lpg;         /* directory page lpg + 1; 0 = none (mirror of meta) */
    u32 ni_npages;          /* cached from directory */
    u32 ent_count;          /* live entities (rebuilt at open) */
    u32 last_ent_page;      /* insertion affinity; SEG_PT_NONE = none */
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
            }
        }
    }
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

int g4_delete_entity(graph4_t *g, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(g, eid, &e)) return 0;
    /* release string refs */
    st4_decref(g->st, e.name_sid);
    st4_decref(g->st, e.type_sid);
    if (e.obs_count >= 1 && e.obs0_sid) st4_decref(g->st, e.obs0_sid);
    if (e.obs_count >= 2 && e.obs1_sid) st4_decref(g->st, e.obs1_sid);
    if (!ni_remove(g, e.name_sid)) return 0;
    u8 *pg = seg_txn_touch(g->gs, EID_LPG(eid));
    if (!pg || !seg_page_delete(pg, (u16)EID_SLOT(eid))) return 0;
    g->ent_count--;
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
    /* dedup: same obs already present? release the extra ref and refuse */
    if ((e.obs_count >= 1 && e.obs0_sid == sid)) { st4_decref(g->st, sid); return 0; }
    u8 *r = ent_rec_w(g, eid);
    if (!r) { st4_decref(g->st, sid); return 0; }
    if (e.obs_count == 0) g4st32(r + 32, sid);
    else                  g4st32(r + 36, sid);
    r[40] = (u8)(e.obs_count + 1);
    g4st64(r + 24, mtime);                               /* obs_mtime */
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
    st4_decref(g->st, sid);
    return 1;
}
