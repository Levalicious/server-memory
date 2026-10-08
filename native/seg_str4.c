/*
 * seg_str4.c — refcounted interned strings over a segstore.
 *
 * See segstore.h st4 block for the contract (Design_St4Strings_2026_08_24).
 * Placement: interns go to the last string page with room (kind affinity is
 * automatic — this layer only allocates SEG_KIND_STRING pages), else a fresh
 * page. Refcount bumps are same-size in-place updates (no growth path).
 */
#include "segstore.h"

#include <stdlib.h>
#include <string.h>

#define SID_MAKE(lpg, slot) ((((u32)(lpg) << 12) | (u32)(slot)) + 1u)
#define SID_LPG(sid)  (((sid) - 1u) >> 12)
#define SID_SLOT(sid) (((sid) - 1u) & 0xFFFu)
#define ST4_MAX_LPG   (1u << 20)

/* FNV-1a 64 */
static u64 str_hash(const u8 *b, u16 len) {
    u64 h = 0xCBF29CE484222325ull;
    for (u16 i = 0; i < len; i++) { h ^= b[i]; h *= 0x100000001B3ull; }
    return h ? h : 1;
}

typedef struct { u64 hash; u32 sid; } ent_t;   /* sid 0 = empty */

struct st4 {
    segstore_t *seg;
    ent_t *map; u32 mapcap, nlive;
    u32 last_page;          /* insertion-affinity hint; SEG_PT_NONE = none */
};

/* ---------- map ---------- */

static int map_grow(st4_t *st, u32 mincap);

static int map_insert(st4_t *st, u64 hash, u32 sid) {
    if ((st->nlive + 1) * 4 >= st->mapcap * 3)          /* load < 0.75 */
        if (!map_grow(st, st->mapcap ? st->mapcap * 2 : 1024)) return 0;
    u32 mask = st->mapcap - 1;
    for (u32 i = (u32)hash & mask; ; i = (i + 1) & mask)
        if (st->map[i].sid == 0) { st->map[i].hash = hash; st->map[i].sid = sid; st->nlive++; return 1; }
}

static int map_grow(st4_t *st, u32 mincap) {
    u32 nc = 1024; while (nc < mincap) nc *= 2;
    ent_t *nm = (ent_t *)calloc(nc, sizeof *nm);
    if (!nm) return 0;
    ent_t *old = st->map; u32 oldcap = st->mapcap;
    st->map = nm; st->mapcap = nc; st->nlive = 0;
    for (u32 i = 0; i < oldcap; i++)
        if (old[i].sid) map_insert(st, old[i].hash, old[i].sid);
    free(old);
    return 1;
}

static void map_remove(st4_t *st, u64 hash, u32 sid) {
    u32 mask = st->mapcap - 1;
    u32 i = (u32)hash & mask;
    while (st->map[i].sid && !(st->map[i].sid == sid && st->map[i].hash == hash))
        i = (i + 1) & mask;
    if (!st->map[i].sid) return;
    /* backward-shift deletion */
    st->map[i].sid = 0; st->nlive--;
    u32 j = i;
    for (;;) {
        j = (j + 1) & mask;
        if (!st->map[j].sid) break;
        u32 home = (u32)st->map[j].hash & mask;
        /* can j's entry legally move to i? (home not in (i, j]) */
        int between = (i < j) ? (home > i && home <= j)
                              : (home > i || home <= j);
        if (!between) {
            st->map[i] = st->map[j];
            st->map[j].sid = 0;
            i = j;
        }
    }
}

/* ---------- record access ---------- */

static const u8 *rec_bytes(st4_t *st, u32 sid, u16 *len_out, u32 *ref_out) {
    if (sid == 0) return NULL;
    u32 lpg = SID_LPG(sid); u16 slot = (u16)SID_SLOT(sid);
    const u8 *pg = seg_txn_view(st->seg, lpg);
    if (!pg) return NULL;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, slot, &sz);
    if (!r || sz < 5) return NULL;
    if (ref_out) *ref_out = (u32)r[0] | ((u32)r[1] << 8) | ((u32)r[2] << 16) | ((u32)r[3] << 24);
    if (len_out) *len_out = (u16)(sz - 4);
    return r + 4;
}

static int rec_set_refcount(st4_t *st, u32 sid, u32 rc) {
    u32 lpg = SID_LPG(sid); u16 slot = (u16)SID_SLOT(sid);
    u8 *pg = seg_txn_touch(st->seg, lpg);
    if (!pg) return 0;
    u16 sz = 0;
    const u8 *r = seg_page_read(pg, slot, &sz);
    if (!r || sz < 5) return 0;
    u8 *w = (u8 *)(pg + (r - pg));      /* same buffer, writable */
    w[0] = (u8)rc; w[1] = (u8)(rc >> 8); w[2] = (u8)(rc >> 16); w[3] = (u8)(rc >> 24);
    return 1;
}

/* ---------- lifecycle ---------- */

st4_t *st4_open(segstore_t *seg) {
    st4_t *st = (st4_t *)calloc(1, sizeof *st);
    if (!st) return NULL;
    st->seg = seg;
    st->last_page = SEG_PT_NONE;
    /* rebuild intern map by scanning live records */
    u64 lp = segstore_logical_pages(seg);
    if (lp > ST4_MAX_LPG) { free(st); return NULL; }
    for (u32 l = 0; l < lp; l++) {
        const u8 *pg = segstore_read(seg, l);
        if (!pg) continue;
        st->last_page = l;
        u32 maxslots = (SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / SEG_SLOT_SIZE;
        for (u32 s = 0; s < maxslots; s++) {
            u16 sz = 0;
            const u8 *r = seg_page_read(pg, (u16)s, &sz);
            if (!r) continue;
            if (sz < 5) continue;
            if (!map_insert(st, str_hash(r + 4, (u16)(sz - 4)), SID_MAKE(l, s))) {
                st4_close(st); return NULL;
            }
        }
    }
    return st;
}

void st4_close(st4_t *st) {
    if (!st) return;
    free(st->map);
    free(st);
}

u32 st4_count(const st4_t *st) { return st->nlive; }

/* ---------- ops ---------- */

u32 st4_find(st4_t *st, const u8 *bytes, u16 len) {
    if (!st->mapcap) return 0;
    u64 h = str_hash(bytes, len);
    u32 mask = st->mapcap - 1;
    for (u32 i = (u32)h & mask; st->map[i].sid; i = (i + 1) & mask) {
        if (st->map[i].hash != h) continue;
        u16 rl = 0;
        const u8 *rb = rec_bytes(st, st->map[i].sid, &rl, NULL);
        if (rb && rl == len && memcmp(rb, bytes, len) == 0) return st->map[i].sid;
    }
    return 0;
}

u32 st4_intern(st4_t *st, const u8 *bytes, u16 len) {
    if (len < 1 || len > SEG_PAGE_MAX_REC - 4) return 0;
    u32 sid = st4_find(st, bytes, len);
    if (sid) {
        u32 rc = 0;
        if (!rec_bytes(st, sid, NULL, &rc)) return 0;
        return rec_set_refcount(st, sid, rc + 1) ? sid : 0;
    }
    /* new record: [refcount=1][bytes] */
    u16 rl = (u16)(len + 4);
    u8 *rec = (u8 *)malloc(rl);
    if (!rec) return 0;
    rec[0] = 1; rec[1] = rec[2] = rec[3] = 0;
    memcpy(rec + 4, bytes, len);

    u32 lpg = st->last_page; u16 slot = 0;
    u8 *pg = (lpg != SEG_PT_NONE) ? seg_txn_touch(st->seg, lpg) : NULL;
    if (!pg || !seg_page_insert(pg, rec, rl, &slot)) {
        pg = seg_txn_alloc(st->seg, SEG_KIND_STRING, &lpg);
        if (!pg || lpg >= ST4_MAX_LPG || !seg_page_insert(pg, rec, rl, &slot)) {
            free(rec); return 0;
        }
        st->last_page = lpg;
    }
    free(rec);
    if (slot > 0xFFF) return 0;                 /* sid packing bound (unreachable: max 1019) */
    sid = SID_MAKE(lpg, slot);
    if (!map_insert(st, str_hash(bytes, len), sid)) return 0;
    return sid;
}

const u8 *st4_get(st4_t *st, u32 sid, u16 *len_out) {
    return rec_bytes(st, sid, len_out, NULL);
}

u32 st4_refcount(st4_t *st, u32 sid) {
    u32 rc = 0;
    return rec_bytes(st, sid, NULL, &rc) ? rc : 0;
}

int st4_incref(st4_t *st, u32 sid) {
    u32 rc = 0;
    if (!rec_bytes(st, sid, NULL, &rc)) return 0;
    return rec_set_refcount(st, sid, rc + 1);
}

int st4_decref(st4_t *st, u32 sid) {
    u32 rc = 0; u16 len = 0;
    const u8 *b = rec_bytes(st, sid, &len, &rc);
    if (!b || rc == 0) return 0;
    if (rc > 1) return rec_set_refcount(st, sid, rc - 1);
    /* last ref: remove from map (hash needs the bytes — grab before delete) */
    u64 h = str_hash(b, len);
    u8 *pg = seg_txn_touch(st->seg, SID_LPG(sid));
    if (!pg) return 0;
    if (!seg_page_delete(pg, (u16)SID_SLOT(sid))) return 0;
    map_remove(st, h, sid);
    return 1;
}
