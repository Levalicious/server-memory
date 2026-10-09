/*
 * seg_tree.c — the generic B+tree over slotted pages.
 * (docs/v4-index-design-note.md / Design_IndexRedesign_2026_10_09;
 *  Decision_Lev_ReconciliationRulings_2026_10_09 ruling 1+2.)
 *
 * Invariants:
 *   - Entries in every node are stored in key order (slots 0..n-1 ascending).
 *     Tree pages never contain dead slots (their life is: init + ordered
 *     fills; a mutation re-emits the node's COW working copy).
 *   - Published pages are never mutated: writes go through seg_txn_touch/
 *     seg_txn_alloc and are committed by the caller's txn.
 *   - Node type lives in the page KIND (SEG_KIND_TREE_LEAF/BRANCH), which —
 *     like flags — survives compact/touch.
 *   - Node refs are lpg+1; 0 = no node.
 *   - Branch separator s_k (entry k, k>=1) is the MINIMUM key of child k;
 *     child k's keys lie in [s_k, s_{k+1}). Scan pruning relies on this.
 *   - No minimum fill for the root; a branch with a single child is
 *     collapsed by the root wrapper. Underfull non-root nodes are merged
 *     with an adjacent sibling when the pair fits; otherwise they are left
 *     sparse (deliberate, per the design note).
 *
 * Implementation: node_read is ONE page memcpy — entries are read lazily,
 * slot-by-slot, through seg_page_read. A mutation is a single logical edit
 * (insert / replace / delete at an index, with one optional patched entry)
 * mapped over the original slot array; node_write composes the edited
 * sequence once and re-emits the whole page via seg_page_fill (one pass).
 * Rationale (measured): the previous shape re-parsed every slot per read
 * and re-filled pages through repeated seg_page_insert (O(entries^2) per
 * refill) — fatal for the real-KB import's ~40M tree ops.
 *
 * Key encoding (st_codec_t): fixed `key_fix` bytes, or [u16 klen][bytes].
 * Value: none / u32 / [u16 vlen][bytes].
 * Branch entry record: [u64 child lpg+1][key as above].
 */

#include "segstore.h"
#include <string.h>

/* max slots per page (SEG_PAGE_MAX_SLOTS is private to seg_page.c) */
#define NROWS       ((SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / SEG_SLOT_SIZE)
#define ST_MAX_KEY  SEG_PAGE_MAX_REC      /* any key that can live in a page */

/* logical-edit kinds */
#define NE_NONE 0u
#define NE_INS  1u
#define NE_REP  2u
#define NE_DEL  3u

/* ---- little-endian helper (module-local; seg_page's are static) ---- */
static u64 ld64le(const u8 *p) {
    u64 v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return v;
}
static void st64le(u8 *p, u64 v) { for (int i = 0; i < 8; i++) { p[i] = (u8)v; v >>= 8; } }

/* ---- key / entry codecs ---- */

static u16 key_enc(const st_codec_t *cd, const u8 *k, u16 kl, u8 *out) {
    if (cd->key_fix) { memcpy(out, k, cd->key_fix); return cd->key_fix; }
    out[0] = (u8)kl; out[1] = (u8)(kl >> 8);
    memcpy(out + 2, k, kl);
    return (u16)(2 + kl);
}

static u16 key_from_rec(const st_codec_t *cd, const u8 *r, u16 sz, const u8 **kp) {
    if (cd->key_fix) {
        if (sz < cd->key_fix) return 0;
        *kp = r;
        return cd->key_fix;
    }
    if (sz < 2) return 0;
    u16 kl = (u16)(r[0] | (r[1] << 8));
    if ((u32)kl + 2 > sz) return 0;
    *kp = r + 2;
    return kl;
}

static int key_cmp(const u8 *a, u16 al, const u8 *b, u16 bl) {
    u16 m = al < bl ? al : bl;
    int c = m ? memcmp(a, b, m) : 0;
    if (c) return c;
    if (al == bl) return 0;
    return al < bl ? -1 : 1;              /* shorter prefix sorts first */
}

static u16 entry_enc(const st_codec_t *cd, const u8 *k, u16 kl, const u8 *v, u16 vl, u8 *out) {
    u16 n = key_enc(cd, k, kl, out);
    if (cd->val_mode == 1) { memcpy(out + n, v, 4); n += 4; }
    else if (cd->val_mode == 2) {
        out[n] = (u8)vl; out[n + 1] = (u8)(vl >> 8);
        memcpy(out + n + 2, v, vl);
        n += (u16)(2 + vl);
    }
    return n;
}

static int val_from_rec(const st_codec_t *cd, const u8 *r, u16 sz, const u8 **vp, u16 *vlp) {
    const u8 *kp = NULL;
    u16 kl = key_from_rec(cd, r, sz, &kp);
    u32 off = (u32)kl + (cd->key_fix ? 0u : 2u);
    if (off > sz) { *vlp = 0; *vp = NULL; return 0; }
    if (cd->val_mode == 1) {
        if (sz - off < 4) { *vlp = 0; *vp = NULL; return 0; }
        *vp = r + off; *vlp = 4;
        return 1;
    }
    if (cd->val_mode == 2) {
        if (sz - off < 2) { *vlp = 0; *vp = NULL; return 0; }
        u16 vl = (u16)(r[off] | (r[off + 1] << 8));
        if (sz - off - 2 < vl) { *vlp = 0; *vp = NULL; return 0; }
        *vp = r + off + 2; *vlp = vl;
        return 1;
    }
    *vp = NULL; *vlp = 0;
    return 0;
}

static u16 branch_enc(u64 child, const st_codec_t *cd, const u8 *k, u16 kl, u8 *out) {
    st64le(out, child);
    return (u16)(8 + key_enc(cd, k, kl, out + 8));
}

static u16 branch_key(const st_codec_t *cd, const u8 *r, u16 sz, const u8 **kp) {
    if (sz < 8) return 0;
    return key_from_rec(cd, r + 8, (u16)(sz - 8), kp);
}

/* ---- node_t: one page image + one pending logical edit ---- */

typedef struct {
    u8  img[SEG_PAGE_SIZE];       /* snapshot of the node's page */
    u16 kind;
    u32 n;                        /* slots in img */
    u32 edit;                     /* NE_* mapped over the original slots */
    u32 pos;                      /* edit index (logical) */
    u8  addbuf[ST_MAX_KEY + 8];   /* pending entry for NE_INS / NE_REP */
    u16 addlen;
    int patch;                    /* 1 = logical entry patch_pos carries patchbuf */
    u32 patch_pos;
    u8  patchbuf[8 + ST_MAX_KEY];
    u16 patchlen;
} node_t;

static int node_read(segstore_t *gs, u32 lpg, node_t *nd) {
    const u8 *pg = seg_txn_view(gs, lpg);
    if (!pg) return 0;
    nd->kind = seg_page_kind(pg);
    memcpy(nd->img, pg, SEG_PAGE_SIZE);
    nd->n = seg_page_nslots(pg);
    nd->edit = NE_NONE;
    nd->pos = 0;
    nd->addlen = 0;
    nd->patch = 0;
    nd->patchlen = 0;
    return nd->n <= NROWS;
}

/* logical entry count after the pending edit */
static u32 node_len(const node_t *nd) {
    if (nd->edit == NE_INS) return nd->n + 1;
    if (nd->edit == NE_DEL) return nd->n ? nd->n - 1 : 0;
    return nd->n;
}

/* logical entry k -> (record, size): the original slot, the pending entry,
 * or the patched bytes. */
static const u8 *node_entry(const node_t *nd, u32 k, u16 *sz) {
    if (nd->patch && k == nd->patch_pos) { *sz = nd->patchlen; return nd->patchbuf; }
    u32 s = k;
    if (nd->edit == NE_INS) {
        if (k == nd->pos) { *sz = nd->addlen; return nd->addbuf; }
        if (k > nd->pos) s = k - 1;
    } else if (nd->edit == NE_REP) {
        if (k == nd->pos) { *sz = nd->addlen; return nd->addbuf; }
    } else if (nd->edit == NE_DEL) {
        if (k >= nd->pos) s = k + 1;
    }
    return seg_page_read(nd->img, (u16)s, sz);
}

/* key of logical entry k (branch entries skip the child ref) */
static u16 node_key(const st_codec_t *cd, const node_t *nd, u32 k, const u8 **kp) {
    u16 sz = 0;
    const u8 *r = node_entry(nd, k, &sz);
    if (!r) return 0;
    if (nd->kind == SEG_KIND_TREE_BRANCH) return branch_key(cd, r, sz, kp);
    return key_from_rec(cd, r, sz, kp);
}

static u32 range_bytes(const node_t *nd, u32 from, u32 to) {
    u32 t = SEG_PAGE_HDR_SIZE;
    for (u32 k = from; k < to; k++) {
        u16 sz = 0;
        if (!node_entry(nd, k, &sz)) return 0xFFFFFFFFu;
        t += (u32)sz + SEG_SLOT_SIZE;
    }
    return t;
}
static u32 node_bytes(const node_t *nd) { return range_bytes(nd, 0, node_len(nd)); }
static int node_fits(const node_t *nd) { return node_bytes(nd) <= SEG_PAGE_SIZE; }

/* Pick a split point over the edited sequence; prefers mid, falls back to any
 * point where both halves fit. 0 = unsplittable. */
static int split_mid(const node_t *nd, u32 *mid_out) {
    u32 n = node_len(nd);
    u32 mid = n / 2;
    if (mid >= 1 &&
        range_bytes(nd, 0, mid) <= SEG_PAGE_SIZE && range_bytes(nd, mid, n) <= SEG_PAGE_SIZE) {
        *mid_out = mid;
        return 1;
    }
    u32 total = range_bytes(nd, 0, n);
    u32 pre = SEG_PAGE_HDR_SIZE;
    for (u32 k = 1; k < n; k++) {
        u16 sz = 0;
        if (!node_entry(nd, k - 1, &sz)) return 0;
        pre += (u32)sz + SEG_SLOT_SIZE;
        if (k == mid) continue;
        u32 suf = total - pre + SEG_PAGE_HDR_SIZE;
        if (pre <= SEG_PAGE_SIZE && suf <= SEG_PAGE_SIZE) { *mid_out = k; return 1; }
    }
    return 0;
}

/* re-emit [from,to) of the edited sequence into a page */
static int page_write(segstore_t *gs, u32 lpg, u16 kind,
                      const u8 *const *recs, const u16 *sizes, u32 m) {
    u8 *pg = seg_txn_touch(gs, lpg);
    if (!pg) return 0;
    return seg_page_fill(pg, kind, recs, sizes, m);
}

static int node_write(segstore_t *gs, u32 lpg, u16 kind, const node_t *nd, u32 from, u32 to) {
    const u8 *rp[NROWS + 1];
    u16 rz[NROWS + 1];
    u32 m = to - from;
    if (m > NROWS + 1) return 0;
    for (u32 k = 0; k < m; k++) {
        u16 sz = 0;
        const u8 *r = node_entry(nd, from + k, &sz);
        if (!r || !sz) return 0;
        rp[k] = r;
        rz[k] = sz;
    }
    return page_write(gs, lpg, kind, rp, rz, m);
}

static int node_kind(segstore_t *gs, u32 lpg) {
    const u8 *pg = seg_txn_view(gs, lpg);
    return pg ? (int)seg_page_kind(pg) : -1;
}

static u16 node_first_key(segstore_t *gs, u32 lpg, const st_codec_t *cd, u8 *out) {
    node_t nd;
    if (!node_read(gs, lpg, &nd) || nd.n == 0) return 0;
    const u8 *kp = NULL;
    u16 kl = node_key(cd, &nd, 0, &kp);
    if (!kp || kl > ST_MAX_KEY) return 0;
    memcpy(out, kp, kl);
    return kl;
}

/* first index with key >= k (leaf-style search) */
static u32 node_lower(const st_codec_t *cd, const node_t *nd, const u8 *k, u16 kl, int *hit) {
    u32 lo = 0, hi = nd->n;
    *hit = 0;
    while (lo < hi) {
        u32 mid = (lo + hi) / 2;
        const u8 *kp = NULL;
        u16 rkl = node_key(cd, nd, mid, &kp);
        if (key_cmp(kp, rkl, k, kl) < 0) lo = mid + 1;
        else hi = mid;
    }
    if (lo < nd->n) {
        const u8 *kp = NULL;
        u16 rkl = node_key(cd, nd, lo, &kp);
        *hit = key_cmp(kp, rkl, k, kl) == 0;
    }
    return lo;
}

/* child index for descent: last entry with key <= k (keys below s_1 go left) */
static u32 node_child(const st_codec_t *cd, const node_t *nd, const u8 *k, u16 kl) {
    u32 lo = 0, hi = nd->n;
    while (lo < hi) {
        u32 mid = (lo + hi) / 2;
        const u8 *kp = NULL;
        u16 rkl = node_key(cd, nd, mid, &kp);
        if (key_cmp(kp, rkl, k, kl) <= 0) lo = mid + 1;
        else hi = mid;
    }
    return lo ? lo - 1 : 0;
}

/* ---- insert ---- */

/* 1 = split (sib_out/sep_out set); 0 = done (or dup via *dup); -1 = error. */
static int ins_rec(seg_tree_t *t, u32 lpg, const u8 *erec, u16 esz,
                   u32 *sib_out, u8 *sep_out, u16 *seplen_out, int *dup) {
    node_t nd;
    if (!node_read(t->gs, lpg, &nd) ||
        (nd.kind != SEG_KIND_TREE_LEAF && nd.kind != SEG_KIND_TREE_BRANCH))
        return -1;

    if (nd.kind == SEG_KIND_TREE_LEAF) {
        const u8 *nk = NULL;
        u16 nkl = key_from_rec(&t->cd, erec, esz, &nk);
        if (esz > (u16)sizeof nd.addbuf) return -1;
        memcpy(nd.addbuf, erec, esz);
        nd.addlen = esz;
        int hit = 0;
        u32 pos = node_lower(&t->cd, &nd, nk, nkl, &hit);
        if (hit) {
            if (t->cd.val_mode == 0) { *dup = 1; return 0; }
            *dup = 1;
            nd.edit = NE_REP; nd.pos = pos;
            if (!node_fits(&nd) || !node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &nd, 0, node_len(&nd)))
                return -1;
            return 0;
        }
        nd.edit = NE_INS; nd.pos = pos;
        if (node_fits(&nd)) {
            if (!node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &nd, 0, node_len(&nd))) return -1;
            return 0;
        }
        u32 mid = 0;
        if (!split_mid(&nd, &mid)) return -1;
        u16 rks = 0;
        const u8 *rk = node_entry(&nd, mid, &rks);
        const u8 *kp = NULL;
        u16 rkl = rk ? key_from_rec(&t->cd, rk, rks, &kp) : 0;
        if (!kp || rkl > ST_MAX_KEY) return -1;
        memcpy(sep_out, kp, rkl);               /* KEY bytes only */
        *seplen_out = rkl;
        u32 nlen = node_len(&nd);
        u32 sib = 0;
        u8 *spg = seg_txn_alloc(t->gs, SEG_KIND_TREE_LEAF, &sib);
        int ok = spg && node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &nd, 0, mid)
                     && node_write(t->gs, sib, SEG_KIND_TREE_LEAF, &nd, mid, nlen);
        *sib_out = sib + 1;
        return ok ? 1 : -1;
    }

    /* branch */
    const u8 *nk = NULL;
    u16 nkl = key_from_rec(&t->cd, erec, esz, &nk);
    u32 i = node_child(&t->cd, &nd, nk, nkl);
    u16 zz = 0;
    u64 child = ld64le(node_entry(&nd, i, &zz)) - 1;
    int r = ins_rec(t, (u32)child, erec, esz, sib_out, sep_out, seplen_out, dup);
    if (r <= 0) return r;

    if ((size_t)(8 + 2 + *seplen_out) > sizeof nd.addbuf) return -1;
    nd.addlen = branch_enc(*sib_out, &t->cd, sep_out, *seplen_out, nd.addbuf);
    nd.edit = NE_INS; nd.pos = i + 1;
    if (node_fits(&nd)) {
        if (!node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &nd, 0, node_len(&nd))) return -1;
        return 0;
    }
    u32 mid = 0;
    if (!split_mid(&nd, &mid)) return -1;
    u16 rks = 0;
    const u8 *rk = node_entry(&nd, mid, &rks);
    const u8 *kp = NULL;
    u16 rkl = rk ? branch_key(&t->cd, rk, rks, &kp) : 0;
    if (!kp || rkl > ST_MAX_KEY) return -1;
    memcpy(sep_out, kp, rkl);                   /* KEY bytes only (skip ref) */
    *seplen_out = rkl;
    u32 nlen = node_len(&nd);
    u32 sib = 0;
    u8 *bpg = seg_txn_alloc(t->gs, SEG_KIND_TREE_BRANCH, &sib);
    int ok = bpg && node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &nd, 0, mid)
                 && node_write(t->gs, sib, SEG_KIND_TREE_BRANCH, &nd, mid, nlen);
    *sib_out = sib + 1;
    return ok ? 1 : -1;
}

int seg_tree_insert(seg_tree_t *t, const u8 *k, u16 klen, const u8 *v, u16 vlen) {
    u8 erec[2 + ST_MAX_KEY + 2 + ST_MAX_KEY];
    if ((size_t)2 + klen + 2 + vlen > sizeof erec) return -1;
    u16 esz = entry_enc(&t->cd, k, klen, v, vlen, erec);
    if (esz > SEG_PAGE_MAX_REC) return -1;          /* can never fit a node */

    if (t->root == 0) {
        u32 lpg = 0;
        u8 *pg = seg_txn_alloc(t->gs, SEG_KIND_TREE_LEAF, &lpg);
        if (!pg) return -1;
        const u8 *rp[1] = { erec };
        const u16 rz[1] = { esz };
        if (!seg_page_fill(pg, SEG_KIND_TREE_LEAF, rp, rz, 1)) return -1;
        t->root = lpg + 1;
        t->count = 1;
        return 1;
    }

    u32 sib = 0;
    u8 sep[ST_MAX_KEY];
    u16 seplen = 0;
    int dup = 0;
    int r = ins_rec(t, t->root - 1, erec, esz, &sib, sep, &seplen, &dup);
    if (r < 0) return -1;
    if (dup) return 0;
    if (r == 0) { t->count++; return 1; }

    /* root split: new branch root over (old root @ its first key, sib @ sep).
     * Two entries need two stable buffers — no single-pending trick here. */
    u8 fk[ST_MAX_KEY];
    u16 fkl = node_first_key(t->gs, t->root - 1, &t->cd, fk);
    if (!fkl) return -1;
    u32 newroot = 0;
    u8 *rpg = seg_txn_alloc(t->gs, SEG_KIND_TREE_BRANCH, &newroot);
    if (!rpg) return -1;
    u8 eb0[8 + ST_MAX_KEY], eb1[8 + ST_MAX_KEY];
    const u8 *rp2[2];
    u16 rz2[2];
    rp2[0] = eb0; rz2[0] = branch_enc((u64)t->root, &t->cd, fk, fkl, eb0);
    rp2[1] = eb1; rz2[1] = branch_enc((u64)sib, &t->cd, sep, seplen, eb1);
    if (!seg_page_fill(rpg, SEG_KIND_TREE_BRANCH, rp2, rz2, 2)) return -1;
    t->root = newroot + 1;
    t->count++;
    return 1;
}

/* ---- delete ---- */

/* 0 = absent; 1 = changed, self-contained; 2 = changed-or-underfull (parent
 * may merge); 3 = emptied AND FREED (parent must drop the entry); -1 = error. */
static int del_rec(seg_tree_t *t, u32 lpg, const u8 *k, u16 kl) {
    node_t nd;
    if (!node_read(t->gs, lpg, &nd) ||
        (nd.kind != SEG_KIND_TREE_LEAF && nd.kind != SEG_KIND_TREE_BRANCH))
        return -1;

    if (nd.kind == SEG_KIND_TREE_LEAF) {
        int hit = 0;
        u32 pos = node_lower(&t->cd, &nd, k, kl, &hit);
        if (!hit) return 0;
        if (nd.n <= 1) {                          /* the sole entry goes: free */
            seg_txn_free(t->gs, lpg);
            return 3;
        }
        nd.edit = NE_DEL; nd.pos = pos;
        if (!node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &nd, 0, node_len(&nd))) return -1;
        return node_bytes(&nd) < SEG_PAGE_SIZE / 4 ? 2 : 1;
    }

    /* branch */
    u32 i = node_child(&t->cd, &nd, k, kl);
    u16 zz = 0;
    u64 child = ld64le(node_entry(&nd, i, &zz)) - 1;
    int r = del_rec(t, (u32)child, k, kl);
    if (r <= 0) return r;

    int changed = 0;
    if (r == 3) {                          /* child emptied + freed: drop entry */
        if (nd.n <= 1) {
            seg_txn_free(t->gs, lpg);
            return 3;
        }
        nd.edit = NE_DEL; nd.pos = i;
        changed = 1;
    } else if (r == 2) {                   /* underfull: single-pass sibling merge */
        int ckind = node_kind(t->gs, (u32)child);
        node_t cr;
        if (ckind < 0 || !node_read(t->gs, (u32)child, &cr)) return -1;
        u32 j = (i + 1 < nd.n) ? i + 1 : (i > 0 ? i - 1 : (u32)-1);
        if (j != (u32)-1) {
            u16 zz2 = 0;
            u32 slpg = (u32)(ld64le(node_entry(&nd, j, &zz2)) - 1);
            node_t sr;
            if (node_kind(t->gs, slpg) == ckind && node_read(t->gs, slpg, &sr)) {
                const node_t *first = (j == i + 1) ? &cr : &sr;
                const node_t *second = (j == i + 1) ? &sr : &cr;
                u32 m = first->n + second->n;
                const u8 *rp[NROWS + 1];
                u16 rz[NROWS + 1];
                if (m > NROWS + 1) m = 0;             /* oversized: stay sparse */
                u32 total = SEG_PAGE_HDR_SIZE + m * SEG_SLOT_SIZE;
                u32 x = 0;
                int bad = (m == 0);
                for (u32 a = 0; a < first->n && !bad; a++) {
                    u16 sz = 0;
                    const u8 *r2 = seg_page_read(first->img, (u16)a, &sz);
                    if (!r2 || !sz) { bad = 1; break; }
                    rp[x] = r2; rz[x] = sz; x++;
                    total += (u32)sz;
                }
                for (u32 a = 0; a < second->n && !bad; a++) {
                    u16 sz = 0;
                    const u8 *r2 = seg_page_read(second->img, (u16)a, &sz);
                    if (!r2 || !sz) { bad = 1; break; }
                    rp[x] = r2; rz[x] = sz; x++;
                    total += (u32)sz;
                }
                if (!bad && total <= SEG_PAGE_SIZE &&
                    page_write(t->gs, (u32)child, (u16)ckind, rp, rz, m)) {
                    seg_txn_free(t->gs, slpg);
                    nd.edit = NE_DEL; nd.pos = j;
                    changed = 1;
                    if (j == i - 1) {
                        /* child merged with its LEFT sibling: its first key
                         * changed; after the delete its entry is logical j. */
                        const u8 *fk = NULL;
                        u16 fkl = (ckind == SEG_KIND_TREE_BRANCH)
                                ? branch_key(&t->cd, rp[0], rz[0], &fk)
                                : key_from_rec(&t->cd, rp[0], rz[0], &fk);
                        if (!fk || fkl > ST_MAX_KEY) return -1;
                        nd.patch = 1; nd.patch_pos = j;
                        nd.patchlen = branch_enc(child + 1, &t->cd, fk, fkl, nd.patchbuf);
                    }
                }
            }
        }
    }

    if (changed &&
        !node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &nd, 0, node_len(&nd))) return -1;
    return node_bytes(&nd) < SEG_PAGE_SIZE / 4 ? 2 : 1;
}

int seg_tree_delete(seg_tree_t *t, const u8 *k, u16 klen) {
    if (t->root == 0) return 0;
    int r = del_rec(t, t->root - 1, k, klen);
    if (r < 0) return -1;
    if (r == 0) return 0;
    if (r == 3) {                          /* the root page emptied; del_rec
                                            * already freed it */
        t->root = 0;
        t->count = t->count ? t->count - 1 : 0;
        return 1;
    }

    /* normalize the root: collapse single-child branches */
    for (;;) {
        if (t->root == 0) break;
        node_t nd;
        if (!node_read(t->gs, t->root - 1, &nd)) return -1;
        if (nd.n == 0) { seg_txn_free(t->gs, t->root - 1); t->root = 0; break; }
        if (nd.kind == SEG_KIND_TREE_LEAF) break;
        if (nd.n == 1) {
            u16 zz = 0;
            u32 c = (u32)(ld64le(node_entry(&nd, 0, &zz)) - 1);
            seg_txn_free(t->gs, t->root - 1);
            t->root = c + 1;
            continue;                       /* the child may itself be a 1-child branch */
        }
        break;
    }
    t->count = t->count ? t->count - 1 : 0;
    return 1;
}

/* ---- lookup ---- */

int seg_tree_lookup(seg_tree_t *t, const u8 *k, u16 klen, u8 *v, u16 *vlen) {
    if (!t->root) return 0;
    u32 lpg = t->root - 1;
    for (;;) {
        node_t nd;
        if (!node_read(t->gs, lpg, &nd)) return -1;
        if (nd.kind == SEG_KIND_TREE_LEAF) {
            int hit = 0;
            u32 pos = node_lower(&t->cd, &nd, k, klen, &hit);
            if (!hit) return 0;
            u16 sz = 0;
            const u8 *r = node_entry(&nd, pos, &sz);
            const u8 *vp = NULL; u16 vl = 0;
            int has = r && val_from_rec(&t->cd, r, sz, &vp, &vl);
            if (vlen) {
                if (has && v) {
                    u16 n = vl < *vlen ? vl : *vlen;
                    memcpy(v, vp, n);
                    *vlen = n;
                } else {
                    *vlen = has ? vl : 0;
                }
            }
            return 1;
        }
        u32 i = node_child(&t->cd, &nd, k, klen);
        u16 zz = 0;
        lpg = (u32)(ld64le(node_entry(&nd, i, &zz)) - 1);
    }
}

/* ---- ordered scan ---- */

typedef struct {
    seg_tree_t *t;
    const u8 *lo; u16 lol; int lo_incl;
    const u8 *hi; u16 hil; int hi_incl;
    int (*cb)(void *, const u8 *, u16, const u8 *, u16);
    void *ctx;
    int stop;
} scan_ctx_t;

static int scan_node(scan_ctx_t *sc, u32 lpg) {
    if (sc->stop) return 0;
    node_t nd;
    if (!node_read(sc->t->gs, lpg, &nd)) return -1;
    if (nd.kind == SEG_KIND_TREE_LEAF) {
        int hit = 0;
        u32 pos = sc->lo ? node_lower(&sc->t->cd, &nd, sc->lo, sc->lol, &hit) : 0;
        for (u32 x = pos; x < nd.n; x++) {
            u16 sz = 0;
            const u8 *r = node_entry(&nd, x, &sz);
            const u8 *kp = NULL;
            u16 kl = r ? key_from_rec(&sc->t->cd, r, sz, &kp) : 0;
            if (sc->lo) {
                int c = key_cmp(kp, kl, sc->lo, sc->lol);
                if (c < 0 || (c == 0 && !sc->lo_incl)) continue;
            }
            if (sc->hi) {
                int c = key_cmp(kp, kl, sc->hi, sc->hil);
                if (c > 0 || (c == 0 && !sc->hi_incl)) break;
            }
            const u8 *vp = NULL; u16 vl = 0;
            if (r) (void)val_from_rec(&sc->t->cd, r, sz, &vp, &vl);
            if (sc->cb(sc->ctx, kp, kl, vp, vl) == 0) { sc->stop = 1; break; }
        }
        return 0;
    }
    for (u32 i = 0; i < nd.n && !sc->stop; i++) {
        u16 sz = 0;
        const u8 *r = node_entry(&nd, i, &sz);
        if (i > 0 && sc->hi) {
            const u8 *sk = NULL;
            u16 skl = branch_key(&sc->t->cd, r, sz, &sk);
            int c = key_cmp(sk, skl, sc->hi, sc->hil);
            if (c > 0 || (c == 0 && !sc->hi_incl)) break;
        }
        if (sc->lo && i + 1 < nd.n) {
            /* child i's keys are all < s_{i+1}: skip when the NEXT separator
             * is <= lo (nothing in this subtree can reach the lower bound). */
            u16 sz2 = 0;
            const u8 *r2 = node_entry(&nd, i + 1, &sz2);
            const u8 *sk = NULL;
            u16 skl = branch_key(&sc->t->cd, r2, sz2, &sk);
            if (key_cmp(sk, skl, sc->lo, sc->lol) <= 0) continue;
        }
        u64 child = ld64le(r) - 1;
        if (scan_node(sc, (u32)child) < 0) return -1;
    }
    return 0;
}

int seg_tree_scan(seg_tree_t *t,
                  const u8 *lo, u16 lol, int lo_incl,
                  const u8 *hi, u16 hil, int hi_incl,
                  int (*cb)(void *, const u8 *, u16, const u8 *, u16),
                  void *ctx) {
    if (t->root == 0) return 0;
    scan_ctx_t sc = { t, lo, lol, lo_incl, hi, hil, hi_incl, cb, ctx, 0 };
    int r = scan_node(&sc, t->root - 1);
    if (r < 0) return -1;
    return sc.stop ? 1 : 0;
}

void seg_tree_open(seg_tree_t *t, segstore_t *gs, st_codec_t cd, u32 root) {
    t->gs = gs;
    t->cd = cd;
    t->root = root;
    t->count = 0;                          /* graph4 restores counts via META */
}

u32 seg_tree_root(const seg_tree_t *t) { return t->root; }
u64 seg_tree_count(const seg_tree_t *t) { return t->count; }

