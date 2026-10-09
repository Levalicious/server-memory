/*
 * seg_tree.c — the generic B+tree over slotted pages.
 * (docs/v4-index-design-note.md / Design_IndexRedesign_2026_10_09;
 *  Decision_Lev_ReconciliationRulings_2026_10_09 ruling 1+2.)
 *
 * Invariants:
 *   - Entries in every node are stored in key order (slots 0..n-1 ascending).
 *     Tree pages never contain dead slots (their life is: init + ordered
 *     inserts; a mutation re-inits and refills the node's COW working copy).
 *   - Published pages are never mutated: writes go through seg_txn_touch/
 *     seg_txn_alloc and are committed by the caller's txn.
 *   - Node type lives in the page KIND (SEG_KIND_TREE_LEAF/BRANCH), which —
 *     like flags — survives compact/touch.
 *   - Node refs are lpg+1; 0 = no node.
 *   - No minimum fill for the root; a branch with a single child is
 *     collapsed by the root wrapper. Underfull non-root leaves are merged
 *     with an adjacent sibling when the pair fits; otherwise they are left
 *     sparse (deliberate, per the design note).
 *
 * Key encoding (st_codec_t): fixed `key_fix` bytes, or [u16 klen][bytes].
 * Value: none / u32 / [u16 vlen][bytes].
 * Branch entry record: [u64 child lpg+1][key as above].
 */

#include "segstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ST_MAX_KEY  SEG_PAGE_MAX_REC      /* any key that can live in a page */

/* ---- little-endian helpers (module-local; seg_page's are static) ---- */
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

/* key inside a record: fixed keys at offset 0, variable as [u16][bytes]. */
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

/* value out of a record (val_mode 1/2); 1 = has value, 0 = posting. */
static int val_from_rec(const st_codec_t *cd, const u8 *r, u16 sz, const u8 **vp, u16 *vlp) {
    const u8 *kp = NULL;
    u16 kl = key_from_rec(cd, r, sz, &kp);
    if (!kl && cd->key_fix) return 0;     /* malformed; callers treat via lookup miss */
    u32 off = (u32)kl + (cd->key_fix ? 0 : 2);
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

/* ---- rows: a node's live entries, copied out (never alias page memory) ---- */

typedef struct { u8 **r; u16 *z; u32 n, cap; } rows_t;

static void rows_free(rows_t *rows) {
    if (rows->r) { for (u32 i = 0; i < rows->n; i++) free(rows->r[i]); }
    free(rows->r); free(rows->z);
    rows->r = NULL; rows->z = NULL; rows->n = 0;
}

/* free only the row ARRAYS; entry buffers were moved/aliased elsewhere. */
static void rows_detach(rows_t *rows) {
    free(rows->r); free(rows->z);
    rows->r = NULL; rows->z = NULL; rows->n = 0;
}

static int rows_read(segstore_t *gs, u32 lpg, rows_t *rows) {
    memset(rows, 0, sizeof *rows);
    const u8 *pg = seg_txn_view(gs, lpg);
    if (!pg) return 0;
    u16 n = seg_page_nslots(pg);
    rows->cap = n ? n : 1;
    rows->r = (u8 **)malloc(sizeof(u8 *) * rows->cap);
    rows->z = (u16 *)malloc(sizeof(u16) * rows->cap);
    if (!rows->r || !rows->z) return 0;
    for (u16 s = 0; s < n; s++) {
        u16 sz = 0;
        const u8 *rec = seg_page_read(pg, s, &sz);
        if (!rec || !sz) return 0;        /* tree pages never hold dead slots */
        u8 *cp = (u8 *)malloc(sz);
        if (!cp) return 0;
        memcpy(cp, rec, sz);
        rows->r[rows->n] = cp;
        rows->z[rows->n] = sz;
        rows->n++;
    }
    return 1;
}

static u32 rows_bytes(const rows_t *rows) {
    u32 t = SEG_PAGE_HDR_SIZE;
    for (u32 i = 0; i < rows->n; i++) t += (u32)rows->z[i] + SEG_SLOT_SIZE;
    return t;
}
static int rows_fit(const rows_t *rows) { return rows_bytes(rows) <= SEG_PAGE_SIZE; }

/* dst gets a COPY of rec (callers keep ownership of their buffers). */
static void rows_insert_at(rows_t *dst, const rows_t *src, u32 pos, const u8 *rec, u16 sz) {
    dst->n = src->n + 1;
    dst->cap = dst->n;
    dst->r = (u8 **)malloc(sizeof(u8 *) * dst->cap);
    dst->z = (u16 *)malloc(sizeof(u16) * dst->cap);
    for (u32 i = 0; i < pos; i++) { dst->r[i] = src->r[i]; dst->z[i] = src->z[i]; }
    dst->r[pos] = (u8 *)malloc(sz);
    if (dst->r[pos]) memcpy(dst->r[pos], rec, sz);
    dst->z[pos] = sz;
    for (u32 i = pos; i < src->n; i++) { dst->r[i + 1] = src->r[i]; dst->z[i + 1] = src->z[i]; }
}

static void rows_split(rows_t *src, u32 mid, rows_t *l, rows_t *r) {
    l->n = mid; l->cap = mid ? mid : 1;
    r->n = src->n - mid; r->cap = r->n ? r->n : 1;
    l->r = (u8 **)malloc(sizeof(u8 *) * l->cap); l->z = (u16 *)malloc(sizeof(u16) * l->cap);
    r->r = (u8 **)malloc(sizeof(u8 *) * r->cap); r->z = (u16 *)malloc(sizeof(u16) * r->cap);
    for (u32 i = 0; i < mid; i++) { l->r[i] = src->r[i]; l->z[i] = src->z[i]; }
    for (u32 i = mid; i < src->n; i++) { r->r[i - mid] = src->r[i]; r->z[i - mid] = src->z[i]; }
    src->n = 0;                            /* ownership moved */
}

static int node_write(segstore_t *gs, u32 lpg, u16 kind, const rows_t *rows) {
    u8 *pg = seg_txn_touch(gs, lpg);
    if (!pg) return 0;
    seg_page_init(pg, kind);
    for (u32 i = 0; i < rows->n; i++) {
        u16 slot = 0;
        if (!seg_page_insert(pg, rows->r[i], rows->z[i], &slot) || slot != i) return 0;
    }
    return 1;
}

static int node_kind(segstore_t *gs, u32 lpg) {
    const u8 *pg = seg_txn_view(gs, lpg);
    return pg ? (int)seg_page_kind(pg) : -1;
}

/* first key of a node (leaf entry 0, or branch entry 0's separator). */
static u16 node_first_key(segstore_t *gs, u32 lpg, const st_codec_t *cd, u8 *out) {
    const u8 *pg = seg_txn_view(gs, lpg);
    if (!pg) return 0;
    const u8 *rec = seg_page_read(pg, 0, NULL);
    u16 sz = 0;
    rec = seg_page_read(pg, 0, &sz);
    if (!rec) return 0;
    const u8 *kp = NULL;
    u16 kl = (seg_page_kind(pg) == SEG_KIND_TREE_BRANCH)
           ? branch_key(cd, rec, sz, &kp)
           : key_from_rec(cd, rec, sz, &kp);
    if (!kl) return 0;
    memcpy(out, kp, kl);
    return kl;
}

/* first index with key >= k (leaf) / > k (branch: last <= k is the child). */
static u32 rows_lower(const st_codec_t *cd, const rows_t *rows, int branch,
                      const u8 *k, u16 kl, int *hit) {
    u32 lo = 0, hi = rows->n;
    *hit = 0;
    while (lo < hi) {
        u32 mid = (lo + hi) / 2;
        const u8 *kp = NULL;
        u16 rkl = branch ? branch_key(cd, rows->r[mid], rows->z[mid], &kp)
                         : key_from_rec(cd, rows->r[mid], rows->z[mid], &kp);
        int c = key_cmp(kp, rkl, k, kl);
        if (c < 0) lo = mid + 1;
        else hi = mid;
    }
    if (lo < rows->n) {
        const u8 *kp = NULL;
        u16 rkl = branch ? branch_key(cd, rows->r[lo], rows->z[lo], &kp)
                         : key_from_rec(cd, rows->r[lo], rows->z[lo], &kp);
        *hit = key_cmp(kp, rkl, k, kl) == 0;
    }
    return lo;
}

static u32 branch_child(const st_codec_t *cd, const rows_t *rows, const u8 *k, u16 kl) {
    u32 lo = 0, hi = rows->n;
    while (lo < hi) {
        u32 mid = (lo + hi) / 2;
        const u8 *kp = NULL;
        u16 rkl = branch_key(cd, rows->r[mid], rows->z[mid], &kp);
        if (key_cmp(kp, rkl, k, kl) <= 0) lo = mid + 1;
        else hi = mid;
    }
    return lo ? lo - 1 : 0;               /* keys below the first separator go left */
}

/* ---- insert ---- */

/* 1 = split (sib_out/sep_out set); 0 = done (or dup via *dup); -1 = error. */
static int ins_rec(seg_tree_t *t, u32 lpg, u8 *erec, u16 esz,
                   u32 *sib_out, u8 *sep_out, u16 *seplen_out, int *dup) {
    int kind = node_kind(t->gs, lpg);
    rows_t rows;
    if ((kind != SEG_KIND_TREE_LEAF && kind != SEG_KIND_TREE_BRANCH) ||
        !rows_read(t->gs, lpg, &rows)) {
        rows_free(&rows);
        return -1;
    }

    if (kind == SEG_KIND_TREE_LEAF) {
        const u8 *nk = NULL;
        u16 nkl = key_from_rec(&t->cd, erec, esz, &nk);
        if (!nkl && !t->cd.key_fix) { rows_free(&rows); return -1; }
        int hit = 0;
        u32 pos = rows_lower(&t->cd, &rows, 0, nk, nkl, &hit);
        if (hit) {
            *dup = 1;                     /* key already present */
            if (t->cd.val_mode == 0) { rows_free(&rows); return 0; }
            free(rows.r[pos]);
            rows.r[pos] = (u8 *)malloc(esz);
            if (!rows.r[pos]) { rows_free(&rows); return -1; }
            memcpy(rows.r[pos], erec, esz);
            rows.z[pos] = esz;
            int ok = rows_fit(&rows) && node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &rows);
            rows_free(&rows);
            return ok ? 0 : -1;
        }
        rows_t nrows;
        rows_insert_at(&nrows, &rows, pos, erec, esz);
        rows_detach(&rows);               /* entries moved into nrows */
        if (rows_fit(&nrows)) {
            int ok = node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &nrows);
            rows_free(&nrows);
            return ok ? 0 : -1;
        }
        u32 mid = nrows.n / 2;
        rows_t L, R;
        rows_split(&nrows, mid, &L, &R);
        rows_free(&nrows);
        u32 sib = 0;
        u8 *spg = seg_txn_alloc(t->gs, SEG_KIND_TREE_LEAF, &sib);
        int ok = spg && node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &L)
                     && node_write(t->gs, sib, SEG_KIND_TREE_LEAF, &R);
        const u8 *rk = NULL;
        u16 rkl = key_from_rec(&t->cd, R.r[0], R.z[0], &rk);
        memcpy(sep_out, rk, rkl);
        *seplen_out = rkl;
        *sib_out = sib + 1;
        rows_free(&L); rows_free(&R);
        return ok ? 1 : -1;
    }

    /* branch */
    const u8 *nk = NULL;
    u16 nkl = key_from_rec(&t->cd, erec, esz, &nk);
    u32 i = branch_child(&t->cd, &rows, nk, nkl);
    u64 child = ld64le(rows.r[i]) - 1;
    int r = ins_rec(t, (u32)child, erec, esz, sib_out, sep_out, seplen_out, dup);
    if (r <= 0) { rows_free(&rows); return r; }

    u8 brec[8 + 2 + ST_MAX_KEY];
    u16 bsz = branch_enc(*sib_out, &t->cd, sep_out, *seplen_out, brec);

    rows_t nrows;
    rows_insert_at(&nrows, &rows, i + 1, brec, bsz);
    rows_detach(&rows);                   /* entries moved into nrows */
    if (rows_fit(&nrows)) {
        int ok = node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &nrows);
        rows_free(&nrows);
        return ok ? 0 : -1;
    }
    u32 mid = nrows.n / 2;
    rows_t L, R;
    rows_split(&nrows, mid, &L, &R);
    rows_free(&nrows);
    u32 sib = 0;
    u8 *bpg = seg_txn_alloc(t->gs, SEG_KIND_TREE_BRANCH, &sib);
    int ok = bpg && node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &L)
                 && node_write(t->gs, sib, SEG_KIND_TREE_BRANCH, &R);
    const u8 *rk = NULL;
    u16 rkl = branch_key(&t->cd, R.r[0], R.z[0], &rk);
    memcpy(sep_out, rk, rkl);
    *seplen_out = rkl;
    *sib_out = sib + 1;
    rows_free(&L); rows_free(&R);
    return ok ? 1 : -1;
}

int seg_tree_insert(seg_tree_t *t, const u8 *k, u16 klen, const u8 *v, u16 vlen) {
    u8 erec[2 + ST_MAX_KEY + 2 + ST_MAX_KEY];
    if ((size_t)2 + klen + 2 + vlen > sizeof erec) return -1;
    u16 esz = entry_enc(&t->cd, k, klen, v, vlen, erec);

    if (t->root == 0) {
        u32 lpg = 0;
        u8 *pg = seg_txn_alloc(t->gs, SEG_KIND_TREE_LEAF, &lpg);
        u8 *rec = (u8 *)malloc(esz);
        if (!pg || !rec) { free(rec); return -1; }
        memcpy(rec, erec, esz);
        rows_t one;
        memset(&one, 0, sizeof one);
        one.n = one.cap = 1;
        one.r = (u8 **)malloc(sizeof(u8 *)); one.z = (u16 *)malloc(sizeof(u16));
        if (!one.r || !one.z) { free(rec); free(one.r); free(one.z); return -1; }
        one.r[0] = rec; one.z[0] = esz;   /* owned by `one` from here */
        int ok = node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &one);
        rows_free(&one);
        if (!ok) return -1;
        t->root = lpg + 1;
        t->count = 1;
        return 1;
    }

    u8 *rec = (u8 *)malloc(esz);
    if (!rec) return -1;
    memcpy(rec, erec, esz);
    u32 sib = 0;
    u8 sep[ST_MAX_KEY];
    u16 seplen = 0;
    int dup = 0;
    int r = ins_rec(t, t->root - 1, rec, esz, &sib, sep, &seplen, &dup);
    free(rec);                            /* the tree copies; caller owns */
    if (r < 0) return -1;
    if (dup) return 0;
    if (r == 0) { t->count++; return 1; }

    /* root split: new branch root over (old root @ firstkey, sib @ sep) */
    u8 fk[ST_MAX_KEY];
    u16 fkl = node_first_key(t->gs, t->root - 1, &t->cd, fk);
    u32 newroot = 0;
    u8 *rpg = seg_txn_alloc(t->gs, SEG_KIND_TREE_BRANCH, &newroot);
    if (!rpg || !fkl) return -1;
    u8 b0[8 + 2 + ST_MAX_KEY], b1[8 + 2 + ST_MAX_KEY];
    u16 z0 = branch_enc((u64)t->root, &t->cd, fk, fkl, b0);
    u16 z1 = branch_enc((u64)sib, &t->cd, sep, seplen, b1);
    rows_t two;
    memset(&two, 0, sizeof two);
    two.n = two.cap = 2;
    two.r = (u8 **)malloc(sizeof(u8 *) * 2); two.z = (u16 *)malloc(sizeof(u16) * 2);
    if (!two.r || !two.z) { free(two.r); free(two.z); return -1; }
    two.r[0] = (u8 *)malloc(z0); two.z[0] = z0; memcpy(two.r[0], b0, z0);
    two.r[1] = (u8 *)malloc(z1); two.z[1] = z1; memcpy(two.r[1], b1, z1);
    int ok = two.r[0] && two.r[1] && node_write(t->gs, newroot, SEG_KIND_TREE_BRANCH, &two);
    rows_free(&two);
    if (!ok) return -1;
    t->root = newroot + 1;
    t->count++;
    return 1;
}

/* ---- delete ---- */

/* 0 = absent; 1 = changed, self-contained; 2 = changed, underfull (parent may
 * merge); 3 = emptied AND FREED (parent must drop the entry); -1 = error. */
static int del_rec(seg_tree_t *t, u32 lpg, const u8 *k, u16 kl) {
    int kind = node_kind(t->gs, lpg);
    rows_t rows;
    if ((kind != SEG_KIND_TREE_LEAF && kind != SEG_KIND_TREE_BRANCH) ||
        !rows_read(t->gs, lpg, &rows)) {
        rows_free(&rows);
        return -1;
    }

    if (kind == SEG_KIND_TREE_LEAF) {
        int hit = 0;
        u32 pos = rows_lower(&t->cd, &rows, 0, k, kl, &hit);
        if (!hit) { rows_free(&rows); return 0; }
        free(rows.r[pos]);
        for (u32 i = pos; i + 1 < rows.n; i++) { rows.r[i] = rows.r[i + 1]; rows.z[i] = rows.z[i + 1]; }
        rows.n--;
        if (rows.n == 0) {
            seg_txn_free(t->gs, lpg);
            rows_free(&rows);
            return 3;
        }
        int ok = node_write(t->gs, lpg, SEG_KIND_TREE_LEAF, &rows);
        int underfull = rows_bytes(&rows) < SEG_PAGE_SIZE / 4;
        rows_free(&rows);
        return ok ? (underfull ? 2 : 1) : -1;
    }

    /* branch */
    u32 i = branch_child(&t->cd, &rows, k, kl);
    u64 child = ld64le(rows.r[i]) - 1;
    int r = del_rec(t, (u32)child, k, kl);
    if (r <= 0) { rows_free(&rows); return r; }

    int changed = 0;
    if (r == 3) {                          /* child emptied + freed: drop entry */
        free(rows.r[i]);
        for (u32 x = i; x + 1 < rows.n; x++) { rows.r[x] = rows.r[x + 1]; rows.z[x] = rows.z[x + 1]; }
        rows.n--;
        changed = 1;
        if (rows.n == 0) {
            seg_txn_free(t->gs, lpg);
            rows_free(&rows);
            return 3;
        }
    } else if (r == 2) {                   /* underfull: single-pass sibling merge */
        int ckind = node_kind(t->gs, (u32)child);
        rows_t cr;
        if (ckind < 0 || !rows_read(t->gs, (u32)child, &cr)) { rows_free(&rows); return -1; }
        u32 j = (i + 1 < rows.n) ? i + 1 : (i > 0 ? i - 1 : 0xFFFFFFFFu);
        if (j != 0xFFFFFFFFu) {
            u32 slpg = (u32)(ld64le(rows.r[j]) - 1);
            rows_t sr;
            if (node_kind(t->gs, slpg) == ckind && rows_read(t->gs, slpg, &sr)) {
                rows_t comb;
                memset(&comb, 0, sizeof comb);
                comb.n = cr.n + sr.n;
                comb.cap = comb.n;
                comb.r = (u8 **)malloc(sizeof(u8 *) * comb.cap);
                comb.z = (u16 *)malloc(sizeof(u16) * comb.cap);
                if (!comb.r || !comb.z) { rows_free(&cr); rows_free(&sr); rows_free(&comb); rows_free(&rows); return -1; }
                const rows_t *first = (j == i + 1) ? &cr : &sr;
                const rows_t *second = (j == i + 1) ? &sr : &cr;
                u32 x = 0;
                for (u32 a = 0; a < first->n; a++, x++) { comb.r[x] = first->r[a]; comb.z[x] = first->z[a]; }
                for (u32 a = 0; a < second->n; a++, x++) { comb.r[x] = second->r[a]; comb.z[x] = second->z[a]; }
                int ok = rows_fit(&comb) && node_write(t->gs, (u32)child, (u16)ckind, &comb);
                if (ok) {
                    seg_txn_free(t->gs, slpg);
                    free(rows.r[j]);
                    for (u32 x2 = j; x2 + 1 < rows.n; x2++) { rows.r[x2] = rows.r[x2 + 1]; rows.z[x2] = rows.z[x2 + 1]; }
                    rows.n--;
                    if (j == i - 1) {
                        /* child's first key changed (merged with its LEFT sibling);
                         * after the shift the child's entry sits at i-1. */
                        const u8 *fk = NULL;
                        u16 fkl = (ckind == SEG_KIND_TREE_BRANCH)
                                ? branch_key(&t->cd, comb.r[0], comb.z[0], &fk)
                                : key_from_rec(&t->cd, comb.r[0], comb.z[0], &fk);
                        u8 nb[8 + 2 + ST_MAX_KEY];
                        u16 nbz = branch_enc(child + 1, &t->cd, fk, fkl, nb);
                        u32 ci = i - 1;
                        free(rows.r[ci]);
                        rows.r[ci] = (u8 *)malloc(nbz);
                        rows.z[ci] = nbz;
                        if (rows.r[ci]) memcpy(rows.r[ci], nb, nbz);
                    }
                    changed = 1;
                }
                rows_detach(&comb);       /* entries owned by cr/sr */
            }
            rows_free(&sr);
        }
        rows_free(&cr);
    } else {
        rows_free(&rows);
        return 1;
    }

    int ok = node_write(t->gs, lpg, SEG_KIND_TREE_BRANCH, &rows);
    int underfull = rows_bytes(&rows) < SEG_PAGE_SIZE / 4;
    int nempty = (rows.n == 0);
    rows_free(&rows);
    if (!ok) return -1;
    if (nempty) { seg_txn_free(t->gs, lpg); return 3; }
    (void)changed;
    return underfull ? 2 : 1;
}

int seg_tree_delete(seg_tree_t *t, const u8 *k, u16 klen) {
    if (t->root == 0) return 0;
    int r = del_rec(t, t->root - 1, k, klen);
    if (r < 0) return -1;
    if (r == 0) return 0;
    if (r == 3) {                          /* the root page itself emptied and
                                            * was freed inside del_rec */
        t->root = 0;
        t->count = t->count ? t->count - 1 : 0;
        return 1;
    }

    /* normalize the root (r == 1 | 2): collapse single-child branches down. */
    for (;;) {
        if (t->root == 0) break;
        int kind = node_kind(t->gs, t->root - 1);
        if (kind < 0) return -1;
        rows_t rr;
        if (!rows_read(t->gs, t->root - 1, &rr)) { rows_free(&rr); return -1; }
        if (rr.n == 0) { seg_txn_free(t->gs, t->root - 1); t->root = 0; rows_free(&rr); break; }
        if (kind == SEG_KIND_TREE_LEAF) { rows_free(&rr); break; }
        if (rr.n == 1) {
            u32 c = (u32)(ld64le(rr.r[0]) - 1);
            seg_txn_free(t->gs, t->root - 1);
            t->root = c + 1;
            rows_free(&rr);
            continue;                       /* the child may itself be a 1-child branch */
        }
        rows_free(&rr);
        break;
    }
    t->count = t->count ? t->count - 1 : 0;
    return 1;
}

/* ---- lookup ---- */

int seg_tree_lookup(seg_tree_t *t, const u8 *k, u16 klen, u8 *v, u16 *vlen) {
    u32 lpg = t->root ? t->root - 1 : 0;
    if (!t->root) return 0;
    for (;;) {
        int kind = node_kind(t->gs, lpg);
        if (kind < 0) return -1;
        rows_t rows;
        if (!rows_read(t->gs, lpg, &rows)) { rows_free(&rows); return -1; }
        if (kind == SEG_KIND_TREE_LEAF) {
            int hit = 0;
            u32 pos = rows_lower(&t->cd, &rows, 0, k, klen, &hit);
            int out = 0;
            if (hit) {
                const u8 *vp = NULL; u16 vl = 0;
                int has = val_from_rec(&t->cd, rows.r[pos], rows.z[pos], &vp, &vl);
                if (has) {
                    if (v && vlen) {
                        u16 n = vl < *vlen ? vl : *vlen;
                        memcpy(v, vp, n);
                        *vlen = n;
                    } else if (vlen) *vlen = vl;
                } else if (vlen) *vlen = 0;
                out = 1;
            }
            rows_free(&rows);
            return out;
        }
        u32 i = branch_child(&t->cd, &rows, k, klen);
        u64 child = ld64le(rows.r[i]) - 1;
        rows_free(&rows);
        lpg = (u32)child;
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
    int kind = node_kind(sc->t->gs, lpg);
    if (kind < 0) return -1;
    rows_t rows;
    if (!rows_read(sc->t->gs, lpg, &rows)) { rows_free(&rows); return -1; }
    if (kind == SEG_KIND_TREE_LEAF) {
        int hit = 0;
        u32 pos = sc->lo ? rows_lower(&sc->t->cd, &rows, 0, sc->lo, sc->lol, &hit) : 0;
        (void)hit;
        for (u32 x = pos; x < rows.n; x++) {
            const u8 *kp = NULL;
            u16 kl = key_from_rec(&sc->t->cd, rows.r[x], rows.z[x], &kp);
            if (sc->hi) {
                int c = key_cmp(kp, kl, sc->hi, sc->hil);
                if (c > 0 || (c == 0 && !sc->hi_incl)) break;
            }
            const u8 *vp = NULL; u16 vl = 0;
            (void)val_from_rec(&sc->t->cd, rows.r[x], rows.z[x], &vp, &vl);
            if (sc->cb(sc->ctx, kp, kl, vp, vl) == 0) { sc->stop = 1; break; }
        }
        rows_free(&rows);
        return 0;
    }
    for (u32 i = 0; i < rows.n && !sc->stop; i++) {
        const u8 *kp = NULL;
        u16 kl = branch_key(&sc->t->cd, rows.r[i], rows.z[i], &kp);
        if (sc->hi && i > 0) {
            int c = key_cmp(kp, kl, sc->hi, sc->hil);
            if (c > 0 || (c == 0 && !sc->hi_incl)) break;
        }
        u64 child = ld64le(rows.r[i]) - 1;
        int r = scan_node(sc, (u32)child);
        if (r < 0) { rows_free(&rows); return -1; }
    }
    rows_free(&rows);
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
    t->count = 0;                          /* caller restores from catalog */
}

u32 seg_tree_root(const seg_tree_t *t) { return t->root; }
u64 seg_tree_count(const seg_tree_t *t) { return t->count; }
