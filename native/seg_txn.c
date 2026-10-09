/*
 * seg_txn.c — COW transaction layer (Decision_LogicalPgnoShadowTable).
 *
 * On-disk fixed-format pages owned by this layer (all addressed PHYSICALLY):
 *
 *   ptable page:  1024 x u32 physical entries (SEG_PT_NONE = unmapped)
 *   ptable root:  [u32 ntpages][u32 tpage_phys x ntpages]
 *   freelist pg:  [u32 next_phys (0=end)][u32 nwords][u32 words x nwords]
 *     word stream: nfree, free[], ngroups, { txid_lo, txid_hi, n, pgnos[] }*
 *
 * Commit writes: dirty data pages + dirty ptable pages + new root + new
 * freelist chain, all to NEVER-LIVE physical pages (freelist pop or
 * watermark growth) — the COW discipline is enforced right here, which
 * discharges segfile_commit's documented caller obligation.
 *
 * Freelist chain pages are allocated from WATERMARK ONLY: their content
 * depends on the final free[] state, so they must not themselves consume
 * from it (fixpoint avoided by construction).
 */
#include "segstore.h"

#include <stdlib.h>
#include <string.h>

#define PT_ENTRIES   (SEG_PAGE_SIZE / 4u)              /* 1024 */
#define ROOT_MAX     (SEG_PAGE_SIZE / 4u - 1u)         /* 1023 tpages */
#define FL_WORDS     ((SEG_PAGE_SIZE - 8u) / 4u)       /* 1022 payload words */
#define MAX_LOGICAL  ((u64)ROOT_MAX * PT_ENTRIES)

typedef struct { u64 txid; u32 *pg; u32 n; } pend_t;
typedef struct { u32 lpg; u8 *buf; } dirty_t;

struct segstore;
static dirty_t *find_dirty(struct segstore *st, u32 lpg);

struct segstore {
    segfile_t *sf;
    u32 *ptable; u64 logical_pages, lp_cap;
    u32 *freev; u32 nfree, freecap;
    pend_t *pend; u32 npend, pendcap;
    segpin_t *pins;
    /* open txn */
    int txn_open;
    dirty_t *dirty; u32 ndirty, dirtycap;
    u32 *dhash; u32 dhashcap;         /* open-addressed lpg -> dirty idx + 1 (0 = empty) */
    u32 *freed; u32 nfreed, freedcap;
    u32 *fhash; u32 fhashcap;         /* open-addressed lpg + 1 set (0 = empty) */
    u64 txn_logical_pages;
    u32 txn_nameindex_root, txn_indirect_root;
};

/* ---------- little-endian u32 in fixed-format pages ---------- */
static u32  ld32(const u8 *p) { return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24); }
static void st32(u8 *p, u32 v) { p[0]=(u8)v; p[1]=(u8)(v>>8); p[2]=(u8)(v>>16); p[3]=(u8)(v>>24); }

/* ---------- dynamic array push helpers ---------- */
static int push_u32(u32 **a, u32 *n, u32 *cap, u32 v) {
    if (*n == *cap) {
        u32 nc = *cap ? *cap * 2 : 64;
        u32 *na = (u32 *)realloc(*a, (size_t)nc * 4);
        if (!na) return 0;
        *a = na; *cap = nc;
    }
    (*a)[(*n)++] = v;
    return 1;
}

/* ---------- txn lookup indices (open addressing, linear probing) ----------
 * find_dirty and the freed-membership scans were LINEAR; with the real-KB
 * import (hundreds of thousands of dirty pages in one txn) every view/touch/
 * free became O(ndirty|nfreed) => O(n^2) per import. The dirty array keeps
 * its append + swap-remove shape (commit output unchanged); these indices
 * only accelerate membership. Load factor stays <= 1/2, so a probe always
 * terminates and backward-shift delete needs no tombstones. */

static u32 hash32(u32 v) {
    v ^= v >> 16; v *= 0x7feb352du; v ^= v >> 15; v *= 0x846ca68bu; v ^= v >> 16;
    return v;
}

static int dhash_rehash(segstore_t *st, u32 ncap) {
    u32 *nh = (u32 *)calloc(ncap, 4);
    if (!nh) return 0;
    u32 mask = ncap - 1;
    for (u32 i = 0; i < st->ndirty; i++) {
        u32 j = hash32(st->dirty[i].lpg) & mask;
        while (nh[j]) j = (j + 1) & mask;
        nh[j] = i + 1;
    }
    free(st->dhash);
    st->dhash = nh; st->dhashcap = ncap;
    return 1;
}

/* ensure room for one more dirty entry */
static int dhash_reserve(segstore_t *st) {
    if (st->dhashcap >= 2 * (st->ndirty + 1)) return 1;
    u32 nc = st->dhashcap ? st->dhashcap : 16;
    while (nc < 2 * (st->ndirty + 1)) nc *= 2;
    return dhash_rehash(st, nc);
}

static void dhash_put(segstore_t *st, u32 lpg, u32 idx) {   /* pre-reserved */
    u32 mask = st->dhashcap - 1;
    u32 j = hash32(lpg) & mask;
    while (st->dhash[j]) j = (j + 1) & mask;
    st->dhash[j] = idx + 1;
}

static int dhash_slot(segstore_t *st, u32 lpg, u32 *slot_out) {
    if (!st->dhashcap) return 0;
    u32 mask = st->dhashcap - 1;
    u32 j = hash32(lpg) & mask;
    while (st->dhash[j]) {
        u32 idx = st->dhash[j] - 1;
        if (idx < st->ndirty && st->dirty[idx].lpg == lpg) { *slot_out = j; return 1; }
        j = (j + 1) & mask;
    }
    return 0;
}

/* backward-shift delete (the cluster stays probe-tight) */
static void dhash_del_slot(segstore_t *st, u32 j) {
    u32 mask = st->dhashcap - 1;
    st->dhash[j] = 0;
    for (u32 x = (j + 1) & mask; st->dhash[x]; x = (x + 1) & mask) {
        u32 idx = st->dhash[x] - 1;
        u32 h = hash32(st->dirty[idx].lpg) & mask;
        if (((x - h) & mask) >= ((x - j) & mask)) {
            st->dhash[j] = st->dhash[x];
            st->dhash[x] = 0;
            j = x;
        }
    }
}

static int fhash_rehash(segstore_t *st, u32 ncap) {
    u32 *nh = (u32 *)calloc(ncap, 4);
    if (!nh) return 0;
    u32 mask = ncap - 1;
    for (u32 i = 0; i < st->nfreed; i++) {
        u32 j = hash32(st->freed[i] + 1) & mask;
        while (nh[j]) j = (j + 1) & mask;
        nh[j] = st->freed[i] + 1;
    }
    free(st->fhash);
    st->fhash = nh; st->fhashcap = ncap;
    return 1;
}

static int freed_contains(const segstore_t *st, u32 lpg) {
    if (!st->fhashcap) return 0;
    u32 mask = st->fhashcap - 1;
    u32 j = hash32(lpg + 1) & mask;
    while (st->fhash[j]) {
        if (st->fhash[j] == lpg + 1) return 1;
        j = (j + 1) & mask;
    }
    return 0;
}

static int freed_push(segstore_t *st, u32 lpg) {            /* append-only */
    if (st->fhashcap < 2 * (st->nfreed + 1)) {
        u32 nc = st->fhashcap ? st->fhashcap : 16;
        while (nc < 2 * (st->nfreed + 1)) nc *= 2;
        if (!fhash_rehash(st, nc)) return 0;
    }
    if (!push_u32(&st->freed, &st->nfreed, &st->freedcap, lpg)) return 0;
    u32 mask = st->fhashcap - 1;
    u32 j = hash32(lpg + 1) & mask;
    while (st->fhash[j]) j = (j + 1) & mask;
    st->fhash[j] = lpg + 1;
    return 1;
}

/* end-of-txn scratch reset (begin / abort / commit) */
static void txn_clear_scratch(segstore_t *st) {
    st->ndirty = 0;
    st->nfreed = 0;
    if (st->dhash) memset(st->dhash, 0, (size_t)st->dhashcap * 4);
    if (st->fhash) memset(st->fhash, 0, (size_t)st->fhashcap * 4);
}

/* ---------- allocator state ---------- */

static u64 min_pin_txid(const segstore_t *st) {
    u64 m = (u64)-1;
    for (const segpin_t *p = st->pins; p; p = p->next)
        if (p->txid < m) m = p->txid;
    return m;
}

/* move every pending group whose retire-txid clears the pin gate to free[] */
static int promote_pending(segstore_t *st) {
    u64 gate = min_pin_txid(st);
    u32 w = 0;
    for (u32 i = 0; i < st->npend; i++) {
        pend_t *g = &st->pend[i];
        if (g->txid <= gate) {          /* no pin sees a state that needs them */
            for (u32 j = 0; j < g->n; j++)
                if (!push_u32(&st->freev, &st->nfree, &st->freecap, g->pg[j]))
                    return 0;
            free(g->pg);
        } else st->pend[w++] = *g;
    }
    st->npend = w;
    return 1;
}

/* pop a reusable physical page, else grow the watermark */
static u32 phys_alloc(segstore_t *st, u64 *watermark) {
    if (st->nfree > 0) return st->freev[--st->nfree];
    return (u32)((*watermark)++);
}

/* ================= open / create ================= */

static segstore_t *st_new(segfile_t *sf) {
    segstore_t *st = (segstore_t *)calloc(1, sizeof *st);
    if (!st) return NULL;
    st->sf = sf;
    return st;
}

static int pt_reserve(segstore_t *st, u64 lp) {
    if (lp <= st->lp_cap) return 1;
    u64 nc = st->lp_cap ? st->lp_cap : PT_ENTRIES;
    while (nc < lp) nc *= 2;
    u32 *np = (u32 *)realloc(st->ptable, (size_t)nc * 4);
    if (!np) return 0;
    for (u64 i = st->lp_cap; i < nc; i++) np[i] = SEG_PT_NONE;
    st->ptable = np; st->lp_cap = nc;
    return 1;
}

segstore_t *segstore_create(seg_io_t *io, u16 seg_id, u16 extent_pages_log2) {
    segfile_t *sf = segfile_create(io, seg_id, extent_pages_log2);
    if (!sf) return NULL;
    return st_new(sf);          /* roots 0 = empty table / empty freelist */
}

static int load_ptable(segstore_t *st) {
    const seg_meta_t *m = &st->sf->meta;
    st->logical_pages = m->logical_pages;
    if (!pt_reserve(st, st->logical_pages ? st->logical_pages : 1)) return 0;
    if (m->ptable_root_pgno == 0) return st->logical_pages == 0;
    const u8 *root = segfile_page(st->sf, m->ptable_root_pgno);
    if (!root) return 0;
    u32 ntp = ld32(root);
    if (ntp > ROOT_MAX) return 0;
    if ((u64)ntp * PT_ENTRIES < st->logical_pages) return 0;
    for (u32 t = 0; t < ntp; t++) {
        u32 tphys = ld32(root + 4 + 4u * t);
        const u8 *tp = segfile_page(st->sf, tphys);
        if (!tp) return 0;
        for (u32 e = 0; e < PT_ENTRIES; e++) {
            u64 lpg = (u64)t * PT_ENTRIES + e;
            if (lpg >= st->logical_pages) break;
            st->ptable[lpg] = ld32(tp + 4u * e);
        }
    }
    return 1;
}

static int load_freelist(segstore_t *st) {
    u32 phys = st->sf->meta.freelist_root_pgno;
    /* gather the word stream across the chain */
    u32 *words = NULL, nwords = 0, wcap = 0;
    while (phys != 0) {
        const u8 *fp = segfile_page(st->sf, phys);
        if (!fp) { free(words); return 0; }
        u32 next = ld32(fp), nw = ld32(fp + 4);
        if (nw > FL_WORDS) { free(words); return 0; }
        for (u32 i = 0; i < nw; i++)
            if (!push_u32(&words, &nwords, &wcap, ld32(fp + 8 + 4u * i)))
                { free(words); return 0; }
        phys = next;
    }
    u32 i = 0;
    #define TAKE(v) do { if (i >= nwords) goto bad; (v) = words[i++]; } while (0)
    u32 nf; if (nwords == 0) return 1;          /* empty freelist */
    TAKE(nf);
    for (u32 k = 0; k < nf; k++) { u32 v; TAKE(v);
        if (!push_u32(&st->freev, &st->nfree, &st->freecap, v)) goto bad; }
    u32 ng; TAKE(ng);
    for (u32 g = 0; g < ng; g++) {
        u32 lo, hi, n; TAKE(lo); TAKE(hi); TAKE(n);
        pend_t pd; pd.txid = ((u64)hi << 32) | lo; pd.n = n;
        pd.pg = (u32 *)malloc((size_t)n * 4);
        if (!pd.pg) goto bad;
        for (u32 k = 0; k < n; k++) { u32 v; TAKE(v); pd.pg[k] = v; }
        if (st->npend == st->pendcap) {
            u32 nc = st->pendcap ? st->pendcap * 2 : 16;
            pend_t *np = (pend_t *)realloc(st->pend, (size_t)nc * sizeof *np);
            if (!np) { free(pd.pg); goto bad; }
            st->pend = np; st->pendcap = nc;
        }
        st->pend[st->npend++] = pd;
    }
    #undef TAKE
    free(words);
    return 1;
bad:
    free(words);
    return 0;
}

static segstore_t *open_from(segfile_t *sf) {
    if (!sf) return NULL;
    segstore_t *st = st_new(sf);
    if (!st) { segfile_close(sf); return NULL; }
    if (!load_ptable(st) || !load_freelist(st)) { segstore_close(st); return NULL; }
    /* restart clears pins: everything pending is promotable */
    if (!promote_pending(st)) { segstore_close(st); return NULL; }
    return st;
}

/* NOTE: open consumes io on failure too (segfile_open* does not close io
 * when it merely fails to select a meta, so do it here — uniform ownership) */
segstore_t *segstore_open(seg_io_t *io) {
    segfile_t *sf = segfile_open(io);
    if (!sf) { io->close(io); return NULL; }
    return open_from(sf);
}

segstore_t *segstore_open_at(seg_io_t *io, u64 txid) {
    segfile_t *sf = segfile_open_at(io, txid);
    if (!sf) { io->close(io); return NULL; }
    return open_from(sf);
}

void segstore_close(segstore_t *st) {
    if (!st) return;
    seg_txn_abort(st);
    for (u32 i = 0; i < st->npend; i++) free(st->pend[i].pg);
    free(st->pend); free(st->freev); free(st->ptable);
    free(st->dirty); free(st->freed);
    free(st->dhash); free(st->fhash);
    for (segpin_t *p = st->pins; p; ) {
        segpin_t *nx = p->next; free(p->ptable); free(p); p = nx;
    }
    segfile_close(st->sf);
    free(st);
}

u64 segstore_txid(const segstore_t *st)          { return st->sf->meta.txid; }
u32 segstore_nameindex_root(const segstore_t *st) {
    return st->txn_open ? st->txn_nameindex_root : st->sf->meta.nameindex_root_pgno;
}
u64 segstore_logical_pages(const segstore_t *st) { return st->logical_pages; }

const u8 *seg_txn_view(segstore_t *st, u32 lpg) {
    if (st->txn_open) {
        if (freed_contains(st, lpg)) return NULL;
        dirty_t *d = find_dirty(st, lpg);
        if (d) return d->buf;
        if (lpg >= st->txn_logical_pages) return NULL;
    }
    return segstore_read(st, lpg);
}

const u8 *segstore_read(segstore_t *st, u32 lpg) {
    if (lpg >= st->logical_pages) return NULL;
    u32 phys = st->ptable[lpg];
    if (phys == SEG_PT_NONE) return NULL;
    return segfile_page(st->sf, phys);
}

/* ================= pins ================= */

segpin_t *seg_pin(segstore_t *st) {
    segpin_t *p = (segpin_t *)calloc(1, sizeof *p);
    if (!p) return NULL;
    p->txid = st->sf->meta.txid;
    p->logical_pages = st->logical_pages;
    if (st->logical_pages) {
        p->ptable = (u32 *)malloc((size_t)st->logical_pages * 4);
        if (!p->ptable) { free(p); return NULL; }
        memcpy(p->ptable, st->ptable, (size_t)st->logical_pages * 4);
    }
    p->next = st->pins; st->pins = p;
    return p;
}

void seg_unpin(segstore_t *st, segpin_t *pin) {
    for (segpin_t **pp = &st->pins; *pp; pp = &(*pp)->next)
        if (*pp == pin) { *pp = pin->next; break; }
    free(pin->ptable); free(pin);
}

const u8 *seg_pin_read(segstore_t *st, const segpin_t *pin, u32 lpg) {
    if (lpg >= pin->logical_pages) return NULL;
    u32 phys = pin->ptable[lpg];
    if (phys == SEG_PT_NONE) return NULL;
    return segfile_page(st->sf, phys);  /* phys < old watermark <= current */
}

/* ================= txn ================= */

int seg_txn_begin(segstore_t *st) {
    if (st->txn_open) return 0;
    st->txn_open = 1;
    txn_clear_scratch(st);
    st->txn_logical_pages = st->logical_pages;
    st->txn_nameindex_root = st->sf->meta.nameindex_root_pgno;
    st->txn_indirect_root  = st->sf->meta.indirect_root_pgno;
    return 1;
}

static dirty_t *find_dirty(segstore_t *st, u32 lpg) {
    u32 j;
    if (!dhash_slot(st, lpg, &j)) return NULL;
    return &st->dirty[st->dhash[j] - 1];
}

static u8 *add_dirty(segstore_t *st, u32 lpg) {
    u8 *buf = (u8 *)malloc(SEG_PAGE_SIZE);
    if (!buf) return NULL;
    if (st->ndirty == st->dirtycap) {
        u32 nc = st->dirtycap ? st->dirtycap * 2 : 32;
        dirty_t *nd = (dirty_t *)realloc(st->dirty, (size_t)nc * sizeof *nd);
        if (!nd) { free(buf); return NULL; }
        st->dirty = nd; st->dirtycap = nc;
    }
    if (!dhash_reserve(st)) { free(buf); return NULL; }
    dhash_put(st, lpg, st->ndirty);
    st->dirty[st->ndirty].lpg = lpg;
    st->dirty[st->ndirty].buf = buf;
    st->ndirty++;
    return buf;
}

u8 *seg_txn_touch(segstore_t *st, u32 lpg) {
    if (!st->txn_open || lpg >= st->txn_logical_pages) return NULL;
    if (freed_contains(st, lpg)) return NULL;         /* freed this txn */
    dirty_t *d = find_dirty(st, lpg);
    if (d) return d->buf;
    const u8 *cur = segstore_read(st, lpg);
    if (!cur) return NULL;                            /* unmapped */
    u8 *buf = add_dirty(st, lpg);
    if (!buf) return NULL;
    if (!seg_page_compact(buf, cur)) {                /* COW + compact */
        st->ndirty--; free(buf); return NULL;         /* corrupt source */
    }
    return buf;
}

u8 *seg_txn_alloc(segstore_t *st, u16 kind_hint, u32 *lpg_out) {
    if (!st->txn_open || st->txn_logical_pages >= MAX_LOGICAL) return NULL;
    u32 lpg = (u32)st->txn_logical_pages;
    u8 *buf = add_dirty(st, lpg);
    if (!buf) return NULL;
    st->txn_logical_pages++;
    memset(buf, 0, SEG_PAGE_SIZE);
    seg_page_init(buf, kind_hint);
    if (lpg_out) *lpg_out = lpg;
    return buf;
}

int seg_txn_free(segstore_t *st, u32 lpg) {
    if (!st->txn_open || lpg >= st->txn_logical_pages) return 0;
    if (freed_contains(st, lpg)) return 0;            /* double free */
    u32 slot;
    if (dhash_slot(st, lpg, &slot)) {                 /* alloc'd/touched this txn */
        u32 pos = st->dhash[slot] - 1;
        free(st->dirty[pos].buf);
        dhash_del_slot(st, slot);
        u32 last = st->ndirty - 1;
        if (pos != last) {                            /* swap-remove; keep the
                                                       * moved entry's index */
            st->dirty[pos] = st->dirty[last];
            u32 mslot;
            if (dhash_slot(st, st->dirty[pos].lpg, &mslot))
                st->dhash[mslot] = pos + 1;
        }
        st->ndirty = last;
        if (lpg >= st->logical_pages)                 /* never committed: no retire */
            return freed_push(st, lpg);
    } else if (st->ptable[lpg] == SEG_PT_NONE) return 0;   /* already unmapped */
    return freed_push(st, lpg);
}

void seg_txn_set_roots(segstore_t *st, u32 nameindex_root, u32 indirect_root) {
    st->txn_nameindex_root = nameindex_root;
    st->txn_indirect_root  = indirect_root;
}

void seg_txn_abort(segstore_t *st) {
    if (!st->txn_open) return;
    for (u32 i = 0; i < st->ndirty; i++) free(st->dirty[i].buf);
    txn_clear_scratch(st);
    st->txn_open = 0;
}

/* ---------- commit ---------- */

typedef struct { u32 *pgnos; const u8 **bufs; u8 **owned; u32 n, cap; } wset_t;

static int wset_add(wset_t *w, u32 phys, const u8 *buf, u8 *owned) {
    if (w->n == w->cap) {
        u32 nc = w->cap ? w->cap * 2 : 64;
        u32 *np = (u32 *)realloc(w->pgnos, (size_t)nc * 4);
        const u8 **nb = (const u8 **)realloc((void *)w->bufs, (size_t)nc * sizeof *nb);
        u8 **no = (u8 **)realloc(w->owned, (size_t)nc * sizeof *no);
        if (!np || !nb || !no) { free(np); return 0; }
        w->pgnos = np; w->bufs = nb; w->owned = no; w->cap = nc;
    }
    w->pgnos[w->n] = phys; w->bufs[w->n] = buf; w->owned[w->n] = owned;
    w->n++;
    return 1;
}

int seg_txn_commit(segstore_t *st) {
    return seg_txn_commit_as(st, st->sf->meta.txid + 1);
}

int seg_txn_commit_as(segstore_t *st, u64 commit_txid) {
    if (commit_txid <= st->sf->meta.txid) return 0;
    if (!st->txn_open) return 0;
    if (st->ndirty == 0 && st->nfreed == 0 &&
        st->txn_logical_pages == st->logical_pages &&
        st->txn_nameindex_root == st->sf->meta.nameindex_root_pgno &&
        st->txn_indirect_root  == st->sf->meta.indirect_root_pgno) {
        st->txn_open = 0;                     /* true no-op */
        return 1;
    }
    if (!promote_pending(st)) return 0;

    int ok = 0;
    u64 wm = st->sf->meta.watermark;
    u64 next_txid = commit_txid;
    u32 *retire = NULL; u32 nretire = 0, retirecap = 0;
    u32 *newpt = NULL;
    wset_t w; memset(&w, 0, sizeof w);
    u32 old_ntp = st->sf->meta.ptable_root_pgno
        ? (u32)((st->logical_pages + PT_ENTRIES - 1) / PT_ENTRIES) : 0;

    /* working copy of the page table at new size */
    u64 new_lp = st->txn_logical_pages;
    u32 new_ntp = (u32)((new_lp + PT_ENTRIES - 1) / PT_ENTRIES);
    newpt = (u32 *)malloc((size_t)(new_ntp ? new_ntp : 1) * PT_ENTRIES * 4);
    if (!newpt) goto out;
    for (u64 i = 0; i < (u64)new_ntp * PT_ENTRIES; i++)
        newpt[i] = (i < st->logical_pages) ? st->ptable[i] : SEG_PT_NONE;

    /* freed pages: retire old phys, unmap */
    for (u32 i = 0; i < st->nfreed; i++) {
        u32 lpg = st->freed[i];
        if (lpg < st->logical_pages && st->ptable[lpg] != SEG_PT_NONE)
            if (!push_u32(&retire, &nretire, &retirecap, st->ptable[lpg])) goto out;
        newpt[lpg] = SEG_PT_NONE;
    }
    /* dirty pages: retire old phys, allocate new, stage write */
    u8 *dirty_tp = (u8 *)calloc(new_ntp ? new_ntp : 1, 1);     /* bool per tpage */
    if (!dirty_tp) goto out;
    for (u32 i = 0; i < st->ndirty; i++) {
        u32 lpg = st->dirty[i].lpg;
        if (lpg < st->logical_pages && st->ptable[lpg] != SEG_PT_NONE)
            if (!push_u32(&retire, &nretire, &retirecap, st->ptable[lpg]))
                { free(dirty_tp); goto out; }
        u32 phys = phys_alloc(st, &wm);
        newpt[lpg] = phys;
        if (!wset_add(&w, phys, st->dirty[i].buf, NULL)) { free(dirty_tp); goto out; }
        dirty_tp[lpg / PT_ENTRIES] = 1;
    }
    for (u32 i = 0; i < st->nfreed; i++)
        if (st->freed[i] / PT_ENTRIES < new_ntp) dirty_tp[st->freed[i] / PT_ENTRIES] = 1;
    for (u32 t = old_ntp; t < new_ntp; t++) dirty_tp[t] = 1;   /* new table pages */

    /* old ptable pages being replaced + old root + old freelist chain retire */
    const u8 *oldroot = st->sf->meta.ptable_root_pgno
        ? segfile_page(st->sf, st->sf->meta.ptable_root_pgno) : NULL;
    for (u32 t = 0; t < old_ntp && t < new_ntp; t++)
        if (dirty_tp[t] && oldroot)
            if (!push_u32(&retire, &nretire, &retirecap, ld32(oldroot + 4 + 4u * t)))
                { free(dirty_tp); goto out; }
    if (st->sf->meta.ptable_root_pgno)
        if (!push_u32(&retire, &nretire, &retirecap, st->sf->meta.ptable_root_pgno))
            { free(dirty_tp); goto out; }
    for (u32 phys = st->sf->meta.freelist_root_pgno; phys != 0; ) {
        const u8 *fp = segfile_page(st->sf, phys);
        if (!fp) { free(dirty_tp); goto out; }
        if (!push_u32(&retire, &nretire, &retirecap, phys)) { free(dirty_tp); goto out; }
        phys = ld32(fp);
    }

    /* new ptable pages + root (alloc may consume free[]) */
    u32 *tp_phys = (u32 *)malloc((size_t)(new_ntp ? new_ntp : 1) * 4);
    if (!tp_phys) { free(dirty_tp); goto out; }
    for (u32 t = 0; t < new_ntp; t++) {
        if (dirty_tp[t]) {
            u8 *img = (u8 *)malloc(SEG_PAGE_SIZE);
            if (!img) { free(dirty_tp); free(tp_phys); goto out; }
            for (u32 e = 0; e < PT_ENTRIES; e++)
                st32(img + 4u * e, newpt[(u64)t * PT_ENTRIES + e]);
            tp_phys[t] = phys_alloc(st, &wm);
            if (!wset_add(&w, tp_phys[t], img, img)) { free(img); free(dirty_tp); free(tp_phys); goto out; }
        } else tp_phys[t] = ld32(oldroot + 4 + 4u * t);   /* unchanged: shared */
    }
    u32 root_phys = 0;
    if (new_ntp > 0) {
        u8 *rimg = (u8 *)calloc(1, SEG_PAGE_SIZE);
        if (!rimg) { free(dirty_tp); free(tp_phys); goto out; }
        st32(rimg, new_ntp);
        for (u32 t = 0; t < new_ntp; t++) st32(rimg + 4 + 4u * t, tp_phys[t]);
        root_phys = phys_alloc(st, &wm);
        if (!wset_add(&w, root_phys, rimg, rimg)) { free(rimg); free(dirty_tp); free(tp_phys); goto out; }
    }
    free(dirty_tp); free(tp_phys);

    /* freelist snapshot: free[] is now FINAL (freelist pages come from wm).
     * stream = nfree, free[], ngroups(+1 for this txn's retire set), groups */
    {
        u32 total = 1 + st->nfree + 1;
        for (u32 g = 0; g < st->npend; g++) total += 3 + st->pend[g].n;
        if (nretire) total += 3 + nretire;
        u32 *words = (u32 *)malloc((size_t)total * 4);
        if (!words) goto out;
        u32 i = 0;
        words[i++] = st->nfree;
        for (u32 k = 0; k < st->nfree; k++) words[i++] = st->freev[k];
        words[i++] = st->npend + (nretire ? 1 : 0);
        for (u32 g = 0; g < st->npend; g++) {
            words[i++] = (u32)st->pend[g].txid;
            words[i++] = (u32)(st->pend[g].txid >> 32);
            words[i++] = st->pend[g].n;
            for (u32 k = 0; k < st->pend[g].n; k++) words[i++] = st->pend[g].pg[k];
        }
        if (nretire) {
            words[i++] = (u32)next_txid; words[i++] = (u32)(next_txid >> 32);
            words[i++] = nretire;
            for (u32 k = 0; k < nretire; k++) words[i++] = retire[k];
        }
        /* chain pages: allocate FIRST (freelist-may-feed-itself, the libmdbx
         * GC discipline), then rebuild the stream from the post-pop free[].
         * npgs from the pre-pop upper bound stays sufficient: each pop only
         * shrinks the stream by one word. */
        u32 npgs = (total + FL_WORDS - 1) / FL_WORDS;
        if (npgs == 0) npgs = 1;
        u32 *chain = (u32 *)malloc((size_t)npgs * 4);
        if (!chain) { free(words); goto out; }
        for (u32 c = 0; c < npgs; c++) chain[c] = phys_alloc(st, &wm);
        /* rebuild stream: free[] may have shrunk by up to npgs pops */
        {
            u32 i2 = 0;
            words[i2++] = st->nfree;
            for (u32 k = 0; k < st->nfree; k++) words[i2++] = st->freev[k];
            words[i2++] = st->npend + (nretire ? 1 : 0);
            for (u32 g = 0; g < st->npend; g++) {
                words[i2++] = (u32)st->pend[g].txid;
                words[i2++] = (u32)(st->pend[g].txid >> 32);
                words[i2++] = st->pend[g].n;
                for (u32 k = 0; k < st->pend[g].n; k++) words[i2++] = st->pend[g].pg[k];
            }
            if (nretire) {
                words[i2++] = (u32)next_txid; words[i2++] = (u32)(next_txid >> 32);
                words[i2++] = nretire;
                for (u32 k = 0; k < nretire; k++) words[i2++] = retire[k];
            }
            total = i2;
        }
        u32 fl_root = chain[0];
        for (u32 pgi = 0; pgi < npgs; pgi++) {
            u32 start = pgi * FL_WORDS;
            u32 nw = (start >= total) ? 0
                   : (start + FL_WORDS <= total) ? FL_WORDS : total - start;
            u8 *img = (u8 *)calloc(1, SEG_PAGE_SIZE);
            if (!img) { free(chain); free(words); goto out; }
            st32(img, (pgi + 1 < npgs) ? chain[pgi + 1] : 0);
            st32(img + 4, nw);
            for (u32 k2 = 0; k2 < nw; k2++) st32(img + 8 + 4u * k2, words[start + k2]);
            if (!wset_add(&w, chain[pgi], img, img)) { free(img); free(chain); free(words); goto out; }
        }
        free(chain);
        free(words);

        /* meta + the real commit */
        seg_meta_t nm = st->sf->meta;
        nm.txid = next_txid;
        nm.watermark = wm;
        nm.logical_pages = new_lp;
        nm.ptable_root_pgno = root_phys;
        nm.freelist_root_pgno = fl_root;
        nm.nameindex_root_pgno = st->txn_nameindex_root;
        nm.indirect_root_pgno  = st->txn_indirect_root;
        if (!segfile_commit(st->sf, w.pgnos, w.bufs, w.n, &nm)) goto out;
    }

    /* install in-memory state */
    if (!pt_reserve(st, (u64)new_ntp * PT_ENTRIES)) goto out;   /* cannot fail mid-state: reserve is additive */
    memcpy(st->ptable, newpt, (size_t)new_ntp * PT_ENTRIES * 4);
    st->logical_pages = new_lp;
    if (nretire) {
        if (st->npend == st->pendcap) {
            u32 nc = st->pendcap ? st->pendcap * 2 : 16;
            pend_t *np = (pend_t *)realloc(st->pend, (size_t)nc * sizeof *np);
            if (np) { st->pend = np; st->pendcap = nc; }
        }
        if (st->npend < st->pendcap) {
            st->pend[st->npend].txid = next_txid;
            st->pend[st->npend].pg = retire;
            st->pend[st->npend].n = nretire;
            st->npend++;
            retire = NULL;                     /* ownership moved */
        }
    }
    ok = 1;

out:
    for (u32 i = 0; i < w.n; i++) free(w.owned[i]);
    free(w.pgnos); free((void *)w.bufs); free(w.owned);
    free(newpt); free(retire);
    for (u32 i = 0; i < st->ndirty; i++) free(st->dirty[i].buf);
    txn_clear_scratch(st);
    st->txn_open = 0;
    return ok;
}
