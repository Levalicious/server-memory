#include "mf_cart.h"

typedef struct __attribute__((packed)) { u64 size, left, right, parent; } mfc_node_t;

static inline mfc_node_t *N(mf_t *mf, u64 o) { return (mfc_node_t *)mf_ptr(mf, o); }

static inline u64 round32(u64 s) {
    s = (s + (MFC_QUANTUM - 1)) & ~(u64)(MFC_QUANTUM - 1);
    return s < MFC_MIN_BLOCK ? MFC_MIN_BLOCK : s;
}

/* Rotate child x above its parent p, preserving BST order. Updates parent links + root. */
static void rot(mf_t *mf, u64 x) {
    u64 p = N(mf, x)->parent;
    u64 g = N(mf, p)->parent;
    if (N(mf, p)->left == x) {            /* right rotation */
        u64 b = N(mf, x)->right;
        N(mf, x)->right = p; N(mf, p)->parent = x;
        N(mf, p)->left = b; if (b) N(mf, b)->parent = p;
    } else {                              /* left rotation */
        u64 b = N(mf, x)->left;
        N(mf, x)->left = p; N(mf, p)->parent = x;
        N(mf, p)->right = b; if (b) N(mf, b)->parent = p;
    }
    N(mf, x)->parent = g;
    if (!g) mf->hdr->free_root = x;
    else if (N(mf, g)->left == p) N(mf, g)->left = x; else N(mf, g)->right = x;
}

/* Insert a free block (addr = off, BST key) maintaining the max-heap on size. */
static void cart_insert(mf_t *mf, u64 off, u64 size) {
    N(mf, off)->size = size; N(mf, off)->left = 0; N(mf, off)->right = 0; N(mf, off)->parent = 0;
    mf->hdr->free_bytes += size; mf->hdr->free_count += 1;

    u64 root = mf->hdr->free_root;
    if (!root) { mf->hdr->free_root = off; return; }

    u64 cur = root, par = 0;
    while (cur) { par = cur; cur = (off < cur) ? N(mf, cur)->left : N(mf, cur)->right; }
    N(mf, off)->parent = par;
    if (off < par) N(mf, par)->left = off; else N(mf, par)->right = off;

    while (N(mf, off)->parent && N(mf, N(mf, off)->parent)->size < N(mf, off)->size)
        rot(mf, off);
}

/* Remove a node: rotate it down (toward the larger child) to a leaf, then detach. */
static void cart_remove(mf_t *mf, u64 off) {
    for (;;) {
        u64 l = N(mf, off)->left, r = N(mf, off)->right;
        if (!l && !r) break;
        u64 bigger = !l ? r : !r ? l : (N(mf, l)->size >= N(mf, r)->size ? l : r);
        rot(mf, bigger);
    }
    u64 p = N(mf, off)->parent;
    if (!p) mf->hdr->free_root = 0;
    else if (N(mf, p)->left == off) N(mf, p)->left = 0; else N(mf, p)->right = 0;
    mf->hdr->free_bytes -= N(mf, off)->size; mf->hdr->free_count -= 1;
}

/* Lowest-address free block with size >= n, in O(h). Uses heap: left subtree max = left->size. */
static u64 cart_fit(mf_t *mf, u64 n) {
    u64 cur = mf->hdr->free_root;
    while (cur) {
        u64 l = N(mf, cur)->left;
        if (l && N(mf, l)->size >= n) { cur = l; continue; }
        if (N(mf, cur)->size >= n) return cur;
        cur = N(mf, cur)->right;
    }
    return 0;
}

static u64 cart_succ(mf_t *mf, u64 off) {  /* smallest address > off */
    u64 cur = mf->hdr->free_root, s = 0;
    while (cur) { if (cur > off) { s = cur; cur = N(mf, cur)->left; } else cur = N(mf, cur)->right; }
    return s;
}
static u64 cart_pred(mf_t *mf, u64 off) {  /* largest address < off */
    u64 cur = mf->hdr->free_root, p = 0;
    while (cur) { if (cur < off) { p = cur; cur = N(mf, cur)->right; } else cur = N(mf, cur)->left; }
    return p;
}

u64 mfc_alloc(mf_t *mf, u64 size) {
    u64 n = round32(size);
    u64 b = cart_fit(mf, n);
    if (b) {
        u64 bsz = N(mf, b)->size;
        cart_remove(mf, b);
        u64 rem = bsz - n;                 /* 0 or >= 32 */
        if (rem >= MFC_MIN_BLOCK) cart_insert(mf, b + n, rem);
        return b;
    }
    if (mf_ensure(mf, n) < 0) return 0;
    u64 off = mf->hdr->allocated;
    mf->hdr->allocated += n;
    return off;
}

void mfc_free(mf_t *mf, u64 off, u64 size) {
    if (!off) return;
    u64 n = round32(size);

    /* merge with the immediately-following free block, if physically adjacent */
    u64 s = cart_succ(mf, off);
    if (s && off + n == s) { n += N(mf, s)->size; cart_remove(mf, s); }

    /* merge with the immediately-preceding free block, if physically adjacent */
    u64 p = cart_pred(mf, off);
    if (p && p + N(mf, p)->size == off) { u64 psz = N(mf, p)->size; cart_remove(mf, p); off = p; n += psz; }

    cart_insert(mf, off, n);
}

void mfc_coalesce(mf_t *mf) { (void)mf; }   /* continuous */

u64 mfc_freelist_len(mf_t *mf) { return mf->hdr->free_count; }
u64 mfc_free_bytes(mf_t *mf)   { return mf->hdr->free_bytes; }

/* ---- test-only invariant checker ---- */
static void v_rec(mf_t *mf, u64 node, u64 parent, u64 lo, u64 hi,
                  u64 *count, u64 *bytes, int *ok) {
    if (!node) return;
    if (N(mf, node)->parent != parent) *ok = 0;        /* parent link */
    if (node <= lo || node >= hi) *ok = 0;             /* BST order by address */
    if (parent && N(mf, parent)->size < N(mf, node)->size) *ok = 0;  /* max-heap on size */
    (*count)++; (*bytes) += N(mf, node)->size;
    v_rec(mf, N(mf, node)->left,  node, lo,  node, count, bytes, ok);
    v_rec(mf, N(mf, node)->right, node, node, hi,  count, bytes, ok);
}

int mfc_validate(mf_t *mf) {
    int ok = 1; u64 count = 0, bytes = 0;
    v_rec(mf, mf->hdr->free_root, 0, 0, ~(u64)0, &count, &bytes, &ok);
    if (count != mf->hdr->free_count) ok = 0;
    if (bytes != mf->hdr->free_bytes) ok = 0;
    return ok ? 0 : 1;
}
