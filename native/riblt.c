#include "riblt.h"

#include "blake2s.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

/* ---- hashing ---- */

uint64_t riblt_seed(const void* key, size_t key_len)
{
    uint8_t h[8];
    hg_blake2s(h, 8u, key, key_len);
    uint64_t s = 0;
    for (int i = 7; i >= 0; i--) s = (s << 8) | h[i];
    return s;
}

uint64_t riblt_hash64(uint64_t seed, const uint8_t sym[RIBLT_WIDTH])
{
    uint8_t in[8 + RIBLT_WIDTH];
    for (int i = 0; i < 8; i++) in[i] = (uint8_t)(seed >> (8 * i));
    memcpy(in + 8, sym, RIBLT_WIDTH);
    uint8_t h[8];
    hg_blake2s(h, 8u, in, sizeof in);
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | h[i];
    return v;
}

/* ---- mapping: xorshift64 stream + closed-form skip (alpha = 0.5) ---- */

static uint64_t xs_seed(uint64_t seed, const uint8_t sym[RIBLT_WIDTH])
{
    uint64_t s = riblt_hash64(seed, sym);
    return s ? s : 1u;                         /* xorshift needs a nonzero state */
}

static uint32_t next_skip(uint32_t i, uint64_t* state)
{
    uint64_t s = *state;
    s ^= s << 13; s ^= s >> 7; s ^= s << 17;
    *state = s;
    double r = (double)s * (1.0 / 18446744073709551616.0);   /* [0, 1) */
    double a = 3.0 + 2.0 * (double)i;
    double cf = sqrt((a * a - r) / (4.0 * (1.0 - r))) - 0.5 * a;
    int32_t g = (int32_t)ceil(cf);
    if (g < 1) g = 1;                          /* indices strictly advance */
    return (uint32_t)g;
}

/* ---- packed (idx << 32 | id) min-heap ---- */

static void heap_push(uint64_t* h, uint32_t* n, uint64_t e)
{
    uint32_t i = (*n)++;
    h[i] = e;
    while (i > 0) {
        uint32_t p = (i - 1u) / 2u;
        if (h[p] <= h[i]) break;
        uint64_t t = h[p]; h[p] = h[i]; h[i] = t;
        i = p;
    }
}

static uint64_t heap_pop(uint64_t* h, uint32_t* n)
{
    uint64_t top = h[0];
    h[0] = h[--(*n)];
    uint32_t i = 0;
    for (;;) {
        uint32_t l = 2u * i + 1u, r = l + 1u, m = i;
        if (l < *n && h[l] < h[m]) m = l;
        if (r < *n && h[r] < h[m]) m = r;
        if (m == i) break;
        uint64_t t = h[m]; h[m] = h[i]; h[i] = t;
        i = m;
    }
    return top;
}

#define PACK(idx, id)  (((uint64_t)(idx) << 32) | (uint64_t)(id))
#define P_IDX(e)       ((uint32_t)((e) >> 32))
#define P_ID(e)        ((uint32_t)((e) & 0xFFFFFFFFu))

/* ---- encoder ---- */

int riblt_enc_init(riblt_enc* e, const void* key, size_t key_len, const uint8_t* symbols, uint32_t n)
{
    memset(e, 0, sizeof *e);
    e->seed = riblt_seed(key, key_len);
    e->n = n;
    if (n) {
        e->src  = malloc((size_t)n * RIBLT_WIDTH);
        e->xs   = malloc((size_t)n * sizeof *e->xs);
        e->heap = malloc((size_t)n * sizeof *e->heap);
        if (!e->src || !e->xs || !e->heap) { riblt_enc_free(e); return -1; }
        memcpy(e->src, symbols, (size_t)n * RIBLT_WIDTH);
        for (uint32_t i = 0; i < n; i++) {
            e->xs[i]   = xs_seed(e->seed, e->src + (size_t)i * RIBLT_WIDTH);
            e->heap[i] = PACK(0, i);               /* every source touches cell 0 first */
        }
        e->heap_len = n;
    }
    return 0;
}

void riblt_enc_free(riblt_enc* e)
{
    free(e->src); free(e->xs); free(e->heap); free(e->coded);
    memset(e, 0, sizeof *e);
}

static int cells_reserve(riblt_cell** cells, uint32_t* cap, uint32_t need)
{
    if (need <= *cap) return 0;
    uint32_t nc = *cap ? *cap : 32u;
    while (nc < need) nc *= 2u;
    riblt_cell* p = realloc(*cells, (size_t)nc * sizeof *p);
    if (!p) return -1;
    memset(p + *cap, 0, (size_t)(nc - *cap) * sizeof *p);
    *cells = p;
    *cap = nc;
    return 0;
}

const riblt_cell* riblt_enc_cell(riblt_enc* e, uint32_t idx)
{
    if (cells_reserve(&e->coded, &e->coded_cap, idx + 1u) != 0) return NULL;
    if (e->coded_len <= idx) e->coded_len = idx + 1u;
    while (e->heap_len > 0 && P_IDX(e->heap[0]) <= idx) {
        uint64_t ent = heap_pop(e->heap, &e->heap_len);
        uint32_t cur = P_IDX(ent), sidx = P_ID(ent);
        const uint8_t* s = e->src + (size_t)sidx * RIBLT_WIDTH;
        riblt_cell* c = &e->coded[cur];
        for (uint32_t k = 0; k < RIBLT_WIDTH; k++) c->sum[k] ^= s[k];
        c->count++;
        c->checksum ^= riblt_hash64(e->seed, s);
        heap_push(e->heap, &e->heap_len, PACK(cur + next_skip(cur, &e->xs[sidx]), sidx));
    }
    return &e->coded[idx];
}

/* ---- decoder ---- */

int riblt_dec_init(riblt_dec* d, riblt_enc* local, const void* key, size_t key_len)
{
    memset(d, 0, sizeof *d);
    d->local = local;
    d->seed = riblt_seed(key, key_len);
    return 0;
}

void riblt_dec_free(riblt_dec* d)
{
    free(d->comb); free(d->fed); free(d->peeled); free(d->pq);
    free(d->local_only); free(d->remote_only);
    memset(d, 0, sizeof *d);
}

static int sym_list_push(uint8_t** list, uint32_t* n, uint32_t* cap, const uint8_t* sym)
{
    if (*n == *cap) {
        uint32_t nc = *cap ? *cap * 2u : 16u;
        uint8_t* p = realloc(*list, (size_t)nc * RIBLT_WIDTH);
        if (!p) return -1;
        *list = p;
        *cap = nc;
    }
    memcpy(*list + (size_t)(*n) * RIBLT_WIDTH, sym, RIBLT_WIDTH);
    (*n)++;
    return 0;
}

static void cell_remove(riblt_cell* c, const uint8_t* sym, int32_t sign, uint64_t seed)
{
    for (uint32_t k = 0; k < RIBLT_WIDTH; k++) c->sum[k] ^= sym[k];
    c->count -= sign;
    c->checksum ^= riblt_hash64(seed, sym);
}

static int cell_is_zero(const riblt_cell* c)
{
    if (c->count != 0 || c->checksum != 0) return 0;
    for (uint32_t k = 0; k < RIBLT_WIDTH; k++) if (c->sum[k]) return 0;
    return 1;
}

/* Remove a peeled symbol from every fed cell it maps to, then park its
 * iterator so cells fed later are handled as they arrive. */
static int record_peel(riblt_dec* d, const uint8_t* sym, int32_t sign)
{
    if (d->peeled_n == d->peeled_cap) {
        uint32_t nc = d->peeled_cap ? d->peeled_cap * 2u : 16u;
        riblt_peeled* p = realloc(d->peeled, (size_t)nc * sizeof *p);
        uint64_t* q = realloc(d->pq, (size_t)nc * sizeof *q);
        if (p) d->peeled = p;
        if (q) d->pq = q;
        if (!p || !q) return -1;
        d->peeled_cap = nc;
    }
    riblt_peeled* p = &d->peeled[d->peeled_n];
    p->xs = xs_seed(d->seed, sym);
    p->next = 0;
    p->sign = sign;
    memcpy(p->sym, sym, RIBLT_WIDTH);
    while (p->next < d->comb_len) {
        if (d->fed[p->next]) cell_remove(&d->comb[p->next], sym, sign, d->seed);
        p->next += next_skip(p->next, &p->xs);
    }
    heap_push(d->pq, &d->pq_len, PACK(p->next, d->peeled_n));
    d->peeled_n++;
    return 0;
}

static int peel(riblt_dec* d)
{
    int progress = 1;
    while (progress) {
        progress = 0;
        for (uint32_t i = 0; i < d->comb_len; i++) {
            riblt_cell* c = &d->comb[i];
            if (!d->fed[i] || (c->count != 1 && c->count != -1)) continue;
            if (riblt_hash64(d->seed, c->sum) != c->checksum) continue;
            uint8_t sym[RIBLT_WIDTH];
            memcpy(sym, c->sum, RIBLT_WIDTH);
            int32_t sign = c->count;
            int rc = (sign == 1) ? sym_list_push(&d->local_only, &d->local_only_n, &d->local_only_cap, sym)
                                 : sym_list_push(&d->remote_only, &d->remote_only_n, &d->remote_only_cap, sym);
            if (rc != 0 || record_peel(d, sym, sign) != 0) return -1;
            progress = 1;
            break;                              /* removals changed earlier cells: rescan */
        }
    }
    return 0;
}

int riblt_dec_decoded(const riblt_dec* d)
{
    if (d->comb_len == 0 || !d->fed[0]) return 0;
    for (uint32_t i = 0; i < d->comb_len; i++)
        if (d->fed[i] && !cell_is_zero(&d->comb[i])) return 0;
    return 1;
}

int riblt_dec_feed(riblt_dec* d, uint32_t idx, const riblt_cell* remote)
{
    /* Indices must not go backwards: the parked iterators of peeled symbols
     * have already passed every index below comb_len, so a cell fed there
     * later would re-expose them (Finding_RIBLT_GapFeedOscillation). The
     * wire layer reorders within a window and drops stragglers; a skipped
     * index is simply a cell that never contributes. */
    if (idx < d->comb_len) return riblt_dec_decoded(d);
    if (idx >= d->comb_cap) {
        uint32_t nc = d->comb_cap ? d->comb_cap : 32u;
        while (nc <= idx) nc *= 2u;
        riblt_cell* c = realloc(d->comb, (size_t)nc * sizeof *c);
        uint8_t* f = realloc(d->fed, nc);
        if (c) d->comb = c;
        if (f) d->fed = f;
        if (!c || !f) return -1;
        memset(d->comb + d->comb_cap, 0, (size_t)(nc - d->comb_cap) * sizeof *c);
        memset(d->fed + d->comb_cap, 0, nc - d->comb_cap);
        d->comb_cap = nc;
    }
    if (idx >= d->comb_len) d->comb_len = idx + 1u;
    const riblt_cell* local = riblt_enc_cell(d->local, idx);
    if (!local) return -1;
    riblt_cell* c = &d->comb[idx];
    for (uint32_t k = 0; k < RIBLT_WIDTH; k++) c->sum[k] = local->sum[k] ^ remote->sum[k];
    c->count = local->count - remote->count;
    c->checksum = local->checksum ^ remote->checksum;
    d->fed[idx] = 1;
    d->fed_n++;
    /* peels recorded earlier that map to this cell (or any fed cell at or
     * before it that their iterator has not reached) */
    while (d->pq_len > 0 && P_IDX(d->pq[0]) <= idx) {
        uint64_t ent = heap_pop(d->pq, &d->pq_len);
        riblt_peeled* p = &d->peeled[P_ID(ent)];
        if (p->next < d->comb_len && d->fed[p->next]) cell_remove(&d->comb[p->next], p->sym, p->sign, d->seed);
        p->next += next_skip(p->next, &p->xs);
        heap_push(d->pq, &d->pq_len, PACK(p->next, P_ID(ent)));
    }
    if (peel(d) != 0) return -1;
    int done = riblt_dec_decoded(d);
    if (done && d->first_decode_after == 0) d->first_decode_after = d->fed_n;
    return done;
}
