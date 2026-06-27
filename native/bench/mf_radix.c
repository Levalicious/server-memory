#include "mf_radix.h"

typedef struct __attribute__((packed)) { u64 size; u64 next; } mfr_free_t;

u64 mfr_coalesce_threshold = 0;
u64 mfr_coalesce_calls = 0;

static inline u64 round16(u64 s) {
    s = (s + (MFR_QUANTUM - 1)) & ~(u64)(MFR_QUANTUM - 1);
    return s < MFR_MIN_BLOCK ? MFR_MIN_BLOCK : s;
}
static inline mfr_free_t *fb(mf_t *mf, u64 off) { return (mfr_free_t *)mf_ptr(mf, off); }

u64 mfr_alloc(mf_t *mf, u64 size) {
    u64 n = round16(size);

    u64 prev = 0, cur = mf->hdr->free_root;
    while (cur) {
        mfr_free_t *b = fb(mf, cur);
        if (b->size >= n) {
            u64 rem = b->size - n;     /* 0 or >= 16 (all multiples of 16) */
            u64 next = b->next;
            if (rem >= MFR_MIN_BLOCK) {
                u64 lo = cur + n;
                mfr_free_t *lb = fb(mf, lo);
                lb->size = rem;
                lb->next = next;
                if (prev) fb(mf, prev)->next = lo; else mf->hdr->free_root = lo;
                mf->hdr->free_bytes -= n;          /* remainder stays; count unchanged */
            } else {
                if (prev) fb(mf, prev)->next = next; else mf->hdr->free_root = next;
                mf->hdr->free_bytes -= n;          /* exact fit (rem==0) */
                mf->hdr->free_count -= 1;
            }
            return cur;
        }
        prev = cur;
        cur = b->next;
    }

    if (mf_ensure(mf, n) < 0) return 0;
    u64 off = mf->hdr->allocated;
    mf->hdr->allocated += n;
    return off;
}

void mfr_free(mf_t *mf, u64 off, u64 size) {
    if (!off) return;
    u64 n = round16(size);
    mfr_free_t *b = fb(mf, off);
    b->size = n;
    b->next = mf->hdr->free_root;
    mf->hdr->free_root = off;
    mf->hdr->free_bytes += n;
    mf->hdr->free_count += 1;
    if (mfr_coalesce_threshold && mf->hdr->free_count > mfr_coalesce_threshold)
        mfr_coalesce(mf);
}

u64 mfr_freelist_len(mf_t *mf) { return mf->hdr->free_count; }
u64 mfr_free_bytes(mf_t *mf)   { return mf->hdr->free_bytes; }

/* LSD radix sort of the free list by offset (8 bits/pass, bucket sublists), then linear merge. */
void mfr_coalesce(mf_t *mf) {
    u64 head = mf->hdr->free_root;
    if (!head) return;
    mfr_coalesce_calls++;

    enum { BITS = 8, RADIX = 256 };
    int passes = 0;
    for (u64 m = mf->hdr->file_size; m; m >>= BITS) passes++;
    if (!passes) passes = 1;

    for (int p = 0; p < passes; p++) {
        u64 bhead[RADIX], btail[RADIX];
        for (int i = 0; i < RADIX; i++) { bhead[i] = 0; btail[i] = 0; }

        for (u64 cur = head; cur; ) {
            mfr_free_t *b = fb(mf, cur);
            u64 next = b->next;
            int d = (int)((cur >> (p * BITS)) & (RADIX - 1));
            if (!bhead[d]) bhead[d] = cur; else fb(mf, btail[d])->next = cur;
            btail[d] = cur;
            cur = next;
        }

        u64 nh = 0, nt = 0;
        for (int i = 0; i < RADIX; i++) {
            if (!bhead[i]) continue;
            if (!nh) nh = bhead[i]; else fb(mf, nt)->next = bhead[i];
            nt = btail[i];
        }
        if (nt) fb(mf, nt)->next = 0;
        head = nh;
    }

    u64 count = 0;
    for (u64 cur = head; cur; ) {
        mfr_free_t *b = fb(mf, cur);
        u64 next = b->next;
        while (next && cur + b->size == next) {
            mfr_free_t *nb = fb(mf, next);
            b->size += nb->size;
            next = nb->next;
            b->next = next;
        }
        count++;
        cur = next;
    }

    mf->hdr->free_root = head;
    mf->hdr->free_count = count;   /* free_bytes unchanged: merging conserves bytes */
}
