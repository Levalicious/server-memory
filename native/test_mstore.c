/*
 * test_mstore.c — manifest store units + CROSS-SEGMENT crash harness.
 *
 * The new property beyond seg-level R1-R3: cross-segment ATOMICITY. A store
 * commit touching segments {A,B} must never recover with A at the new state
 * and B at the old one — the manifest pivot + dual-meta rollback (open_at)
 * must always select one consistent store txid. The harness cuts one global
 * event clock through manifest + all segment files and checks every point.
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

typedef struct { seg_io_t vt; const u8 *buf; u64 size; } io_mem_t;
static int mem_write(seg_io_t *io, const void *b, u64 l, u64 o){(void)io;(void)b;(void)l;(void)o;return 0;}
static int mem_sync(seg_io_t *io){(void)io;return 1;}
static int mem_extend(seg_io_t *io, u64 n){(void)io;(void)n;return 0;}
static const u8 *mem_base(seg_io_t *io, u64 *s){io_mem_t*m=(io_mem_t*)io;if(s)*s=m->size;return m->buf;}
static void mem_close(seg_io_t *io){free(io);}
static seg_io_t *io_mem(const u8 *buf, u64 size){
    io_mem_t *m=(io_mem_t*)calloc(1,sizeof *m);
    m->vt.write=mem_write;m->vt.sync=mem_sync;m->vt.extend=mem_extend;
    m->vt.read_base=mem_base;m->vt.close=mem_close;m->buf=buf;m->size=size;return &m->vt;
}

static void fill_rec(u8 *rec, u16 size, u64 salt) {
    for (u16 i = 0; i < size; i++) rec[i] = (u8)(salt * 197 + i * 11);
}

enum { NSEG = 3 };

int main(void) {
    printf("test_mstore:\n");

    TEST(create_multiseg_commit_reopen);
    {
        seg_io_t *sios[NSEG];
        for (u32 s = 0; s < NSEG; s++) sios[s] = seg_io_sim_open();
        seg_io_t *mio = seg_io_sim_open();
        mstore_t *ms = mstore_create(mio, sios, NSEG, 2);
        assert(ms && mstore_txid(ms) == 0);

        /* multi-seg tx: pages in segs 0 and 2; seg 1 untouched */
        assert(mstore_txn_begin(ms));
        u8 rec[64]; u16 slot;
        u32 l0, l2;
        u8 *p0 = seg_txn_alloc(mstore_seg(ms, 0), SEG_KIND_ENTITY, &l0);
        u8 *p2 = seg_txn_alloc(mstore_seg(ms, 2), SEG_KIND_ENTITY, &l2);
        assert(p0 && p2);
        fill_rec(rec, 64, 1); assert(seg_page_insert(p0, rec, 64, &slot));
        fill_rec(rec, 64, 2); assert(seg_page_insert(p2, rec, 64, &slot));
        assert(mstore_txn_commit(ms));
        assert(mstore_txid(ms) == 1);
        assert(segstore_txid(mstore_seg(ms, 0)) == 1);
        assert(segstore_txid(mstore_seg(ms, 1)) == 0);      /* untouched: no advance */
        assert(segstore_txid(mstore_seg(ms, 2)) == 1);

        /* single-seg tx (sparse txids on other segs) */
        assert(mstore_txn_begin(ms));
        u8 *w = seg_txn_touch(mstore_seg(ms, 0), l0);
        assert(w);
        fill_rec(rec, 32, 3); assert(seg_page_insert(w, rec, 32, &slot));
        assert(mstore_txn_commit(ms));
        assert(mstore_txid(ms) == 2);
        assert(segstore_txid(mstore_seg(ms, 0)) == 2);
        assert(segstore_txid(mstore_seg(ms, 2)) == 1);      /* sparse */

        /* no-op store tx */
        assert(mstore_txn_begin(ms) && mstore_txn_commit(ms));
        assert(mstore_txid(ms) == 2);

        /* reopen from copies */
        u8 *imgs[NSEG + 1]; u64 sizes[NSEG + 1];
        for (u32 s = 0; s < NSEG; s++) {
            const u8 *b = sios[s]->read_base(sios[s], &sizes[s]);
            imgs[s] = (u8 *)malloc(sizes[s]); memcpy(imgs[s], b, sizes[s]);
        }
        const u8 *mb = mio->read_base(mio, &sizes[NSEG]);
        imgs[NSEG] = (u8 *)malloc(sizes[NSEG]); memcpy(imgs[NSEG], mb, sizes[NSEG]);
        mstore_close(ms);

        seg_io_t *rio[NSEG];
        for (u32 s = 0; s < NSEG; s++) rio[s] = io_mem(imgs[s], sizes[s]);
        mstore_t *ms2 = mstore_open(io_mem(imgs[NSEG], sizes[NSEG]), rio, NSEG);
        assert(ms2 && mstore_txid(ms2) == 2);
        const u8 *rd = segstore_read(mstore_seg(ms2, 0), l0);
        assert(rd && seg_page_validate(rd));
        rd = segstore_read(mstore_seg(ms2, 2), l2);
        assert(rd && seg_page_validate(rd));
        mstore_close(ms2);
        for (u32 s = 0; s <= NSEG; s++) free(imgs[s]);
    }
    PASS();

    TEST(crash_cross_segment_atomicity);
    {
        enum { NTX = 10, MAXL = 16 };
        seg_io_t *sios[NSEG];
        for (u32 s = 0; s < NSEG; s++) sios[s] = seg_io_sim_open();
        seg_io_t *mio = seg_io_sim_open();
        mstore_t *ms = mstore_create(mio, sios, NSEG, 2);
        assert(ms);

        /* model: per acked store txid, per-seg logical page images */
        static u8  snap[NTX + 1][NSEG][MAXL][SEG_PAGE_SIZE];
        static int snap_live[NTX + 1][NSEG][MAXL];
        static u64 snap_lp[NTX + 1][NSEG];
        static u64 ack_gseq[NTX + 1];
        memset(snap_live, 0, sizeof snap_live);
        memset(snap_lp, 0, sizeof snap_lp);
        ack_gseq[0] = seg_io_sim_gseq_now();

        u8 rec[80];
        for (u64 t = 1; t <= NTX; t++) {
            assert(mstore_txn_begin(ms));
            /* every txn touches a t-dependent SUBSET of segments — the
             * multi-seg windows are where atomicity can break */
            for (u32 s = 0; s < NSEG; s++) {
                if (((t + s) % 3) == 0 && t != 1) continue;   /* skip seg */
                segstore_t *seg = mstore_seg(ms, s);
                u32 lpg; u16 sl;
                if (segstore_logical_pages(seg) < 3) {
                    u8 *pg = seg_txn_alloc(seg, SEG_KIND_ENTITY, &lpg);
                    assert(pg);
                    fill_rec(rec, 40, t * 100 + s * 10 + lpg);
                    assert(seg_page_insert(pg, rec, 40, &sl));
                } else {
                    lpg = (u32)((t + s) % segstore_logical_pages(seg));
                    u8 *w = seg_txn_touch(seg, lpg);
                    assert(w);
                    fill_rec(rec, 30, t * 100 + s * 10 + lpg);
                    if (!seg_page_insert(w, rec, 30, &sl)) {
                        for (u32 k = 0; k < 170; k++) seg_page_delete(w, (u16)k);
                        assert(seg_page_insert(w, rec, 30, &sl));
                    }
                }
            }
            assert(mstore_txn_commit(ms));
            assert(mstore_txid(ms) == t);

            for (u32 s = 0; s < NSEG; s++) {
                segstore_t *seg = mstore_seg(ms, s);
                snap_lp[t][s] = segstore_logical_pages(seg);
                for (u32 l = 0; l < MAXL; l++) {
                    const u8 *rd = (l < snap_lp[t][s]) ? segstore_read(seg, l) : NULL;
                    snap_live[t][s][l] = rd != NULL;
                    if (rd) memcpy(snap[t][s][l], rd, SEG_PAGE_SIZE);
                }
            }
            ack_gseq[t] = seg_io_sim_gseq_now();
        }

        u64 gmax = seg_io_sim_gseq_now();
        u32 states = 0, plus_one = 0;
        for (u64 g = 0; g <= gmax; g++) {
            u8 *imgs[NSEG + 1]; u64 sizes[NSEG + 1];
            for (u32 s = 0; s < NSEG; s++)
                imgs[s] = seg_io_sim_replay_gseq(sios[s], g, &sizes[s]);
            imgs[NSEG] = seg_io_sim_replay_gseq(mio, g, &sizes[NSEG]);

            u64 acked = 0;
            for (u64 t = 0; t <= NTX; t++) if (ack_gseq[t] <= g) acked = t;

            seg_io_t *rio[NSEG];
            for (u32 s = 0; s < NSEG; s++) rio[s] = io_mem(imgs[s], sizes[s]);
            seg_io_t *rmio = io_mem(imgs[NSEG], sizes[NSEG]);
            mstore_t *r = mstore_open(rmio, rio, NSEG);
            if (ack_gseq[0] > g) {                     /* pre-create crash */
                if (r) mstore_close(r);
                /* else: open consumed every io wrapper (uniform ownership) */
                for (u32 s = 0; s <= NSEG; s++) free(imgs[s]);
                continue;
            }
            assert(r != NULL);                         /* creation acked: must open */
            u64 rt = mstore_txid(r);
            assert(rt == acked || rt == acked + 1);
            if (rt == acked + 1) plus_one++;
            /* THE atomicity check: every segment must present exactly the
             * store-txid-rt state — no mixed states, ever. */
            for (u32 s = 0; s < NSEG; s++) {
                segstore_t *seg = mstore_seg(r, s);
                assert(segstore_logical_pages(seg) == snap_lp[rt][s]);
                for (u32 l = 0; l < MAXL; l++) {
                    const u8 *rd = (l < snap_lp[rt][s]) ? segstore_read(seg, l) : NULL;
                    if (!snap_live[rt][s][l]) { assert(rd == NULL); continue; }
                    assert(rd && memcmp(rd, snap[rt][s][l], SEG_PAGE_SIZE) == 0);
                }
            }
            mstore_close(r);
            for (u32 s = 0; s <= NSEG; s++) free(imgs[s]);
            states++;
        }
        printf("(%u states, +1 %u) ", states, plus_one);
        assert(states > 100);
        mstore_close(ms);
    }
    PASS();

    printf("test_mstore: %d tests passed\n", tests_run);
    return 0;
}
