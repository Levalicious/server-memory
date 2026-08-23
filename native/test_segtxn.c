/*
 * test_segtxn.c — txn layer units + crash harness over REAL COW txns.
 *
 * Property set on top of segfile's R1-R3: logical-state equality. The model
 * snapshots the full logical page space after every acked commit; every
 * event-prefix crash state must recover to txid ∈ {acked, acked+1} with
 * every logical page byte-identical to that txid's snapshot — through page
 * recycling (freelist reuse), table growth, frees, and pin-gated windows.
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

/* read-only mem io (same shim as test_segfile) */
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
    for (u16 i = 0; i < size; i++) rec[i] = (u8)(salt * 131 + i * 7);
}

int main(void) {
    printf("test_segtxn:\n");

    TEST(alloc_touch_read_roundtrip);
    {
        segstore_t *st = segstore_create(seg_io_sim_open(), 1, 2);
        assert(st && segstore_txid(st) == 0);
        assert(seg_txn_begin(st));
        u32 lpg; u8 *pg = seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg);
        assert(pg && lpg == 0);
        u8 rec[50]; fill_rec(rec, 50, 1);
        u16 slot; assert(seg_page_insert(pg, rec, 50, &slot));
        assert(seg_txn_commit(st));
        assert(segstore_txid(st) == 1 && segstore_logical_pages(st) == 1);
        const u8 *rd = segstore_read(st, 0);
        assert(rd && seg_page_validate(rd));
        u16 sz; const u8 *r = seg_page_read(rd, slot, &sz);
        assert(r && sz == 50 && memcmp(r, rec, 50) == 0);
        /* touch + modify in txn 2; committed view unchanged until commit */
        assert(seg_txn_begin(st));
        u8 *w = seg_txn_touch(st, 0);
        assert(w);
        u8 rec2[60]; fill_rec(rec2, 60, 2);
        assert(seg_page_insert(w, rec2, 60, &slot));
        assert(seg_page_read(segstore_read(st, 0), slot, &sz) == NULL); /* not yet */
        assert(seg_txn_commit(st));
        r = seg_page_read(segstore_read(st, 0), slot, &sz);
        assert(r && sz == 60 && memcmp(r, rec2, 60) == 0);
        segstore_close(st);
    }
    PASS();

    TEST(abort_discards_free_unmaps_noop_commits);
    {
        segstore_t *st = segstore_create(seg_io_sim_open(), 1, 2);
        assert(seg_txn_begin(st));
        u32 a, b;
        assert(seg_txn_alloc(st, SEG_KIND_ENTITY, &a));
        assert(seg_txn_alloc(st, SEG_KIND_ADJ, &b));
        assert(seg_txn_commit(st) && segstore_logical_pages(st) == 2);
        u64 t = segstore_txid(st);
        /* abort discards */
        assert(seg_txn_begin(st));
        u8 *w = seg_txn_touch(st, a); assert(w);
        u8 rec[20]; fill_rec(rec, 20, 9);
        u16 slot; assert(seg_page_insert(w, rec, 20, &slot));
        seg_txn_abort(st);
        assert(segstore_txid(st) == t);
        assert(seg_page_read(segstore_read(st, a), slot, NULL) == NULL);
        /* free unmaps; double-free refused; touch-after-free refused */
        assert(seg_txn_begin(st));
        assert(seg_txn_free(st, b) == 1);
        assert(seg_txn_free(st, b) == 0);
        assert(seg_txn_touch(st, b) == NULL);
        assert(seg_txn_commit(st));
        assert(segstore_read(st, b) == NULL);
        assert(segstore_read(st, a) != NULL);
        /* no-op commit does not advance txid */
        u64 t2 = segstore_txid(st);
        assert(seg_txn_begin(st) && seg_txn_commit(st));
        assert(segstore_txid(st) == t2);
        segstore_close(st);
    }
    PASS();

    TEST(reopen_rebuilds_ptable_and_freelist);
    {
        seg_io_t *io = seg_io_sim_open();
        segstore_t *st = segstore_create(io, 7, 2);
        u8 rec[40];
        for (u64 t = 1; t <= 8; t++) {
            assert(seg_txn_begin(st));
            u32 lpg; u8 *pg = seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg);
            assert(pg);
            fill_rec(rec, 40, t);
            u16 slot; assert(seg_page_insert(pg, rec, 40, &slot));
            if (t == 5) assert(seg_txn_free(st, 1));      /* churn */
            assert(seg_txn_commit(st));
        }
        u64 size; const u8 *base = io->read_base(io, &size);
        u8 *copy = (u8 *)malloc(size); memcpy(copy, base, size);
        segstore_close(st);
        segstore_t *st2 = segstore_open(io_mem(copy, size));
        assert(st2 && segstore_txid(st2) == 8);
        assert(segstore_logical_pages(st2) == 8);
        assert(segstore_read(st2, 1) == NULL);            /* stayed freed */
        for (u32 l = 0; l < 8; l++) {
            if (l == 1) continue;
            const u8 *rd = segstore_read(st2, l);
            assert(rd && seg_page_validate(rd));
        }
        segstore_close(st2);
        free(copy);
    }
    PASS();

    TEST(physical_reuse_bounds_file_growth);
    {
        seg_io_t *io = seg_io_sim_open();
        segstore_t *st = segstore_create(io, 2, 2);
        /* one long-lived page, then heavy touch churn: physical space must
         * stabilize (freelist recycling), not grow linearly with txns */
        assert(seg_txn_begin(st));
        u32 lpg; assert(seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg));
        assert(seg_txn_commit(st));
        u8 rec[30];
        u64 size_mid = 0;
        for (u64 t = 0; t < 60; t++) {
            assert(seg_txn_begin(st));
            u8 *w = seg_txn_touch(st, lpg);
            assert(w);
            fill_rec(rec, 30, t);
            u16 slot;
            if (!seg_page_insert(w, rec, 30, &slot)) {    /* page filled: reset */
                seg_txn_abort(st);
                assert(seg_txn_begin(st));
                w = seg_txn_touch(st, lpg);
                /* delete everything live to make room */
                for (u32 s = 0; s < 200; s++) seg_page_delete(w, (u16)s);
                assert(seg_page_insert(w, rec, 30, &slot));
            }
            assert(seg_txn_commit(st));
            if (t == 19) io->read_base(io, &size_mid);    /* size after warmup */
        }
        u64 size_end = 0;
        io->read_base(io, &size_end);
        /* recycling bound: the last 40 churn txns may not grow the file more
         * than a handful of extent clusters (without reuse: 40 txns x ~4
         * pages each = 160 pages = 655K of growth). */
        assert(size_end - size_mid <= 16 * SEG_PAGE_SIZE);
        assert(segstore_logical_pages(st) == 1);
        segstore_close(st);
    }
    PASS();

    TEST(pins_freeze_snapshots_and_gate_reuse);
    {
        segstore_t *st = segstore_create(seg_io_sim_open(), 3, 2);
        u8 rec[80]; u16 slot0 = 0;
        assert(seg_txn_begin(st));
        u32 lpg; u8 *pg = seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg);
        fill_rec(rec, 80, 100);
        assert(seg_page_insert(pg, rec, 80, &slot0));
        assert(seg_txn_commit(st));

        segpin_t *pin = seg_pin(st);
        assert(pin && pin->txid == 1);
        const u8 *pinned_before = seg_pin_read(st, pin, lpg);
        assert(pinned_before);
        u8 frozen[SEG_PAGE_SIZE]; memcpy(frozen, pinned_before, SEG_PAGE_SIZE);

        /* 20 commits of churn while the pin is held */
        for (u64 t = 0; t < 20; t++) {
            assert(seg_txn_begin(st));
            u8 *w = seg_txn_touch(st, lpg); assert(w);
            fill_rec(rec, 80, 200 + t);
            u16 s;
            if (!seg_page_insert(w, rec, 80, &s)) {
                for (u32 k = 0; k < 100; k++) if (k != slot0) seg_page_delete(w, (u16)k);
                assert(seg_page_insert(w, rec, 80, &s));
            }
            assert(seg_txn_commit(st));
        }
        /* the pinned view must be byte-identical: reuse was gated */
        const u8 *pinned_after = seg_pin_read(st, pin, lpg);
        assert(pinned_after && memcmp(pinned_after, frozen, SEG_PAGE_SIZE) == 0);
        seg_unpin(st, pin);
        segstore_close(st);
    }
    PASS();

    /* ================= crash harness: real COW txns ================= */

    TEST(crash_prefix_replay_txn_workload);
    {
        enum { NTX = 15, MAXL = 32 };
        seg_io_t *io = seg_io_sim_open();
        segstore_t *st = segstore_create(io, 4, 2);
        assert(st);

        static u8  snap[NTX + 1][MAXL][SEG_PAGE_SIZE];
        static int snap_live[NTX + 1][MAXL];
        static u64 snap_lp[NTX + 1];
        static u32 ack_ev[NTX + 1];
        memset(snap_live, 0, sizeof snap_live);
        snap_lp[0] = 0;
        ack_ev[0] = seg_io_sim_event_count(io);

        u8 rec[100];
        for (u64 t = 1; t <= NTX; t++) {
            assert(seg_txn_begin(st));
            /* mixed workload: alloc on most txns, touch olds, free some */
            if (t % 4 != 0 || segstore_logical_pages(st) == 0) {
                u32 lpg; u8 *pg = seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg);
                assert(pg && lpg < MAXL);
                fill_rec(rec, (u16)(20 + t), t * 1000 + lpg);
                u16 s; assert(seg_page_insert(pg, rec, (u16)(20 + t), &s));
            }
            for (u32 l = 0; l < segstore_logical_pages(st); l++) {
                if ((l + t) % 3 != 0) continue;
                u8 *w = seg_txn_touch(st, l);
                if (!w) continue;                        /* freed page */
                fill_rec(rec, 24, t * 500 + l);
                u16 s;
                if (!seg_page_insert(w, rec, 24, &s)) {
                    for (u32 k = 0; k < 170; k++) seg_page_delete(w, (u16)k);
                    assert(seg_page_insert(w, rec, 24, &s));
                }
            }
            if (t == 7) assert(seg_txn_free(st, 2));
            if (t == 11) assert(seg_txn_free(st, 5));
            assert(seg_txn_commit(st));

            /* snapshot committed logical state */
            snap_lp[t] = segstore_logical_pages(st);
            for (u32 l = 0; l < MAXL; l++) {
                const u8 *rd = (l < snap_lp[t]) ? segstore_read(st, l) : NULL;
                snap_live[t][l] = rd != NULL;
                if (rd) memcpy(snap[t][l], rd, SEG_PAGE_SIZE);
            }
            ack_ev[t] = seg_io_sim_event_count(io);
        }

        u32 nev = seg_io_sim_event_count(io);
        u32 states = 0, plus_one = 0, skipped_pre = 0;
        for (u32 k = 0; k <= nev; k++) {
            u64 size = 0;
            u8 *disk = seg_io_sim_replay_prefix(io, k, &size);
            u64 acked = 0;
            for (u64 t = 0; t <= NTX; t++) if (ack_ev[t] <= k) acked = t;
            seg_io_t *ro = io_mem(disk, size);
            segstore_t *r = segstore_open(ro);
            if (ack_ev[0] > k) {          /* pre-create crash */
                if (r) segstore_close(r); else free(ro);
                free(disk); skipped_pre++; continue;
            }
            assert(r != NULL);
            u64 rt = segstore_txid(r);
            assert(rt == acked || rt == acked + 1);
            if (rt == acked + 1) plus_one++;
            assert(segstore_logical_pages(r) == snap_lp[rt]);
            for (u32 l = 0; l < MAXL; l++) {
                const u8 *rd = (l < snap_lp[rt]) ? segstore_read(r, l) : NULL;
                if (!snap_live[rt][l]) { assert(rd == NULL); continue; }
                assert(rd && memcmp(rd, snap[rt][l], SEG_PAGE_SIZE) == 0);
                assert(seg_page_validate(rd) == 1);
            }
            segstore_close(r);
            free(disk);
            states++;
        }
        printf("(%u states, +1 %u) ", states, plus_one);
        assert(states > 100);             /* txn commits emit many events */
        assert(plus_one > 0);
        segstore_close(st);
    }
    PASS();

    printf("test_segtxn: %d tests passed\n", tests_run);
    return 0;
}
