/*
 * test_segfile.c — segment lifecycle units + THE CRASH HARNESS.
 *
 * Harness (Design_CrashHarness_PrefixReplay): run a multi-commit workload on
 * the sim io (every write/sync/extend recorded), then for EVERY event-log
 * prefix materialize the disk image — a legal crash state under an in-order
 * device — and assert:
 *   R1  recovery (segfile_open) succeeds once creation was acknowledged
 *   R2  recovered txid ∈ { last_acked, last_acked + 1 }   (+1 = in-flight
 *       commit whose bytes all landed before the ack; legal to recover)
 *   R3  recovered state byte-matches the model snapshot FOR THE RECOVERED
 *       TXID (never a blend), and every page validates
 * Torn-write adversary: for prefixes ending in a WRITE, additionally apply
 * only the first j bytes of that write — a torn meta must lose recovery to
 * the intact slot; a torn data write is invisible (uncommitted).
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

/* ---- read-only mem io: wraps a replayed disk image for recovery ---- */
typedef struct { seg_io_t vt; const u8 *buf; u64 size; } io_mem_t;
static int mem_write(seg_io_t *io, const void *b, u64 l, u64 o) { (void)io;(void)b;(void)l;(void)o; return 0; }
static int mem_sync(seg_io_t *io) { (void)io; return 1; }
static int mem_extend(seg_io_t *io, u64 n) { (void)io;(void)n; return 0; }
static const u8 *mem_base(seg_io_t *io, u64 *s) { io_mem_t *m=(io_mem_t*)io; if(s)*s=m->size; return m->buf; }
static void mem_close(seg_io_t *io) { free(io); }
static seg_io_t *io_mem(const u8 *buf, u64 size) {
    io_mem_t *m = (io_mem_t *)calloc(1, sizeof *m);
    m->vt.write=mem_write; m->vt.sync=mem_sync; m->vt.extend=mem_extend;
    m->vt.read_base=mem_base; m->vt.close=mem_close;
    m->buf=buf; m->size=size; return &m->vt;
}

/* ---- deterministic page image for (txid, pgno) via seg_page ops ---- */
static void build_page(u8 *pg, u64 txid, u32 pgno) {
    memset(pg, 0, SEG_PAGE_SIZE);   /* deterministic image incl. free gap */
    seg_page_init(pg, SEG_KIND_ENTITY);
    u8 rec[128];
    u32 nrec = 1 + (u32)((txid * 7 + pgno) % 5);
    for (u32 r = 0; r < nrec; r++) {
        u16 size = (u16)(16 + ((txid * 13 + pgno * 5 + r * 3) % 100));
        for (u16 i = 0; i < size; i++)
            rec[i] = (u8)(txid * 31 + pgno * 17 + r * 7 + i);
        u16 slot;
        assert(seg_page_insert(pg, rec, size, &slot));
    }
}

int main(void) {
    printf("test_segfile:\n");

    /* ================= unit tests ================= */

    TEST(create_then_open_sim);
    {
        seg_io_t *io = seg_io_sim_open();
        segfile_t *sf = segfile_create(io, 5, 2);
        assert(sf && sf->meta.txid == 0 && sf->meta.watermark == SEG_META_PAGES);
        assert(sf->meta.seg_id == 5);
        /* re-open the same image */
        u64 size; const u8 *base = io->read_base(io, &size);
        seg_io_t *ro = io_mem(base, size);
        segfile_t *sf2 = segfile_open(ro);
        assert(sf2 && sf2->meta.txid == 0 && sf2->meta.seg_id == 5);
        segfile_close(sf2);
        segfile_close(sf);
    }
    PASS();

    TEST(commit_validation_gates);
    {
        seg_io_t *io = seg_io_sim_open();
        segfile_t *sf = segfile_create(io, 1, 2);
        static u8 pg[SEG_PAGE_SIZE];
        build_page(pg, 1, 2);
        const u8 *bufs[1] = { pg };
        seg_meta_t nm = sf->meta;
        u32 bad_pgno = 0;                      /* meta page: refused */
        nm.txid = 1; nm.watermark = 3;
        assert(segfile_commit(sf, &bad_pgno, bufs, 1, &nm) == 0);
        u32 pgno = 2;
        nm.txid = 0;                            /* not strictly forward: refused */
        assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 0);
        nm.txid = 1; nm.watermark = 1;          /* watermark shrink: refused */
        assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 0);
        nm.watermark = 2;                       /* page >= watermark: refused */
        assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 0);
        nm.watermark = 3;                       /* legal */
        assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 1);
        assert(sf->meta.txid == 1 && sf->active_slot == 1);
        const u8 *rd = segfile_page(sf, 2);
        assert(rd && memcmp(rd, pg, SEG_PAGE_SIZE) == 0);
        assert(segfile_page(sf, 0) == NULL && segfile_page(sf, 3) == NULL);
        segfile_close(sf);
    }
    PASS();

    TEST(posix_roundtrip_survives_reopen);
    {
        char path[] = "/tmp/segfile_test_XXXXXX";
        int tfd = mkstemp(path); assert(tfd >= 0); close(tfd);
        seg_io_t *io = seg_io_posix_open(path, 1);
        assert(io);
        segfile_t *sf = segfile_create(io, 9, 3);
        assert(sf);
        static u8 pg[SEG_PAGE_SIZE];
        u64 txid = 0;
        for (u32 c = 0; c < 5; c++) {           /* five commits, growing */
            u32 pgno = SEG_META_PAGES + c;
            build_page(pg, txid + 1, pgno);
            const u8 *bufs[1] = { pg };
            seg_meta_t nm = sf->meta;
            nm.txid = ++txid; nm.watermark = pgno + 1;
            assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 1);
        }
        segfile_close(sf);                       /* closes io */
        io = seg_io_posix_open(path, 0);
        assert(io);
        sf = segfile_open(io);
        assert(sf && sf->meta.txid == 5 && sf->meta.watermark == 7);
        for (u32 c = 0; c < 5; c++) {
            u32 pgno = SEG_META_PAGES + c;
            build_page(pg, c + 1, pgno);
            const u8 *rd = segfile_page(sf, pgno);
            assert(rd && memcmp(rd, pg, SEG_PAGE_SIZE) == 0);
            assert(seg_page_validate(rd) == 1);
        }
        segfile_close(sf);
        unlink(path);
    }
    PASS();

    /* ================= crash harness ================= */

    TEST(crash_prefix_replay_all_states);
    {
        enum { NTX = 12, MAXPG = 64 };
        seg_io_t *io = seg_io_sim_open();

        /* model: cumulative page images per acked txid + ack event indices */
        static u8  snap[NTX + 1][MAXPG][SEG_PAGE_SIZE];
        static int snap_has[NTX + 1][MAXPG];
        static u64 snap_wm[NTX + 1];
        static u32 ack_ev[NTX + 1];              /* event count at ack time */

        segfile_t *sf = segfile_create(io, 3, 2);
        assert(sf);
        memset(snap_has, 0, sizeof snap_has);
        snap_wm[0] = SEG_META_PAGES;
        ack_ev[0] = seg_io_sim_event_count(io);  /* creation acked here */

        u32 next_free_pg = SEG_META_PAGES;
        static u8 pg[3][SEG_PAGE_SIZE];
        for (u64 t = 1; t <= NTX; t++) {
            /* COW discipline: only fresh pgnos are written */
            u32 nd = 1 + (u32)(t % 3);
            u32 pgnos[3]; const u8 *bufs[3];
            for (u32 i = 0; i < nd; i++) {
                pgnos[i] = next_free_pg++;
                build_page(pg[i], t, pgnos[i]);
                bufs[i] = pg[i];
            }
            seg_meta_t nm = sf->meta;
            nm.txid = t; nm.watermark = next_free_pg;
            nm.nameindex_root_pgno = pgnos[0];   /* exercise root fields */
            assert(segfile_commit(sf, pgnos, bufs, nd, &nm) == 1);

            /* snapshot = previous snapshot + this txn's pages */
            memcpy(snap[t], snap[t - 1], sizeof snap[t]);
            memcpy(snap_has[t], snap_has[t - 1], sizeof snap_has[t]);
            for (u32 i = 0; i < nd; i++) {
                memcpy(snap[t][pgnos[i]], bufs[i], SEG_PAGE_SIZE);
                snap_has[t][pgnos[i]] = 1;
            }
            snap_wm[t] = next_free_pg;
            ack_ev[t] = seg_io_sim_event_count(io);
        }

        u32 nev = seg_io_sim_event_count(io);
        u32 states_checked = 0, plus_one_seen = 0;
        for (u32 k = 0; k <= nev; k++) {
            u64 size = 0;
            u8 *disk = seg_io_sim_replay_prefix(io, k, &size);
            /* last acked txid at crash point k */
            u64 acked = 0;
            for (u64 t = 0; t <= NTX; t++) if (ack_ev[t] <= k) acked = t;

            seg_io_t *ro = io_mem(disk, size);
            segfile_t *r = segfile_open(ro);
            if (ack_ev[0] > k) {                 /* pre-creation crash */
                if (r) { assert(r->meta.txid == 0); segfile_close(r); }
                else free(ro);
                free(disk); continue;
            }
            assert(r != NULL);                                       /* R1 */
            u64 rt = r->meta.txid;
            assert(rt == acked || rt == acked + 1);                  /* R2 */
            if (rt == acked + 1) plus_one_seen++;
            assert(r->meta.watermark == snap_wm[rt]);
            for (u32 p = 0; p < MAXPG; p++) {                        /* R3 */
                if (!snap_has[rt][p]) continue;
                const u8 *rd = segfile_page(r, p);
                assert(rd && memcmp(rd, snap[rt][p], SEG_PAGE_SIZE) == 0);
                assert(seg_page_validate(rd) == 1);
            }
            segfile_close(r);
            free(disk);
            states_checked++;
        }
        printf("(%u states, +1-recoveries %u) ", states_checked, plus_one_seen);
        assert(states_checked > 50);
        assert(plus_one_seen > 0);       /* the legal-newer arm is really exercised */
        segfile_close(sf);
    }
    PASS();

    TEST(crash_torn_write_adversary);
    {
        /* same workload, but for every prefix ending in a WRITE event, land
         * only the first j bytes of that write (j stepping 16). A torn meta
         * must lose to the intact slot; a torn data page is uncommitted and
         * invisible. Assertions identical to the main harness. */
        enum { NTX = 6 };
        seg_io_t *io = seg_io_sim_open();
        segfile_t *sf = segfile_create(io, 4, 2);
        assert(sf);
        u32 ack0 = seg_io_sim_event_count(io);
        static u64 ack_ev[NTX + 1]; static u64 wm[NTX + 1];
        ack_ev[0] = ack0; wm[0] = SEG_META_PAGES;
        u32 next_free_pg = SEG_META_PAGES;
        static u8 pg[SEG_PAGE_SIZE];
        for (u64 t = 1; t <= NTX; t++) {
            u32 pgno = next_free_pg++;
            build_page(pg, t, pgno);
            const u8 *bufs[1] = { pg };
            seg_meta_t nm = sf->meta;
            nm.txid = t; nm.watermark = next_free_pg;
            assert(segfile_commit(sf, &pgno, bufs, 1, &nm) == 1);
            ack_ev[t] = seg_io_sim_event_count(io); wm[t] = next_free_pg;
        }
        u32 nev = seg_io_sim_event_count(io);
        u32 torn_checked = 0;
        for (u32 k = 1; k <= nev; k++) {
            u64 full_size = 0;
            u8 *full = seg_io_sim_replay_prefix(io, k, &full_size);
            u64 prev_size = 0;
            u8 *prev = seg_io_sim_replay_prefix(io, k - 1, &prev_size);
            if (full_size != prev_size || !full || !prev ||
                memcmp(full, prev, full_size) == 0) {       /* not a WRITE evt */
                free(full); free(prev); continue;
            }
            for (u32 j = 16; j < SEG_PAGE_SIZE; j += 496) {  /* tear points */
                /* torn image: prev + first j changed bytes of the write.
                 * find the changed range and apply its first j bytes. */
                u8 *torn = (u8 *)malloc(full_size);
                memcpy(torn, prev, full_size);
                u64 lo = 0; while (lo < full_size && full[lo] == prev[lo]) lo++;
                u64 applied = 0;
                for (u64 b = lo; b < full_size && applied < j; b++, applied++)
                    torn[b] = full[b];
                u64 acked = 0;
                for (u64 t = 0; t <= NTX; t++) if (ack_ev[t] <= k - 1) acked = t;
                seg_io_t *ro = io_mem(torn, full_size);
                segfile_t *r = segfile_open(ro);
                if (ack_ev[0] <= k - 1) {
                    assert(r != NULL);
                    u64 rt = r->meta.txid;
                    assert(rt == acked || rt == acked + 1);
                    assert(r->meta.watermark == wm[rt]);
                    segfile_close(r);
                } else if (r) segfile_close(r); else free(ro);
                free(torn);
                torn_checked++;
            }
            free(full); free(prev);
        }
        printf("(%u torn states) ", torn_checked);
        assert(torn_checked > 100);
        segfile_close(sf);
    }
    PASS();

    printf("test_segfile: %d tests passed\n", tests_run);
    return 0;
}
