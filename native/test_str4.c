/*
 * test_str4.c — st4 string layer: units + fuzz-vs-model with reopen cycles.
 *
 * Model = flat array of (bytes, refcount). Invariants after every op:
 * intern dedups (same bytes -> same sid), refcounts match, dead sids read
 * NULL, reopen rebuilds the intern map exactly (intern-after-reopen finds,
 * never duplicates).
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

static u64 rng_state = 0x53545234ull;
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

int main(void) {
    printf("test_str4:\n");

    TEST(intern_dedup_get_roundtrip);
    {
        segstore_t *seg = segstore_create(seg_io_sim_open(), 0, 2);
        st4_t *st = st4_open(seg);
        assert(st && st4_count(st) == 0);
        assert(seg_txn_begin(seg));
        u32 a = st4_intern(st, (const u8 *)"NetworkNotNotepad", 17);
        assert(a != 0 && st4_refcount(st, a) == 1);
        u32 b = st4_intern(st, (const u8 *)"NetworkNotNotepad", 17);
        assert(b == a && st4_refcount(st, a) == 2);       /* dedup + ref bump */
        u32 c = st4_intern(st, (const u8 *)"Melting", 7);
        assert(c != 0 && c != a);
        u16 len = 0;
        const u8 *bs = st4_get(st, a, &len);
        assert(bs && len == 17 && memcmp(bs, "NetworkNotNotepad", 17) == 0);
        assert(st4_find(st, (const u8 *)"Melting", 7) == c);
        assert(st4_find(st, (const u8 *)"Absent", 6) == 0);
        assert(st4_count(st) == 2);
        assert(seg_txn_commit(seg));
        /* still there after commit */
        assert(st4_refcount(st, a) == 2 && st4_refcount(st, c) == 1);
        st4_close(st); segstore_close(seg);
    }
    PASS();

    TEST(decref_deletes_slot_reuse_is_safe);
    {
        segstore_t *seg = segstore_create(seg_io_sim_open(), 0, 2);
        st4_t *st = st4_open(seg);
        assert(seg_txn_begin(seg));
        u32 a = st4_intern(st, (const u8 *)"doomed", 6);
        u32 b = st4_intern(st, (const u8 *)"survivor", 8);
        assert(a && b);
        assert(st4_decref(st, a) == 1);                   /* 1 -> 0: deleted */
        assert(st4_get(st, a, NULL) == NULL);
        assert(st4_refcount(st, a) == 0);
        assert(st4_decref(st, a) == 0);                   /* double decref refused */
        assert(st4_find(st, (const u8 *)"doomed", 6) == 0);
        /* new intern may reuse the dead slot; must be a fresh identity */
        u32 c = st4_intern(st, (const u8 *)"newcomer", 8);
        assert(c != 0);
        u16 len = 0;
        const u8 *bs = st4_get(st, c, &len);
        assert(bs && len == 8 && memcmp(bs, "newcomer", 8) == 0);
        assert(st4_get(st, b, &len) && len == 8);         /* survivor intact */
        assert(seg_txn_commit(seg));
        st4_close(st); segstore_close(seg);
    }
    PASS();

    TEST(reopen_rebuilds_intern_map);
    {
        seg_io_t *io = seg_io_sim_open();
        segstore_t *seg = segstore_create(io, 0, 2);
        st4_t *st = st4_open(seg);
        assert(seg_txn_begin(seg));
        u32 a = st4_intern(st, (const u8 *)"persist_me", 10);
        u32 d = st4_intern(st, (const u8 *)"delete_me", 9);
        assert(a && d);
        assert(st4_decref(st, d) == 1);
        assert(seg_txn_commit(seg));
        u64 size; const u8 *b = io->read_base(io, &size);
        u8 *copy = (u8 *)malloc(size); memcpy(copy, b, size);
        st4_close(st); segstore_close(seg);

        seg_io_t *io2 = seg_io_sim_open();          /* writable: reopen then mutate */
        io2->extend(io2, size);
        io2->write(io2, copy, size, 0);
        segstore_t *seg2 = segstore_open(io2);
        assert(seg2);
        st4_t *st2 = st4_open(seg2);
        assert(st2 && st4_count(st2) == 1);               /* deleted one is gone */
        assert(st4_find(st2, (const u8 *)"persist_me", 10) == a);   /* SAME sid */
        assert(st4_find(st2, (const u8 *)"delete_me", 9) == 0);
        assert(st4_refcount(st2, a) == 1);
        /* intern-after-reopen must dedup against rebuilt map */
        assert(seg_txn_begin(seg2));
        assert(st4_intern(st2, (const u8 *)"persist_me", 10) == a);
        assert(st4_refcount(st2, a) == 2);
        assert(seg_txn_commit(seg2));
        st4_close(st2); segstore_close(seg2);
        free(copy);
    }
    PASS();

    TEST(page_spill_and_size_bounds);
    {
        segstore_t *seg = segstore_create(seg_io_sim_open(), 0, 2);
        st4_t *st = st4_open(seg);
        assert(seg_txn_begin(seg));
        u8 buf[SEG_PAGE_MAX_REC];
        /* 2000 distinct strings force multiple pages */
        u32 sids[2000];
        for (u32 i = 0; i < 2000; i++) {
            int n = snprintf((char *)buf, sizeof buf, "entity_%u_padpadpad", i);
            sids[i] = st4_intern(st, buf, (u16)n);
            assert(sids[i] != 0);
        }
        assert(st4_count(st) == 2000);
        assert(seg_txn_commit(seg));                /* logical_pages is COMMITTED state */
        assert(segstore_logical_pages(seg) > 5);    /* >10 at 4K pages; still multi-page at 8K */
        assert(seg_txn_begin(seg));
        /* max-size string fits; oversize refused */
        memset(buf, 'x', sizeof buf);
        u32 big = st4_intern(st, buf, SEG_PAGE_MAX_REC - 4);
        assert(big != 0);
        assert(st4_intern(st, buf, SEG_PAGE_MAX_REC - 3) == 0);
        assert(st4_intern(st, buf, 0) == 0);
        u16 len = 0;
        assert(st4_get(st, big, &len) && len == SEG_PAGE_MAX_REC - 4);
        /* spot-check identity across spill */
        for (u32 i = 0; i < 2000; i += 97) {
            int n = snprintf((char *)buf, sizeof buf, "entity_%u_padpadpad", i);
            assert(st4_find(st, buf, (u16)n) == sids[i]);
        }
        assert(seg_txn_commit(seg));
        st4_close(st); segstore_close(seg);
    }
    PASS();

    TEST(fuzz_vs_model_10k_ops_with_reopens);
    {
        enum { NSTR = 400, OPS = 10000 };
        static u8  mbytes[NSTR][40]; static u16 mlen[NSTR];
        static u32 mref[NSTR]; static u32 msid[NSTR];
        for (u32 i = 0; i < NSTR; i++) {
            mlen[i] = (u16)(5 + rng() % 30);
            for (u16 j = 0; j < mlen[i]; j++) mbytes[i][j] = (u8)(33 + (rng() % 90));
            mbytes[i][0] = (u8)('A' + i % 26);            /* reduce collisions w/ prefix */
            mref[i] = 0; msid[i] = 0;
        }
        seg_io_t *io = seg_io_sim_open();
        segstore_t *seg = segstore_create(io, 0, 2);
        st4_t *st = st4_open(seg);
        assert(seg_txn_begin(seg));
        u32 commits = 0, reopens = 0;
        for (u32 op = 0; op < OPS; op++) {
            u32 i = (u32)(rng() % NSTR);
            u32 kind = (u32)(rng() % 100);
            if (kind < 45) {                               /* intern */
                u32 sid = st4_intern(st, mbytes[i], mlen[i]);
                assert(sid != 0);
                if (mref[i]) assert(sid == msid[i]);
                msid[i] = sid; mref[i]++;
            } else if (kind < 80) {                        /* decref */
                /* dead entry -> sid 0 (guaranteed invalid; sid 1 may be a
                 * live string belonging to another model index!) */
                int ok = st4_decref(st, msid[i]);
                if (mref[i] == 0) assert(ok == 0);
                else { assert(ok == 1); mref[i]--; if (!mref[i]) msid[i] = 0; }
            } else if (kind < 92) {                        /* get + verify */
                if (mref[i]) {
                    u16 len = 0;
                    const u8 *b = st4_get(st, msid[i], &len);
                    assert(b && len == mlen[i] && memcmp(b, mbytes[i], len) == 0);
                    assert(st4_refcount(st, msid[i]) == mref[i]);
                }
            } else if (kind < 97) {                        /* commit + new txn */
                assert(seg_txn_commit(seg));
                assert(seg_txn_begin(seg));
                commits++;
            } else {                                       /* commit + reopen */
                assert(seg_txn_commit(seg));
                u64 size; const u8 *b = io->read_base(io, &size);
                u8 *copy = (u8 *)malloc(size); memcpy(copy, b, size);
                st4_close(st); segstore_close(seg);
                io = seg_io_sim_open();                    /* rebuild rw store from image */
                /* replay image into fresh sim io: extend+write */
                io->extend(io, size);
                io->write(io, copy, size, 0);
                free(copy);
                seg = segstore_open(io);
                assert(seg);
                st = st4_open(seg);
                assert(st);
                for (u32 k = 0; k < NSTR; k++)             /* sids stable across reopen */
                    if (mref[k]) assert(st4_find(st, mbytes[k], mlen[k]) == msid[k]);
                assert(seg_txn_begin(seg));
                reopens++;
            }
        }
        assert(seg_txn_commit(seg));
        u32 live = 0;
        for (u32 k = 0; k < NSTR; k++) if (mref[k]) live++;
        assert(st4_count(st) == live);
        printf("(commits %u reopens %u live %u) ", commits, reopens, live);
        st4_close(st); segstore_close(seg);
    }
    PASS();

    printf("test_str4: %d tests passed\n", tests_run);
    return 0;
}
