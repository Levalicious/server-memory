/*
 * test_segtree.c — fuzz-vs-model for seg_tree (the libmdbx-style B+tree),
 * plus persistence across a simulated reopen (roots are carried by the
 * segment root slots, node pages by the store itself).
 *
 * Model: keys are fixed-width so byte order == numeric order; the model is a
 * plain alive[] array. Exercises: shuffled inserts (splits, cascades),
 * deletes (rewrite, sibling merge, empty unlink), full drain (root collapse
 * to 0), re-insert after drain, ordered scan vs model, range scan for a
 * fixed-key posting-style tree, reopen + re-verify.
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

static u64 rng_state = 0x53454754ull;   /* "SEGT" */
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

enum { NA = 20000 };

static void key_a(u32 id, u8 *k, u16 *kl) {          /* "k%05u", fixed 6B */
    snprintf((char *)k, 8, "k%05u", id);
    *kl = 6;
}

/* --- collectors (codec-aware; never rely on NUL termination) --- */
typedef struct { u32 *ids; u32 n, cap; int sorted_ok; u32 last; } collect_t;

static int collect_a(void *ctx, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    collect_t *c = (collect_t *)ctx;
    assert(kl == 6 && vl == 4);
    u32 id = 0;
    for (int i = 1; i < 6; i++) id = id * 10 + (u32)(k[i] - '0');
    if (c->n && id <= c->last) c->sorted_ok = 0;
    c->last = id;
    if (c->ids && c->n < c->cap) c->ids[c->n] = id;
    c->n++;
    return 1;
}

static int collect_b(void *ctx, const u8 *k, u16 kl, const u8 *v, u16 vl) {
    collect_t *c = (collect_t *)ctx;
    assert(kl == 7 && vl == 0);
    u32 seq = ((u32)k[3] << 24) | ((u32)k[4] << 16) | ((u32)k[5] << 8) | (u32)k[6];
    if (c->n && seq <= c->last) c->sorted_ok = 0;
    c->last = seq;
    c->n++;
    return 1;
}

int main(void) {
    printf("test_segtree:\n");

    seg_io_t *io1 = seg_io_sim_open();
    segstore_t *seg = segstore_create(io1, 0, 2);
    assert(seg);
    assert(seg_txn_begin(seg));

    seg_tree_t A;
    seg_tree_open(&A, seg, (st_codec_t){ 0 /*var key*/, 1 /*u32 value*/ }, 0);

    static u8 alive[NA];
    memset(alive, 0, sizeof alive);

    TEST(insert_fuzz_splits);
    {
        u32 order[NA];
        for (u32 i = 0; i < NA; i++) order[i] = i;
        for (u32 i = NA - 1; i > 0; i--) { u32 j = (u32)(rng() % (i + 1)); u32 t = order[i]; order[i] = order[j]; order[j] = t; }
        for (u32 i = 0; i < NA; i++) {
            u8 k[8]; u16 kl; key_a(order[i], k, &kl);
            u32 v = order[i];
            u8 vb[4] = { (u8)v, (u8)(v >> 8), (u8)(v >> 16), (u8)(v >> 24) };
            assert(seg_tree_insert(&A, k, kl, vb, 4) == 1);
            alive[order[i]] = 1;
        }
        assert(seg_tree_count(&A) == NA);
        {
            u8 k[8]; u16 kl; key_a(12345, k, &kl);
            u8 vb[4] = { 1, 2, 3, 4 };
            assert(seg_tree_insert(&A, k, kl, vb, 4) == 0);   /* replace */
            u8 out[4]; u16 ol = 4;
            assert(seg_tree_lookup(&A, k, kl, out, &ol) == 1 && ol == 4 && out[0] == 1);
        }
        for (u32 id = 0; id < NA; id += 371) {
            u8 k[8]; u16 kl; key_a(id, k, &kl);
            u8 out[4]; u16 ol = 4;
            assert(seg_tree_lookup(&A, k, kl, out, &ol) == 1);
            assert((u32)(out[0] | (out[1] << 8) | (out[2] << 16) | ((u32)out[3] << 24)) == id);
        }
        PASS();
    }

    TEST(delete_fuzz_merge_and_unlink);
    {
        u32 deleted = 0;
        for (u32 id = 0; id < NA; id += 3) {          /* dense deletes force merges */
            u8 k[8]; u16 kl; key_a(id, k, &kl);
            assert(seg_tree_delete(&A, k, kl) == 1);
            assert(seg_tree_delete(&A, k, kl) == 0);  /* absent second time */
            alive[id] = 0;
            deleted++;
        }
        assert(seg_tree_count(&A) == NA - deleted);
        for (u32 id = 1; id < NA; id += 3) {
            u8 k[8]; u16 kl; key_a(id, k, &kl);
            u8 out[4]; u16 ol = 4;
            assert(seg_tree_lookup(&A, k, kl, out, &ol) == 1);
        }
        PASS();
    }

    TEST(scan_matches_model);
    {
        u32 *buf = (u32 *)malloc(sizeof(u32) * (NA + 8));
        assert(buf);
        collect_t c = { buf, 0, NA + 8, 1, 0 };
        assert(seg_tree_scan(&A, NULL, 0, 1, NULL, 0, 1, collect_a, &c) == 0);
        assert(c.sorted_ok);
        u32 expect = 0;
        for (u32 id = 0; id < NA; id++) if (alive[id]) expect++;
        assert(c.n == expect);
        u32 x = 0;
        for (u32 id = 0; id < NA; id++) if (alive[id]) { assert(buf[x] == id); x++; }
        /* range slice [k00100, k00200) */
        u8 lo[8], hi[8]; u16 l1, l2;
        key_a(100, lo, &l1); key_a(200, hi, &l2);
        collect_t c2 = { buf, 0, NA + 8, 1, 0 };
        assert(seg_tree_scan(&A, lo, l1, 1, hi, l2, 0, collect_a, &c2) == 0);
        u32 exp2 = 0;
        for (u32 id = 100; id < 200; id++) if (alive[id]) exp2++;
        assert(c2.n == exp2);
        free(buf);
        PASS();
    }

    TEST(posting_tree_fixed_keys_and_prefix_scan);
    {
        seg_tree_t B;
        seg_tree_open(&B, seg, (st_codec_t){ 7 /*fixed key*/, 0 /*posting*/ }, 0);
        enum { NB = 6000 };
        for (u32 i = 0; i < NB; i++) {
            u8 k[7] = { (u8)(i % 6), 0, 0, (u8)(i >> 24), (u8)(i >> 16), (u8)(i >> 8), (u8)i };
            assert(seg_tree_insert(&B, k, 7, NULL, 0) == 1);
            assert(seg_tree_insert(&B, k, 7, NULL, 0) == 0);   /* posting dup = no-op */
        }
        assert(seg_tree_count(&B) == NB);
        for (u32 i = 0; i < NB; i += 4) {
            u8 k[7] = { (u8)(i % 6), 0, 0, (u8)(i >> 24), (u8)(i >> 16), (u8)(i >> 8), (u8)i };
            assert(seg_tree_delete(&B, k, 7) == 1);
        }
        /* prefix scan over pfx=0 (hi = [1,0,...] exclusive) */
        u8 lo[7] = { 0, 0, 0, 0, 0, 0, 0 };
        u8 hi[7] = { 1, 0, 0, 0, 0, 0, 0 };
        collect_t c = { NULL, 0, 0, 1, 0 };
        assert(seg_tree_scan(&B, lo, 7, 1, hi, 7, 0, collect_b, &c) == 0);
        u32 expect = 0;
        for (u32 i = 0; i < NB; i++) if (i % 6 == 0 && i % 4 != 0) expect++;
        assert(c.n == expect && c.sorted_ok);
        /* drain B fully: empty-unlink + root collapse to 0 */
        for (u32 i = 0; i < NB; i++) {
            u8 k[7] = { (u8)(i % 6), 0, 0, (u8)(i >> 24), (u8)(i >> 16), (u8)(i >> 8), (u8)i };
            int r = seg_tree_delete(&B, k, 7);
            assert(r == (i % 4 != 0));
        }
        assert(seg_tree_count(&B) == 0 && seg_tree_root(&B) == 0);
        for (u32 i = 0; i < 100; i++) {          /* re-grow after drain */
            u8 k[7] = { 5, 0, 0, (u8)i, 0, 0, 0 };
            assert(seg_tree_insert(&B, k, 7, NULL, 0) == 1);
        }
        assert(seg_tree_count(&B) == 100);
        PASS();
    }

    seg_txn_set_roots(seg, seg_tree_root(&A), 0);
    assert(seg_txn_commit(seg));

    TEST(reopen_persists_tree_and_root);
    {
        u64 size = 0;
        const u8 *img = io1->read_base(io1, &size);
        assert(img && size);
        u8 *copy = (u8 *)malloc((size_t)size);
        assert(copy);
        memcpy(copy, img, (size_t)size);
        segstore_close(seg);

        seg_io_t *io2 = seg_io_sim_open();
        io2->extend(io2, size);
        assert(io2->write(io2, copy, size, 0));
        free(copy);
        segstore_t *seg2 = segstore_open(io2);
        assert(seg2);
        assert(segstore_nameindex_root(seg2) != 0);

        seg_tree_t A2;
        seg_tree_open(&A2, seg2, (st_codec_t){ 0, 1 }, segstore_nameindex_root(seg2));
        for (u32 id = 0; id < NA; id += 617) {
            u8 k[8]; u16 kl; key_a(id, k, &kl);
            u8 out[4]; u16 ol = 4;
            assert(seg_tree_lookup(&A2, k, kl, out, &ol) == (alive[id] ? 1 : 0));
        }
        u32 post = 0;
        for (u32 id = 0; id < NA; id++) if (alive[id]) post++;
        u32 *buf = (u32 *)malloc(sizeof(u32) * (NA + 8));
        assert(buf);
        collect_t c = { buf, 0, NA + 8, 1, 0 };
        assert(seg_tree_scan(&A2, NULL, 0, 1, NULL, 0, 1, collect_a, &c) == 0);
        assert(c.sorted_ok && c.n == post);
        free(buf);
        segstore_close(seg2);
        PASS();
    }

    printf("test_segtree: %d tests passed\n", tests_run);
    return 0;
}
