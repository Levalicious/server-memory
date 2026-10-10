/*
 * test_repl.c — anti-entropy over real store data (docs/shard-seam-design-
 * note.md §8): extract 32-byte symbols from two stores, reconcile them with
 * the RIBLT, and prove the decoded symmetric difference equals the exact
 * set difference computed in-test (adjacency halves and vertex-state rows).
 *
 * The store pair: A is built and committed; B is a byte-clone, then mutated
 * (creates, deletes, observations, relation add/remove, a delete+recreate
 * name). Symbols are extracted under a consistent txn view from both.
 */
#include "segstore.h"
#include "riblt.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static u32 passed = 0;
#define TEST(name) do { printf("  %-50s", (name)); fflush(stdout); } while (0)
#define PASS() do { printf("PASS\n"); passed++; } while (0)

/* ---- symbol collectors ---- */

typedef struct { u8 *v; u32 n, cap; } symset_t;

static void collect(void *ctx, const u8 *s) {
    symset_t *x = (symset_t *)ctx;
    if (x->n == x->cap) {
        x->cap = x->cap ? x->cap * 2 : 1024;
        x->v = (u8 *)realloc(x->v, (size_t)x->cap * RIBLT_WIDTH);
        assert(x->v);
    }
    memcpy(x->v + (size_t)x->n * RIBLT_WIDTH, s, RIBLT_WIDTH);
    x->n++;
}

static int cmp_sym(const void *a, const void *b) { return memcmp(a, b, RIBLT_WIDTH); }

/* canonical edge symbols: a fully-resident edge emits its symbol twice
 * (once per half) — reconciliation sets are deduped */
static void uniq_syms(symset_t *s) {
    u32 w = 0;
    for (u32 i = 0; i < s->n; i++) {
        if (w == 0 || memcmp(s->v + (size_t)(w - 1) * RIBLT_WIDTH,
                             s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH) != 0)
            memcpy(s->v + (size_t)w++ * RIBLT_WIDTH, s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH);
    }
    s->n = w;
}

/* exact symmetric difference of two sorted sets into out_two (A\B then B\A) */
static u32 set_diff(const u8 *A, u32 na, const u8 *B, u32 nb, u8 *out, u32 out_cap)
{
    u32 i = 0, j = 0, w = 0;
    while (i < na || j < nb) {
        int c = (i < na && j < nb) ? memcmp(A + (size_t)i * RIBLT_WIDTH, B + (size_t)j * RIBLT_WIDTH, RIBLT_WIDTH)
              : (i < na ? -1 : 1);
        if (c < 0) { memcpy(out + (size_t)w++ * RIBLT_WIDTH, A + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH); i++; }
        else if (c > 0) { memcpy(out + (size_t)w++ * RIBLT_WIDTH, B + (size_t)j * RIBLT_WIDTH, RIBLT_WIDTH); j++; }
        else { i++; j++; }
        assert(w <= out_cap);
    }
    return w;
}

/* ---- store helpers ---- */

static void copy_file(const char *src, const char *dst) {
    FILE *fi = fopen(src, "rb"); assert(fi);
    FILE *fo = fopen(dst, "wb"); assert(fo);
    char buf[65536]; size_t r;
    while ((r = fread(buf, 1, sizeof buf, fi)) > 0) assert(fwrite(buf, 1, r, fo) == r);
    fclose(fi); fclose(fo);
}

static mstore_t *open_store(const char *mp, const char *gp, const char *sp, int create) {
    seg_io_t *sios[2] = { seg_io_posix_open(gp, create), seg_io_posix_open(sp, create) };
    return create ? mstore_create(seg_io_posix_open(mp, 1), sios, 2, 2)
                  : mstore_open(seg_io_posix_open(mp, 0), sios, 2);
}

int main(void)
{
    printf("test_repl:\n");
    assert(RIBLT_WIDTH == G4_SYM_LEN);   /* the emitters and the RIBLT agree */

    char dirA[] = "/tmp/g4repl_A_XXXXXX", dirB[] = "/tmp/g4repl_B_XXXXXX";
    assert(mkdtemp(dirA) && mkdtemp(dirB));
    char mpa[256], gpa[256], spa[256], mpb[256], gpb[256], spb[256];
    snprintf(mpa, sizeof mpa, "%s/manifest.kb", dirA);
    snprintf(gpa, sizeof gpa, "%s/graph.kb", dirA);
    snprintf(spa, sizeof spa, "%s/strings.kb", dirA);
    snprintf(mpb, sizeof mpb, "%s/manifest.kb", dirB);
    snprintf(gpb, sizeof gpb, "%s/graph.kb", dirB);
    snprintf(spb, sizeof spb, "%s/strings.kb", dirB);

    /* ---- build A ---- */
    {
        mstore_t *ms = open_store(mpa, gpa, spa, 1);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32], ty[16];
        u32 ids[1200];
        for (u32 i = 0; i < 1200; i++) {
            int n = snprintf(nm, sizeof nm, "RA_%04u", i);
            int t = snprintf(ty, sizeof ty, "T%u", i % 7u);
            ids[i] = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)ty, (u16)t, 1000 + i);
            assert(ids[i]);
            if (i < 200) assert(g4_add_observation(g, ids[i], (const u8 *)"memo text here", 14, 2000 + i));
        }
        for (u32 i = 0; i < 1000; i++) {
            char rt[16];
            int r = snprintf(rt, sizeof rt, "rt%u", i % 5u);
            assert(g4_create_relation(g, ids[i], ids[(i * 7u + 3u) % 1200u], (const u8 *)rt, (u16)r, 3000 + i));
        }
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }

    /* ---- clone to B and mutate ---- */
    copy_file(mpa, mpb); copy_file(gpa, gpb); copy_file(spa, spb);
    {
        mstore_t *ms = open_store(mpb, gpb, spb, 0);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32], ty[16];
        u32 fresh[12];
        for (u32 i = 0; i < 12; i++) {                        /* creates */
            int n = snprintf(nm, sizeof nm, "RB_%04u", i);
            int t = snprintf(ty, sizeof ty, "T%u", i % 7u);
            fresh[i] = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)ty, (u16)t, 4000 + i);
            assert(fresh[i]);
        }
        for (u32 i = 0; i < 7; i++) {                         /* deletes (incident adj removed) */
            int n = snprintf(nm, sizeof nm, "RA_%04u", 100u + 13u * i);
            u32 id = g4_lookup(g, (const u8 *)nm, (u16)n);
            assert(id && g4_delete_entity(g, id));
        }
        for (u32 i = 0; i < 5; i++) {                         /* observations */
            int n = snprintf(nm, sizeof nm, "RA_%04u", 700u + i);
            u32 id = g4_lookup(g, (const u8 *)nm, (u16)n);
            assert(g4_add_observation(g, id, (const u8 *)"new note", 8, 5000 + i));
        }
        for (u32 i = 0; i < 6; i++) {                         /* relation adds */
            int n1 = snprintf(nm, sizeof nm, "RA_%04u", 900u + i);
            u32 a = g4_lookup(g, (const u8 *)nm, (u16)n1);
            assert(g4_create_relation(g, a, fresh[i], (const u8 *)"rb", 2, 6000 + i));
        }
        for (u32 i = 0; i < 4; i++) {                         /* relation removes */
            char nm2[32], rt[16];
            int n1 = snprintf(nm, sizeof nm, "RA_%04u", 800u + i);
            int rl = snprintf(rt, sizeof rt, "rt%u", (800u + i) % 5u);
            u32 a = g4_lookup(g, (const u8 *)nm, (u16)n1);
            u32 t = (u32)(((800u + i) * 7u + 3u) % 1200u);
            int n2 = snprintf(nm2, sizeof nm2, "RA_%04u", t);
            u32 b = g4_lookup(g, (const u8 *)nm2, (u16)n2);
            if (b) assert(g4_delete_relation(g, a, b, (const u8 *)rt, (u16)rl));
        }
        {   /* delete + recreate: binding generation advances */
            u32 id = g4_lookup(g, (const u8 *)"RA_0005", 7);
            assert(id && g4_delete_entity(g, id));
            assert(g4_create_entity(g, (const u8 *)"RA_0005", 7, (const u8 *)"T9", 2, 7000));
        }
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }

    /* ---- extract symbols from both ---- */
    static symset_t adjA, adjB, vsA, vsB;
    {
        mstore_t *ma = open_store(mpa, gpa, spa, 0);
        mstore_t *mb = open_store(mpb, gpb, spb, 0);
        assert(ma && mb);
        graph4_t *ga = graph4_open(ma);
        graph4_t *gb = graph4_open(mb);
        assert(ga && gb);
        assert(mstore_txn_begin(ma));
        assert(mstore_txn_begin(mb));
        u32 na = g4_adj_symbols(ga, collect, &adjA);
        u32 nb = g4_adj_symbols(gb, collect, &adjB);
        printf(" adj: A=%u B=%u symbols\n", na, nb);
        assert(na == adjA.n && nb == adjB.n);
        assert(na == 2000u);                                  /* both halves emitted raw */
        qsort(adjA.v, adjA.n, RIBLT_WIDTH, cmp_sym);
        qsort(adjB.v, adjB.n, RIBLT_WIDTH, cmp_sym);
        uniq_syms(&adjA); uniq_syms(&adjB);                   /* canonical: pairs collapse */
        assert(adjA.n == 1000u);                              /* 1000 distinct edges */
        assert(adjB.n > 985u && adjB.n < 1005u);              /* -incident + added */
        u32 va = g4_vstate_symbols(ga, collect, &vsA);
        u32 vb = g4_vstate_symbols(gb, collect, &vsB);
        printf(" vstate: A=%u B=%u symbols\n", va, vb);
        assert(va == vsA.n && vb == vsB.n);
        assert(va == 1200u && vb == 1200u - 7u + 12u);        /* live bindings */
        qsort(vsA.v, vsA.n, RIBLT_WIDTH, cmp_sym);
        qsort(vsB.v, vsB.n, RIBLT_WIDTH, cmp_sym);
        uniq_syms(&vsA); uniq_syms(&vsB);
        assert(mstore_txn_commit(ma));                        /* read-only txn: no-op commit */
        assert(mstore_txn_commit(mb));
        graph4_close(ga); graph4_close(gb);
        mstore_close(ma); mstore_close(mb);
    }

    /* ---- ground truth vs RIBLT, for both symbol families ---- */
    struct { const char *name; symset_t *A, *B; } fams[2] = {
        { "adjacency", &adjA, &adjB },
        { "vertex-state", &vsA, &vsB },
    };
    for (int f = 0; f < 2; f++) {
        symset_t *A = fams[f].A, *B = fams[f].B;
        u8 *truth = (u8 *)malloc((size_t)(A->n + B->n) * RIBLT_WIDTH);
        u32 td = set_diff(A->v, A->n, B->v, B->n, truth, A->n + B->n);
        assert(td > 0);

        printf("  reconcile_matches_ground_truth (%-12s)   ", fams[f].name);
        riblt_enc ea, eb;
        riblt_dec d;
        assert(riblt_enc_init(&ea, "kb", 2, A->v, A->n) == 0);
        assert(riblt_enc_init(&eb, "kb", 2, B->v, B->n) == 0);
        assert(riblt_dec_init(&d, &eb, "kb", 2) == 0);
        int done = 0;
        for (u32 i = 0; i < 4000 && !done; i++)
            done = riblt_dec_feed(&d, i, riblt_enc_cell(&ea, i)) == 1;
        assert(done);
        assert(d.remote_only_n + d.local_only_n == td);
        u8 *rl = (u8 *)malloc((size_t)(d.remote_only_n + d.local_only_n) * RIBLT_WIDTH);
        memcpy(rl, d.remote_only, (size_t)d.remote_only_n * RIBLT_WIDTH);
        memcpy(rl + (size_t)d.remote_only_n * RIBLT_WIDTH, d.local_only,
               (size_t)d.local_only_n * RIBLT_WIDTH);
        qsort(rl, d.remote_only_n + d.local_only_n, RIBLT_WIDTH, cmp_sym);
        u8 *st = (u8 *)malloc((size_t)td * RIBLT_WIDTH);
        memcpy(st, truth, (size_t)td * RIBLT_WIDTH);
        qsort(st, td, RIBLT_WIDTH, cmp_sym);
        assert(memcmp(rl, st, (size_t)td * RIBLT_WIDTH) == 0);   /* exact set equality */
        printf("diff=%u\n", td);
        free(rl); free(st); free(truth);
        riblt_dec_free(&d); riblt_enc_free(&ea); riblt_enc_free(&eb);
        passed++;
    }

    /* cleanup */
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf %s %s", dirA, dirB);
    assert(system(cmd) == 0);
    printf("test_repl: %u tests passed\n", passed);
    return 0;
}
