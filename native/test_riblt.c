/*
 * test_riblt.c — the anti-entropy core: rateless IBLT over 32-byte symbols
 * (ported from sunder/tests/test_riblt.c; see riblt.h provenance). Exact
 * symmetric differences at d = 0 / 12 / 1000, one coded stream serving two
 * peers (universality), and decoding through 20% loss + window reordering.
 */
#include "riblt.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t passed = 0;
#define TEST(name) do { printf("  %-50s", #name); fflush(stdout); } while (0)
#define PASS() do { printf("PASS\n"); passed++; } while (0)

/* splitmix64: every 64-bit output distinct over the period, so 32-byte
 * symbols are distinct (a low-bit LCG per byte repeated every 64 symbols). */
static uint64_t g_seed = 0x5EED1234ull;
static uint64_t rnd64(void)
{
    uint64_t z = (g_seed += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}
static uint32_t rnd(void) { return (uint32_t)(rnd64() >> 32); }

static void gen(uint8_t* out, uint32_t n, uint32_t tag)
{
    for (uint32_t i = 0; i < n; i++)
        for (uint32_t k = 0; k < RIBLT_WIDTH; k += 8) {
            uint64_t v = rnd64() ^ tag;
            for (uint32_t b = 0; b < 8; b++) out[(size_t)i * RIBLT_WIDTH + k + b] = (uint8_t)(v >> (8 * b));
        }
}

static int cmp_sym(const void* a, const void* b) { return memcmp(a, b, RIBLT_WIDTH); }

static int has(const uint8_t* list, uint32_t n, const uint8_t* sym)
{
    for (uint32_t i = 0; i < n; i++)
        if (memcmp(list + (size_t)i * RIBLT_WIDTH, sym, RIBLT_WIDTH) == 0) return 1;
    return 0;
}

/* Local set B = A minus `drop` symbols plus `add` new ones. Feeds A's
 * stream in order to B's decoder; returns cells consumed. */
static uint32_t reconcile(uint32_t n, uint32_t drop, uint32_t add, uint32_t max_cells,
                          riblt_dec* out_dec, riblt_enc* enc_a, riblt_enc* enc_b,
                          uint8_t** a_syms, uint8_t** b_syms)
{
    uint8_t* A = malloc((size_t)n * RIBLT_WIDTH);
    uint8_t* B = malloc((size_t)(n + add) * RIBLT_WIDTH);
    gen(A, n, 0u);
    memcpy(B, A + (size_t)drop * RIBLT_WIDTH, (size_t)(n - drop) * RIBLT_WIDTH);
    gen(B + (size_t)(n - drop) * RIBLT_WIDTH, add, 0xA5A5A5A5u);
    assert(riblt_enc_init(enc_a, "k", 1, A, n) == 0);
    assert(riblt_enc_init(enc_b, "k", 1, B, n - drop + add) == 0);
    assert(riblt_dec_init(out_dec, enc_b, "k", 1) == 0);
    uint32_t i = 0;
    for (; i < max_cells; i++) {
        int r = riblt_dec_feed(out_dec, i, riblt_enc_cell(enc_a, i));
        assert(r >= 0);
        if (r == 1) { i++; break; }
    }
    *a_syms = A; *b_syms = B;
    return i;
}

static void check_diff(const riblt_dec* d, const uint8_t* A, const uint8_t* B, uint32_t n, uint32_t drop, uint32_t add)
{
    assert(riblt_dec_decoded(d));
    assert(d->remote_only_n == drop);      /* in A (remote), not in B */
    assert(d->local_only_n == add);        /* in B (local), not in A */
    for (uint32_t i = 0; i < drop; i++) assert(has(d->remote_only, d->remote_only_n, A + (size_t)i * RIBLT_WIDTH));
    for (uint32_t i = 0; i < add; i++) assert(has(d->local_only, d->local_only_n, B + (size_t)(n - drop + i) * RIBLT_WIDTH));
}

int main(void)
{
    printf("test_riblt:\n");

    TEST(generator_symbols_are_distinct);
    {
        uint8_t* A = malloc(5000u * RIBLT_WIDTH);
        gen(A, 5000, 0u);
        qsort(A, 5000, RIBLT_WIDTH, cmp_sym);
        for (uint32_t i = 1; i < 5000; i++)
            assert(memcmp(A + (size_t)(i - 1) * RIBLT_WIDTH, A + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH) != 0);
        free(A);
    }
    PASS();

    TEST(identical_sets_decode_on_first_cell);
    {
        riblt_enc a, b; riblt_dec d; uint8_t *A, *B;
        uint32_t cells = reconcile(500, 0, 0, 100, &d, &a, &b, &A, &B);
        assert(cells == 1u);
        check_diff(&d, A, B, 500, 0, 0);
        riblt_dec_free(&d); riblt_enc_free(&a); riblt_enc_free(&b); free(A); free(B);
    }
    PASS();

    TEST(small_difference_exact);
    {
        riblt_enc a, b; riblt_dec d; uint8_t *A, *B;
        uint32_t cells = reconcile(1000, 5, 7, 400, &d, &a, &b, &A, &B);
        check_diff(&d, A, B, 1000, 5, 7);
        printf(" d=12: %u cells (%.2fx)\n", cells, (double)cells / 12.0);
        assert(cells <= 60u);                          /* a few dozen cells, not O(n) */
        riblt_dec_free(&d); riblt_enc_free(&a); riblt_enc_free(&b); free(A); free(B);
    }
    PASS();

    TEST(large_difference_linear_in_d);
    {
        riblt_enc a, b; riblt_dec d; uint8_t *A, *B;
        uint32_t cells = reconcile(5000, 600, 400, 4000, &d, &a, &b, &A, &B);
        check_diff(&d, A, B, 5000, 600, 400);
        printf(" d=1000: %u cells (%.2fx)\n", cells, (double)cells / 1000.0);
        assert(cells <= 2000u);                        /* paper: ~1.35 d for d = 1000 */
        riblt_dec_free(&d); riblt_enc_free(&a); riblt_enc_free(&b); free(A); free(B);
    }
    PASS();

    TEST(universality_one_stream_two_peers);
    {
        uint8_t* A = malloc(800u * RIBLT_WIDTH);
        gen(A, 800, 0u);
        riblt_enc ea; assert(riblt_enc_init(&ea, "k", 1, A, 800) == 0);
        uint8_t* B1 = malloc(800u * RIBLT_WIDTH);
        memcpy(B1, A + 3u * RIBLT_WIDTH, 797u * RIBLT_WIDTH);
        gen(B1 + 797u * RIBLT_WIDTH, 3, 0x11u);
        uint8_t* B2 = malloc(800u * RIBLT_WIDTH);
        memcpy(B2, A, 790u * RIBLT_WIDTH);
        gen(B2 + 790u * RIBLT_WIDTH, 10, 0x22u);
        riblt_enc e1, e2; riblt_dec d1, d2;
        assert(riblt_enc_init(&e1, "k", 1, B1, 800) == 0);
        assert(riblt_enc_init(&e2, "k", 1, B2, 800) == 0);
        assert(riblt_dec_init(&d1, &e1, "k", 1) == 0);
        assert(riblt_dec_init(&d2, &e2, "k", 1) == 0);
        int done1 = 0, done2 = 0;
        for (uint32_t i = 0; i < 400 && !(done1 && done2); i++) {
            const riblt_cell* c = riblt_enc_cell(&ea, i);
            if (!done1) done1 = riblt_dec_feed(&d1, i, c) == 1;
            if (!done2) done2 = riblt_dec_feed(&d2, i, c) == 1;
        }
        assert(done1 && done2);
        assert(d1.remote_only_n == 3u && d1.local_only_n == 3u);
        assert(d2.remote_only_n == 10u && d2.local_only_n == 10u);
        riblt_dec_free(&d1); riblt_dec_free(&d2);
        riblt_enc_free(&e1); riblt_enc_free(&e2); riblt_enc_free(&ea);
        free(A); free(B1); free(B2);
    }
    PASS();

    TEST(loss_and_reorder);
    {
        /* cell 0 is delivered reliably; after it 20% of cells never arrive
         * and the rest arrive shuffled in windows of 8; the window buffer
         * hands them to the decoder in index order, lost cells skipped. */
        uint8_t* A = malloc(2000u * RIBLT_WIDTH);
        gen(A, 2000, 0u);
        uint8_t* B = malloc(2000u * RIBLT_WIDTH);
        memcpy(B, A + 50u * RIBLT_WIDTH, 1950u * RIBLT_WIDTH);
        gen(B + 1950u * RIBLT_WIDTH, 50, 0x77u);
        riblt_enc ea, eb; riblt_dec d;
        assert(riblt_enc_init(&ea, "k", 1, A, 2000) == 0);
        assert(riblt_enc_init(&eb, "k", 1, B, 2000) == 0);
        assert(riblt_dec_init(&d, &eb, "k", 1) == 0);
        int done = riblt_dec_feed(&d, 0, riblt_enc_cell(&ea, 0)) == 1;
        uint32_t base = 1;
        while (!done && base < 4000) {
            uint32_t order[8] = { 0, 1, 2, 3, 4, 5, 6, 7 };
            for (uint32_t k = 7; k > 0; k--) {
                uint32_t j = rnd() % (k + 1);
                uint32_t t = order[k]; order[k] = order[j]; order[j] = t;
            }
            uint32_t arrived[8], na = 0;
            for (uint32_t k = 0; k < 8; k++) {
                if (rnd() % 5 == 0) continue;
                arrived[na++] = base + order[k];
            }
            for (uint32_t x = 1; x < na; x++) {            /* window buffer */
                uint32_t v = arrived[x], y = x;
                while (y > 0 && arrived[y - 1] > v) { arrived[y] = arrived[y - 1]; y--; }
                arrived[y] = v;
            }
            for (uint32_t k = 0; k < na && !done; k++)
                done = riblt_dec_feed(&d, arrived[k], riblt_enc_cell(&ea, arrived[k])) == 1;
            base += 8;
        }
        assert(done);
        printf(" d=100 with 20%% loss: %u cells fed (%.2fx)\n", d.fed_n, (double)d.fed_n / 100.0);
        assert(d.remote_only_n == 50u && d.local_only_n == 50u);
        for (uint32_t i = 0; i < 50; i++)
            assert(has(d.remote_only, d.remote_only_n, A + (size_t)i * RIBLT_WIDTH));
        riblt_dec_free(&d); riblt_enc_free(&ea); riblt_enc_free(&eb); free(A); free(B);
    }
    PASS();

    printf("test_riblt: %u tests passed\n", passed);
    return 0;
}
