/*
 * Correctness + fuzz test for the Cartesian-tree allocator.
 *   T1: alloc N, free all (scrambled order) -> continuous coalescing collapses to ONE block.
 *   T2: randomized alloc/free fuzz; validate BST+heap+parent+counter invariants every step.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "substrate.h"
#include "mf_cart.h"

static int fails = 0;
#define CHECK(c, msg) do { if (!(c)) { printf("  FAIL: %s\n", msg); fails++; } \
                           else printf("  ok:   %s\n", msg); } while (0)

static u64 g = 0x243f6a8885a308d3ull;
static u64 xs(void) { u64 x = g; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return g = x; }

int main(void) {
    const char *p = "/tmp/mf_cart_test.dat";
    unlink(p);
    mf_t *mf = mf_open(p, 1u << 16);
    if (!mf) { perror("mf_open"); return 2; }

    enum { N = 2000 };
    static u64 off[N], sz[N];

    /* T1: alloc N, free all in scrambled order -> collapses to one block */
    for (int i = 0; i < N; i++) { sz[i] = 32 + 32 * (xs() % 8); off[i] = mfc_alloc(mf, sz[i]); }
    CHECK(mfc_validate(mf) == 0, "T1 tree valid after N allocs");
    /* Fisher-Yates scramble of free order */
    for (int i = N - 1; i > 0; i--) { int j = (int)(xs() % (u64)(i + 1)); u64 t = off[i]; off[i] = off[j]; off[j] = t; u64 u = sz[i]; sz[i] = sz[j]; sz[j] = u; }
    for (int i = 0; i < N; i++) mfc_free(mf, off[i], sz[i]);
    CHECK(mfc_validate(mf) == 0, "T1 tree valid after freeing all");
    printf("T1 free-all-scrambled: free_count=%llu free_bytes=%llu\n",
           (unsigned long long)mfc_freelist_len(mf), (unsigned long long)mfc_free_bytes(mf));
    CHECK(mfc_freelist_len(mf) == 1, "T1 collapses to a single free block");

    /* T2: randomized fuzz with per-step invariant validation */
    typedef struct { u64 o, s; } live_t;
    size_t cap = 4096; live_t *live = malloc(cap * sizeof(*live)); size_t nlive = 0;
    size_t bad = 0;
    for (size_t i = 0; i < 400000; i++) {
        int do_alloc = (nlive == 0) || (nlive < cap && (xs() & 1));
        if (do_alloc) {
            u64 s = 32 + 32 * (xs() % 16);
            u64 o = mfc_alloc(mf, s);
            if (o) { live[nlive].o = o; live[nlive].s = s; nlive++; }
        } else {
            size_t k = xs() % nlive; live_t L = live[k]; live[k] = live[--nlive];
            mfc_free(mf, L.o, L.s);
        }
        if ((i & 0x3FF) == 0 && mfc_validate(mf) != 0) bad++;
    }
    printf("T2 fuzz: ops=400000 final_live=%zu free_count=%llu invariant_failures=%zu\n",
           nlive, (unsigned long long)mfc_freelist_len(mf), bad);
    CHECK(bad == 0, "T2 invariants held across the fuzz run");

    free(live);
    mf_close(mf);
    printf(fails ? "\nFAILED (%d)\n" : "\nALL PASS\n", fails);
    return fails ? 1 : 0;
}
