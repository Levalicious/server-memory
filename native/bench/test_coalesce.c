/*
 * Correctness test for the radix allocator's coalescing.
 * Discriminates "real fragmentation" from "broken coalesce":
 *   T1: alloc N contiguous, free all, coalesce  -> must collapse to ONE block.
 *   T2: free alternating -> no two adjacent -> stays ~N/2; free rest -> collapses to one.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "substrate.h"
#include "mf_radix.h"

static int fails = 0;
#define CHECK(c, msg) do { if (!(c)) { printf("  FAIL: %s\n", msg); fails++; } \
                           else printf("  ok:   %s\n", msg); } while (0)

int main(void) {
    const char *p = "/tmp/mf_coal_test.dat";
    unlink(p);
    mf_t *mf = mf_open(p, 1u << 16);
    if (!mf) { perror("mf_open"); return 2; }

    enum { N = 1000 };
    static u64 off[N];

    /* T1: contiguous bump allocs, free all, coalesce -> 1 block */
    for (int i = 0; i < N; i++) off[i] = mfr_alloc(mf, 56);
    for (int i = 0; i < N; i++) mfr_free(mf, off[i], 56);
    u64 before = mfr_freelist_len(mf);
    mfr_coalesce(mf);
    u64 after = mfr_freelist_len(mf);
    printf("T1 free-all: before=%llu after=%llu\n",
           (unsigned long long)before, (unsigned long long)after);
    CHECK(before == N, "T1 freelist length == N before coalesce");
    CHECK(after == 1, "T1 coalesces fully to a single block");

    /* T2: alternating frees can't merge; then free the rest -> merges */
    for (int i = 0; i < N; i++) off[i] = mfr_alloc(mf, 56);
    for (int i = 0; i < N; i += 2) mfr_free(mf, off[i], 56);
    mfr_coalesce(mf);
    u64 alt = mfr_freelist_len(mf);
    printf("T2 alternating: after=%llu (expect ~%d)\n", (unsigned long long)alt, N / 2);
    CHECK(alt >= (u64)(N / 2 - 1) && alt <= (u64)(N / 2 + 1), "T2 alternating frees do not merge");
    for (int i = 1; i < N; i += 2) mfr_free(mf, off[i], 56);
    mfr_coalesce(mf);
    u64 all = mfr_freelist_len(mf);
    printf("T2 then free rest: after=%llu\n", (unsigned long long)all);
    CHECK(all == 1, "T2 fully merges after all freed");

    mf_close(mf);
    printf(fails ? "\nFAILED (%d)\n" : "\nALL PASS\n", fails);
    return fails ? 1 : 0;
}
