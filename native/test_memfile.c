/*
 * v3 memoryfile.c validation: fuzz with overlap detection + version-refuse guard.
 * Standalone (no N-API). Run under ASan+UBSan.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "memoryfile.h"

static u64 g = 0x123456789abcdefull;
static u64 xs(void) { u64 x = g; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return g = x; }
static u64 r32(u64 s) { s = (s + 31) & ~(u64)31; return s < 32 ? 32 : s; }

static int fails = 0;
#define CHECK(c, m) do { if (!(c)) { printf("  FAIL: %s\n", m); fails++; } else printf("  ok:   %s\n", m); } while (0)

int main(void) {
    const char *p = "/tmp/memfile_v3_test.dat";
    unlink(p);
    memfile_t *mf = memfile_open(p, 1u << 16);
    if (!mf) { printf("open failed\n"); return 2; }

    typedef struct { u64 o, s, end; } L;
    size_t cap = 8192;
    L *live = malloc(cap * sizeof(L));
    size_t n = 0, overlaps = 0;

    for (size_t i = 0; i < 300000; i++) {
        int a = (n == 0) || (n < cap && (xs() & 1));
        if (a) {
            u64 s = 32 + 32 * (xs() % 16);
            u64 o = memfile_alloc(mf, s);
            if (!o) continue;
            u64 end = o + r32(s);
            for (size_t k = 0; k < n; k++) if (o < live[k].end && live[k].o < end) { overlaps++; break; }
            live[n].o = o; live[n].s = s; live[n].end = end; n++;
        } else {
            size_t k = xs() % n;
            L x = live[k]; live[k] = live[--n];
            memfile_free(mf, x.o, x.s);
        }
    }
    CHECK(overlaps == 0, "no overlapping live allocations across 300k ops");
    printf("  fuzz: live=%zu free_count=%llu free_bytes=%llu allocated=%llu file=%llu\n",
           n, (unsigned long long)mf->header->free_count, (unsigned long long)mf->header->free_bytes,
           (unsigned long long)mf->header->allocated, (unsigned long long)mf->header->file_size);

    for (size_t k = 0; k < n; k++) memfile_free(mf, live[k].o, live[k].s);
    printf("  after free-all: free_count=%llu free_bytes=%llu\n",
           (unsigned long long)mf->header->free_count, (unsigned long long)mf->header->free_bytes);
    CHECK(mf->header->free_count <= 2, "free-all collapses to <=2 blocks");
    memfile_close(mf);
    free(mf);
    free(live);

    /* version-refuse guard: a non-v3 file must be rejected, never opened. */
    const char *p2 = "/tmp/memfile_v3_badver.dat";
    unlink(p2);
    memfile_t *m2 = memfile_open(p2, 1u << 16);
    CHECK(m2 != NULL, "create fresh v3 file");
    if (m2) { m2->header->version = 1; memfile_sync(m2); memfile_close(m2); free(m2); }
    memfile_t *m3 = memfile_open(p2, 1u << 16);
    CHECK(m3 == NULL, "open refuses version != 3 (protects live v1 KB)");
    if (m3) memfile_close(m3);

    printf(fails ? "\nFAILED (%d)\n" : "\nALL PASS\n", fails);
    return fails ? 1 : 0;
}
