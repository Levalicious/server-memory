/*
 * StringTable validation: rehash stress + dedup/refcount + model-based fuzz.
 * Standalone, run under ASan+UBSan.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "stringtable.h"

static int fails = 0;
#define CHECK(c, m) do { if (!(c)) { printf("  FAIL: %s\n", m); fails++; } else printf("  ok:   %s\n", m); } while (0)

static u64 g = 0xabcdef0123ull;
static u64 xs(void) { u64 x = g; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return g = x; }

int main(void) {
    const char *p = "/tmp/st_test.dat";
    unlink(p);
    stringtable_t *st = st_open(p, 1u << 16);
    if (!st) { printf("st_open failed\n"); return 2; }

    char buf[32];

    /* T1: rehash stress — 10k distinct interns, all findable with stable ids */
    enum { N = 10000 };
    static u64 ids[N];
    for (int i = 0; i < N; i++) {
        int l = snprintf(buf, sizeof buf, "string-%d", i);
        ids[i] = st_intern(st, (const u8 *)buf, (u16)l);
        if (!ids[i]) { fails++; break; }
    }
    CHECK(st_count(st) == N, "count == N after N distinct interns (forces rehash)");
    int allfind = 1;
    for (int i = 0; i < N; i++) {
        int l = snprintf(buf, sizeof buf, "string-%d", i);
        if (st_find(st, (const u8 *)buf, (u16)l) != ids[i]) { allfind = 0; break; }
    }
    CHECK(allfind, "all N findable with stable ids after rehash");
    {
        int l = snprintf(buf, sizeof buf, "string-%d", 1234);
        u16 gl; const u8 *gd = st_get(st, ids[1234], &gl);
        CHECK(gl == (u16)l && memcmp(gd, buf, l) == 0, "st_get returns the correct bytes");
    }

    /* T2: dedup + refcount lifecycle */
    {
        const char *s = "duplicate-string";
        u16 sl = (u16)strlen(s);
        u64 a = st_intern(st, (const u8 *)s, sl), b = st_intern(st, (const u8 *)s, sl);
        CHECK(a == b, "duplicate interns return the same id");
        CHECK(st_refcount(st, a) == 2, "refcount == 2 after two interns");
        st_release(st, a);
        CHECK(st_refcount(st, a) == 1, "refcount == 1 after one release");
        st_release(st, a);
        CHECK(st_find(st, (const u8 *)s, sl) == 0, "entry freed + unindexed when refcount hits 0");
    }

    /* T3: model-based fuzz — parallel-track expected refcounts, validate every step */
    enum { P = 2000 };
    static u32 rc[P]; static u64 pid[P];
    for (int i = 0; i < P; i++) { rc[i] = 0; pid[i] = 0; }
    size_t bad = 0;
    for (size_t it = 0; it < 400000; it++) {
        int i = (int)(xs() % P);
        int l = snprintf(buf, sizeof buf, "pool-%d", i);
        if (rc[i] == 0 || (xs() & 1)) {            /* intern */
            u64 id = st_intern(st, (const u8 *)buf, (u16)l);
            if (rc[i] > 0 && id != pid[i]) bad++;  /* dedup must return existing id */
            pid[i] = id; rc[i]++;
        } else {                                   /* release */
            st_release(st, pid[i]); rc[i]--;
        }
        if (rc[i] > 0) {
            if (st_refcount(st, pid[i]) != rc[i]) bad++;
            if (st_find(st, (const u8 *)buf, (u16)l) != pid[i]) bad++;
        } else {
            if (st_find(st, (const u8 *)buf, (u16)l) != 0) bad++;
        }
    }
    CHECK(bad == 0, "model-based fuzz: refcounts + finds stay consistent");
    for (int i = 0; i < P; i++) while (rc[i] > 0) { st_release(st, pid[i]); rc[i]--; }
    CHECK(st_count(st) == N, "after releasing the whole pool, only the T1 strings remain");
    printf("  fuzz bad=%zu final_count=%u (expected %d)\n", bad, st_count(st), N);

    st_close(st);
    printf(fails ? "\nFAILED (%d)\n" : "\nALL PASS\n", fails);
    return fails ? 1 : 0;
}
