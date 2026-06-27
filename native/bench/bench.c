/*
 * Allocator microbench. rdtsc per-op, reports cycle distribution + fragmentation.
 * Build per-variant (inline, no vtable): -DALLOC_RADIX or -DALLOC_CART.
 *
 *   ./bench_X [ops] [seed] [extern_coalesce_interval] [radix_threshold]
 *     extern_coalesce_interval : call coalesce every N ops at harness level (0 = off; default 65536)
 *     radix_threshold          : radix self-coalesces when free_count exceeds this (0 = off; radix only)
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>

#include "substrate.h"

#if defined(ALLOC_RADIX)
#  include "mf_radix.h"
#  define A_ALLOC      mfr_alloc
#  define A_FREE       mfr_free
#  define A_COALESCE   mfr_coalesce
#  define A_FREELEN    mfr_freelist_len
#  define A_FREEBYTES  mfr_free_bytes
#  define A_NAME       "radix"
#  define A_SET_THRESHOLD(t)  (mfr_coalesce_threshold = (u64)(t))
#  define A_COALESCE_CALLS    (mfr_coalesce_calls)
#elif defined(ALLOC_CART)
#  include "mf_cart.h"
#  define A_ALLOC      mfc_alloc
#  define A_FREE       mfc_free
#  define A_COALESCE   mfc_coalesce
#  define A_FREELEN    mfc_freelist_len
#  define A_FREEBYTES  mfc_free_bytes
#  define A_NAME       "cart"
#  define A_SET_THRESHOLD(t)  ((void)(t))
#  define A_COALESCE_CALLS    ((u64)0)
#else
#  error "define an allocator variant (-DALLOC_RADIX or -DALLOC_CART)"
#endif

static inline u64 tsc_begin(void) {
    unsigned a, d;
    __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d));
    return ((u64)d << 32) | a;
}
static inline u64 tsc_end(void) {
    unsigned a, d;
    __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx");
    __asm__ __volatile__("lfence" ::: "memory");
    return ((u64)d << 32) | a;
}

static u64 g_rng;
static inline u64 xs(void) { u64 x = g_rng; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return g_rng = x; }

/* record-size mix: ~entity (56B), ~edge cell (24B), strings 16..512B */
static u64 pick_size(void) {
    u64 r = xs() % 100;
    if (r < 50) return 56;
    if (r < 80) return 24;
    return 16 + 16 * (xs() % 32);
}

static int cmp_u64(const void *a, const void *b) {
    u64 x = *(const u64 *)a, y = *(const u64 *)b;
    return (x > y) - (x < y);
}
static void report(const char *name, u64 *v, size_t n) {
    if (!n) { printf("  %-9s (none)\n", name); return; }
    qsort(v, n, sizeof(u64), cmp_u64);
    size_t p99 = (size_t)(n * 0.99);
    if (p99 >= n) p99 = n - 1;
    double mean = 0;
    for (size_t i = 0; i < n; i++) mean += (double)v[i];
    mean /= (double)n;
    printf("  %-9s n=%-9zu min=%-5llu med=%-5llu mean=%-8.1f p99=%-7llu max=%llu\n",
           name, n, (unsigned long long)v[0], (unsigned long long)v[n / 2],
           mean, (unsigned long long)v[p99], (unsigned long long)v[n - 1]);
}

int main(int argc, char **argv) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set);
    sched_setaffinity(0, sizeof(set), &set);

    size_t ops     = (argc > 1) ? strtoul(argv[1], NULL, 10) : 2000000;
    g_rng          = (argc > 2) ? strtoull(argv[2], NULL, 10) : 0x9e3779b97f4a7c15ull;
    size_t coal_iv = (argc > 3) ? strtoul(argv[3], NULL, 10) : 65536;
    u64 threshold  = (argc > 4) ? strtoull(argv[4], NULL, 10) : 0;
    A_SET_THRESHOLD(threshold);

    const char *path = "/tmp/mfbench.dat";
    unlink(path);
    mf_t *mf = mf_open(path, 1u << 20);
    if (!mf) { perror("mf_open"); return 1; }

    typedef struct { u64 off, size; } live_t;
    size_t cap = 1u << 20;
    live_t *live = malloc(cap * sizeof(*live));
    size_t nlive = 0;
    u64 *at = malloc(ops * sizeof(u64)); size_t na = 0;
    u64 *ft = malloc(ops * sizeof(u64)); size_t nf = 0;
    u64 coal_cycles = 0; size_t coal_n = 0;
    u64 fail = 0;

    for (size_t i = 0; i < ops; i++) {
        int do_alloc = (nlive == 0) || (nlive < cap && (xs() & 1));
        if (do_alloc) {
            u64 sz = pick_size();
            u64 t0 = tsc_begin();
            u64 off = A_ALLOC(mf, sz);
            u64 t1 = tsc_end();
            if (off) { at[na++] = t1 - t0; live[nlive].off = off; live[nlive].size = sz; nlive++; }
            else fail++;
        } else {
            size_t k = xs() % nlive;
            live_t L = live[k];
            live[k] = live[--nlive];
            u64 t0 = tsc_begin();
            A_FREE(mf, L.off, L.size);
            u64 t1 = tsc_end();
            ft[nf++] = t1 - t0;
        }
        if (coal_iv && i && (i % coal_iv) == 0) {
            u64 c0 = tsc_begin();
            A_COALESCE(mf);
            u64 c1 = tsc_end();
            coal_cycles += c1 - c0; coal_n++;
        }
    }

    u64 used = mf->hdr->allocated, freeb = A_FREEBYTES(mf), filesz = mf->hdr->file_size;
    printf("alloc=%s ops=%zu seed=0x%llx fail=%llu coal_iv=%zu threshold=%llu internal_coal=%llu\n",
           A_NAME, ops, (unsigned long long)g_rng, (unsigned long long)fail,
           coal_iv, (unsigned long long)threshold, (unsigned long long)A_COALESCE_CALLS);
    printf("state: live=%zu freelist=%llu free_bytes=%llu allocated=%llu file=%llu  frag=%.3f\n",
           nlive, (unsigned long long)A_FREELEN(mf), (unsigned long long)freeb,
           (unsigned long long)used, (unsigned long long)filesz,
           used ? (double)freeb / (double)used : 0.0);
    printf("cycles/op:\n");
    report("alloc", at, na);
    report("free", ft, nf);
    if (coal_n) printf("  coalesce  n=%zu mean=%.1f\n", coal_n, (double)coal_cycles / (double)coal_n);

    mf_close(mf);
    free(live); free(at); free(ft);
    return 0;
}
