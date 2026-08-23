/*
 * bench_segstore.c — Q1 evidence + txn-layer cost profile.
 *
 * Built twice (4K default, -DSEG_PAGE_SIZE=16384u) by `make bench_segstore`.
 *
 * Workload = synthetic melt: a segment prefilled with entity-like records
 * (76B), then batches touching K scattered logical pages, inserting ~2 small
 * records each, one commit per batch — the real KB write shape.
 *
 * Reported per K:
 *   cyc/commit (p50)      rdtsc around begin..commit (sim io: no device time)
 *   bytes/commit          sim io write accounting (data+ptable+root+freelist)
 *   amp                   bytes written / logical bytes changed
 * Plus: recover (segstore_open) p50, and a posix variant (real fdatasync)
 * for absolute commit latency on /tmp.
 */
#include "segstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static u64 rdtsc_s(void) {
    unsigned a, d; __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d)); return ((u64)d << 32) | a;
}
static u64 rdtsc_e(void) {
    unsigned a, d; __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx"); return ((u64)d << 32) | a;
}

static u64 rng_state = 0x42656E6368ull;
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

static int cmp_u64(const void *a, const void *b) {
    u64 x = *(const u64 *)a, y = *(const u64 *)b;
    return x < y ? -1 : x > y;
}
static u64 med(u64 *v, u32 n) { qsort(v, n, 8, cmp_u64); return v[n / 2]; }

/* Scale ladder per scripts/bench-scale.sh conventions (KB Bench_AllocatorHarness):
 * N seeded ENTITIES at 500 / 5K / 50K / 500K, seed 0x9e3779b97f4a7c15,
 * rdtsc cycles, p50. Entity = 76B record (EntityRecord size, schema v3);
 * pages filled to natural density for the build's page size. */
static const u64 BENCH_SEED = 0x9e3779b97f4a7c15ull;
static u64 g_lpages;                     /* pages the prefill produced */

static void prefill(segstore_t *st, u64 nents) {
    /* 70% fill factor: a live store's pages carry slack (compaction headroom,
     * obs edits landing in place). 100% density makes every later insert
     * overflow; benching that benches the overflow path, not the melt. */
    const u32 cap70 = (SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / (76u + SEG_SLOT_SIZE) * 7u / 10u;
    u8 rec[76];
    seg_txn_begin(st);
    u32 lpg = 0, in_page = 0; u8 *pg = NULL;
    for (u64 e = 0; e < nents; e++) {
        for (u32 i = 0; i < 76; i++) rec[i] = (u8)(e * 31 + i);
        u16 s;
        if (!pg || in_page >= cap70) {
            pg = seg_txn_alloc(st, SEG_KIND_ENTITY, &lpg);
            in_page = 0;
        }
        if (!pg || !seg_page_insert(pg, rec, 76, &s)) { fprintf(stderr, "prefill fail\n"); exit(1); }
        in_page++;
    }
    seg_txn_commit(st);
    g_lpages = segstore_logical_pages(st);
}

/* one melt batch: touch K scattered pages, ~2 small inserts each */
static u64 melt_batch(segstore_t *st, u32 K, u64 *logical_bytes) {
    u8 rec[90];
    seg_txn_begin(st);
    *logical_bytes = 0;
    for (u32 k = 0; k < K; k++) {
        u32 lpg = (u32)(rng() % g_lpages);
        u8 *w = seg_txn_touch(st, lpg);
        if (!w) { k--; continue; }
        for (u32 r = 0; r < 2; r++) {
            u16 size = (u16)(40 + rng() % 50);
            for (u32 i = 0; i < size; i++) rec[i] = (u8)rng();
            u16 s;
            if (!seg_page_insert(w, rec, size, &s)) {
                /* page full: the graph layer allocates a fresh page for the
                 * new record (entities are not evicted to make room) */
                u32 nl;
                u8 *np = seg_txn_alloc(st, SEG_KIND_ENTITY, &nl);
                if (!np || !seg_page_insert(np, rec, size, &s)) continue;
            }
            *logical_bytes += size;
        }
    }
    u64 t0 = rdtsc_s();
    int ok = seg_txn_commit(st);
    u64 t1 = rdtsc_e();
    if (!ok) { fprintf(stderr, "commit failed\n"); exit(1); }
    return t1 - t0;
}

int main(void) {
    printf("bench_segstore  page_size=%u  scale ladder N=500/5K/50K/500K entities, melt K=16\n",
           (unsigned)SEG_PAGE_SIZE);
    printf("%-8s %8s %14s %14s %8s %16s %10s\n",
           "N", "pages", "cyc/commit p50", "bytes/commit", "amp", "recover p50 cyc", "file MB");

    static const u64 Ns[] = { 500, 5000, 50000, 500000 };
    enum { K = 16, ITERS = 100 };
    for (u32 ni = 0; ni < 4; ni++) {
        u64 N = Ns[ni];
        rng_state = BENCH_SEED + N;
        seg_io_t *io = seg_io_sim_open();
        segstore_t *st = segstore_create(io, 1, 4);
        prefill(st, N);
        u64 base_bytes = seg_io_sim_bytes_written(io);

        u64 cyc[ITERS]; u64 lbytes_total = 0;
        for (u32 it = 0; it < ITERS; it++) {
            u64 lb = 0;
            cyc[it] = melt_batch(st, K, &lb);
            lbytes_total += lb;
        }
        u64 bytes = seg_io_sim_bytes_written(io) - base_bytes;
        double per_commit = (double)bytes / ITERS;
        double amp = (double)bytes / (double)(lbytes_total ? lbytes_total : 1);

        /* recover cost on the final image */
        u64 size; const u8 *b = io->read_base(io, &size);
        u64 rcyc[20];
        for (u32 r = 0; r < 20; r++) {
            u8 *copy = (u8 *)malloc(size); memcpy(copy, b, size);
            extern seg_io_t *bench_mem_io(const u8 *, u64);
            seg_io_t *ro = bench_mem_io(copy, size);
            u64 t0 = rdtsc_s();
            segstore_t *r2 = segstore_open(ro);
            u64 t1 = rdtsc_e();
            if (!r2) { fprintf(stderr, "recover failed\n"); exit(1); }
            rcyc[r] = t1 - t0;
            segstore_close(r2);
            free(copy);
        }
        printf("%-8llu %8llu %14llu %14.0f %8.1f %16llu %10.1f\n",
               (unsigned long long)N, (unsigned long long)g_lpages,
               (unsigned long long)med(cyc, ITERS), per_commit, amp,
               (unsigned long long)med(rcyc, 20), (double)size / 1e6);
        segstore_close(st);
    }

    /* posix: absolute commit latency with real fdatasync on /tmp */
    {
        char path[] = "/tmp/bench_segstore_XXXXXX";
        int fd = mkstemp(path); close(fd);
        seg_io_t *io = seg_io_posix_open(path, 1);
        segstore_t *st = segstore_create(io, 1, 4);
        prefill(st, 50000);
        rng_state = BENCH_SEED;
        u64 cyc[50];
        for (u32 it = 0; it < 50; it++) {
            u64 lb; cyc[it] = melt_batch(st, 16, &lb);
        }
        printf("posix /tmp N=50K K=16 cyc/commit p50: %llu (incl. 2x fdatasync)\n",
               (unsigned long long)med(cyc, 50));
        segstore_close(st);
        unlink(path);
    }
    return 0;
}

/* RO mem io for the recover bench (kept out of the seg_io module: test-only) */
typedef struct { seg_io_t vt; const u8 *buf; u64 size; } bm_t;
static int bm_w(seg_io_t *io, const void *b, u64 l, u64 o){(void)io;(void)b;(void)l;(void)o;return 0;}
static int bm_s(seg_io_t *io){(void)io;return 1;}
static int bm_e(seg_io_t *io, u64 n){(void)io;(void)n;return 0;}
static const u8 *bm_b(seg_io_t *io, u64 *s){bm_t*m=(bm_t*)io;if(s)*s=m->size;return m->buf;}
static void bm_c(seg_io_t *io){free(io);}
seg_io_t *bench_mem_io(const u8 *buf, u64 size){
    bm_t *m=(bm_t*)calloc(1,sizeof *m);
    m->vt.write=bm_w;m->vt.sync=bm_s;m->vt.extend=bm_e;m->vt.read_base=bm_b;m->vt.close=bm_c;
    m->buf=buf;m->size=size;return &m->vt;
}
