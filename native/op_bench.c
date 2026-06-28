/*
 * Per-op microbench for the v3 graph store. rdtsc cycles/op over a fixed, seeded
 * graph; emits a per-op distribution as JSON on stdout for base-vs-head CI
 * regression comparison (no committed baselines — see scripts/bench-compare).
 *
 *   make bench                    # -O2 -march=native, NO ASan / NO double-free-check
 *   /tmp/mf_test_bench [N] [seed]
 *
 * Adaptive sampling (ported from riff/benches/lcs_bench.rs, governed by
 * Heuristic_MicrobenchAreStatistical): instead of a hand-tuned fixed iteration
 * count per op, each op is sampled until the relative standard error of the mean
 * RE = CV/sqrt(n) (CV = stddev/mean) drops below TARGET_RE, or a sample/budget cap
 * is hit. Each sample is a BATCH-MEAN (time over B iterations / B): single-op
 * times have fat tails (scheduler/cache outliers) that make CV explode, but the
 * CV of batch-means is well-behaved, which is what makes the RE criterion sound.
 * riff persists per-op counts across runs because Criterion can't grow sample_size
 * mid-run; this harness can, so it resolves the target error in-run (stateless).
 *
 * Reported per op: min/p50/p90/p99/mean (cycles/op) + achieved n + re. p50 (median)
 * is the headline comparison metric: robust to tails and representative of typical
 * ("average") cost; min/p90/p99 give the floor and tail for context.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#include <math.h>
#include "graph.h"

static inline u64 tsc_begin(void) {
    unsigned a, d; __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d)); return ((u64)d << 32) | a;
}
static inline u64 tsc_end(void) {
    unsigned a, d; __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx");
    __asm__ __volatile__("lfence" ::: "memory"); return ((u64)d << 32) | a;
}

static u64 rng;
static inline u64 xs(void) { u64 x = rng; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return rng = x; }

/* Frequency reference: fixed compute+memory work. Its cycles track the run's CPU
 * frequency (invariant TSC counts wall-clock), so dividing op cost by the
 * reference cancels the GLOBAL frequency/turbo factor that otherwise makes the
 * same code look 5-13% different run-to-run. The compare script does the math. */
static u32 g_refbuf[4096];
static u64 reference_work(void) {
    u64 acc = 0;
    for (int j = 0; j < 1024; j++) { u64 r = xs(); acc += g_refbuf[r & 4095]; g_refbuf[r & 4095] = (u32)(acc ^ r); }
    return acc;
}

/* Adaptive-sampling tunables (mirror riff's TARGET_RE_UPPER / clamps). */
#define POOL       4096u
#define POOL_MASK  (POOL - 1u)
static const double TARGET_RE    = 0.02;        /* stop when RE = CV/sqrt(n) <= 2% */
static const double TARGET_BATCH = 20000.0;     /* aim each batch ~20k cycles to amortize rdtsc */
static const size_t MIN_SAMPLES  = 30;
static const size_t MAX_SAMPLES  = 2000;
static const u64    BUDGET_CYC   = 80000000ull; /* per-op wall cap (~30ms @2.6GHz) for O(N) ops */

static int cmp_d(const void *a, const void *b) { double x = *(const double *)a, y = *(const double *)b; return (x > y) - (x < y); }

static int g_first = 1;
/* Compute distribution + RE from the collected batch-mean samples and emit JSON. */
static void emit_op(const char *name, double *s, size_t n) {
    double mean = 0; for (size_t i = 0; i < n; i++) mean += s[i]; mean /= (double)n;
    double var = 0; for (size_t i = 0; i < n; i++) { double d = s[i] - mean; var += d * d; }
    var /= (n > 1) ? (double)(n - 1) : 1.0;
    double cv = (mean > 0.0) ? sqrt(var) / mean : 0.0;
    double re = cv / sqrt((double)n);
    qsort(s, n, sizeof(double), cmp_d);
    size_t p90 = (size_t)(n * 0.90); if (p90 >= n) p90 = n - 1;
    size_t p99 = (size_t)(n * 0.99); if (p99 >= n) p99 = n - 1;
    printf("%s    \"%s\": {\"min\": %.1f, \"p50\": %.1f, \"p90\": %.1f, \"p99\": %.1f, \"mean\": %.1f, \"n\": %zu, \"re\": %.4f}",
           g_first ? "" : ",\n", name, s[0], s[n / 2], s[p90], s[p99], mean, n, re);
    g_first = 0;
}

/* Sample __VA_ARGS__ (a statement using `_k` to index a precomputed input pool)
 * in auto-sized batches until RE <= TARGET_RE, or n / cycle budget is exhausted. */
#define ADAPT(NAME, ...) do {                                                                       \
    u64 _wmin = ~0ull, _wtot = 0; int _wi = 0;                                                      \
    while (_wi < 256 && _wtot < 2000000ull) { size_t _k = (size_t)_wi & POOL_MASK; (void)_k;        \
        u64 _a = tsc_begin(); __VA_ARGS__; u64 _b = tsc_end(); u64 _e = _b - _a;                    \
        _wtot += _e; if (_e < _wmin) _wmin = _e; _wi++; }                                           \
    double _c = (double)_wmin; if (_c < 1.0) _c = 1.0;                                              \
    size_t _B = (size_t)(TARGET_BATCH / _c); if (_B < 1) _B = 1; if (_B > POOL) _B = POOL;          \
    double _mean = 0, _m2 = 0, _re = 1.0; size_t _n = 0; u64 _tot = 0;                              \
    while (_n < MAX_SAMPLES) {                                                                      \
        u64 _t0 = tsc_begin();                                                                      \
        for (size_t _i = 0; _i < _B; _i++) { size_t _k = (_n * _B + _i) & POOL_MASK; (void)_k; __VA_ARGS__; } \
        u64 _t1 = tsc_end(); _tot += (_t1 - _t0);                                                   \
        double _s = (double)(_t1 - _t0) / (double)_B; samp[_n] = _s;                                \
        double _d = _s - _mean; _mean += _d / (double)(_n + 1); _m2 += _d * (_s - _mean); _n++;     \
        if (_n >= MIN_SAMPLES) {                                                                    \
            double _var = _m2 / (double)(_n - 1);                                                   \
            double _cv = (_mean > 0.0) ? sqrt(_var) / _mean : 0.0;                                  \
            _re = _cv / sqrt((double)_n);                                                           \
            if (_re <= TARGET_RE || _tot >= BUDGET_CYC) break;                                      \
        }                                                                                           \
    }                                                                                               \
    emit_op(NAME, samp, _n);                                                                        \
} while (0)

int main(int argc, char **argv) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set); sched_setaffinity(0, sizeof set, &set);
    size_t N = (argc > 1) ? strtoul(argv[1], NULL, 10) : 2000;
    rng       = (argc > 2) ? strtoull(argv[2], NULL, 10) : 0x9e3779b97f4a7c15ull;

    const char *gp = "/tmp/opbench.graph", *sp = "/tmp/opbench.strings";
    unlink(gp); unlink(sp);
    stringtable_t *st = st_open(sp, 1u << 20);
    graph_t *g = graph_open(gp, st, 1u << 20);

    u64 *off = malloc(N * sizeof(u64));
    char nm[32], ty[32], ob[64];
    const u64 now = 1700000000000ull;

    for (size_t i = 0; i < N; i++) {
        int nl = snprintf(nm, sizeof nm, "ent-%zu", i);
        int tl = snprintf(ty, sizeof ty, "type-%zu", i % 20);
        off[i] = graph_create_entity(g, (const u8 *)nm, (u16)nl, (const u8 *)ty, (u16)tl, now);
        int ol = snprintf(ob, sizeof ob, "obs-a-%zu", i); graph_add_observation(g, off[i], (const u8 *)ob, (u16)ol, now);
        ol = snprintf(ob, sizeof ob, "obs-b-%zu", i); graph_add_observation(g, off[i], (const u8 *)ob, (u16)ol, now);
    }
    for (size_t i = 0; i < N; i++) {
        for (int k = 0; k < 3; k++) {
            size_t j = xs() % N; if (j == i) continue;
            int rl = snprintf(nm, sizeof nm, "rel-%llu", (unsigned long long)(xs() % 8));
            graph_create_relation(g, off[i], off[j], (const u8 *)nm, (u16)rl, now);
        }
    }

    /* Precompute input pools so the timed regions contain ONLY the op (no per-iter
     * snprintf/rng inside timing), which lets us batch cleanly. */
    size_t *idx  = malloc(POOL * sizeof *idx);
    size_t *idx2 = malloc(POOL * sizeof *idx2);
    size_t *tpix = malloc(POOL * sizeof *tpix);
    u64    *seed = malloc(POOL * sizeof *seed);
    char  **pat  = malloc(POOL * sizeof *pat);
    for (u32 k = 0; k < POOL; k++) {
        idx[k] = xs() % N; idx2[k] = xs() % N; tpix[k] = xs() % 20; seed[k] = xs();
        char *p = malloc(32); snprintf(p, 32, "ent-%llu$", (unsigned long long)(xs() % N)); pat[k] = p;
    }
    char **names = malloc(N * sizeof *names); u16 *nlen = malloc(N * sizeof *nlen);
    for (size_t i = 0; i < N; i++) { char *p = malloc(24); int l = snprintf(p, 24, "ent-%zu", i); names[i] = p; nlen[i] = (u16)l; }
    char *tynames[20]; u16 tylen[20];
    for (int t = 0; t < 20; t++) { char *p = malloc(16); int l = snprintf(p, 16, "type-%d", t); tynames[t] = p; tylen[t] = (u16)l; }

    printf("{\n  \"graph\": {\"entities\": %zu, \"relations\": %u},\n  \"ops\": {\n", N, graph_relation_count(g));

    double *samp = malloc((MAX_SAMPLES > 2048 ? MAX_SAMPLES : 2048) * sizeof *samp);
    u32 cap = (u32)N + 4; u64 *out = malloc((size_t)cap * 8);
    entity_t e;
    volatile u64 sink = 0; volatile u32 sink32 = 0; (void)sink; (void)sink32;

    ADAPT("reference",         sink   = reference_work());
    ADAPT("lookup",            sink   = graph_lookup(g, (const u8 *)names[idx[_k]], nlen[idx[_k]]));
    ADAPT("read_entity",       graph_read_entity(g, off[idx[_k]], &e));
    ADAPT("neighbors_d1",      sink32 = graph_neighbors(g, off[idx[_k]], 1, DIR_ANY, out, cap));
    ADAPT("neighbors_d2",      sink32 = graph_neighbors(g, off[idx[_k]], 2, DIR_ANY, out, cap));
    ADAPT("find_path_d6",      sink32 = graph_find_path(g, off[idx[_k]], off[idx2[_k]], 6, DIR_ANY, out, cap));
    ADAPT("search",            sink32 = graph_search(g, pat[_k], out, cap));
    ADAPT("entities_by_type",  sink32 = graph_entities_by_type(g, (const u8 *)tynames[tpix[_k]], tylen[tpix[_k]], out, cap));
    ADAPT("random_walk_d5",    sink32 = graph_random_walk(g, off[idx[_k]], 5, DIR_ANY, 1, seed[_k], out, 8));
    ADAPT("inc_walker_visit",  graph_inc_walker_visit(g, off[idx[_k]]));
    ADAPT("structural_sample", graph_structural_sample(g, 1, 0.85));
    ADAPT("compute_merw_psi",  graph_compute_merw_psi(g, 0.85, 100, 1e-8));

    /* Mutating ops: non-stationary (each call grows the graph), so they can't be
     * batch-sampled i.i.d. Keep a fixed single-op timed count; report distribution. */
    const size_t KM = 2000;
    u64 *noff = malloc(KM * sizeof *noff);
    for (size_t i = 0; i < KM; i++) { int nl = snprintf(nm, sizeof nm, "new-%zu", i);
        u64 t0 = tsc_begin(); u64 r = graph_create_entity(g, (const u8 *)nm, (u16)nl, (const u8 *)"NewType", 7, now); u64 t1 = tsc_end();
        noff[i] = r; samp[i] = (double)(t1 - t0); }
    emit_op("create_entity", samp, KM);

    for (size_t i = 0; i < KM; i++) { u64 a = noff[i], b = noff[xs() % KM];
        u64 t0 = tsc_begin(); graph_create_relation(g, a, b, (const u8 *)"newrel", 6, now); u64 t1 = tsc_end();
        samp[i] = (double)(t1 - t0); }
    emit_op("create_relation", samp, KM);

    printf("\n  }\n}\n");

    for (u32 k = 0; k < POOL; k++) free(pat[k]);
    for (size_t i = 0; i < N; i++) free(names[i]);
    for (int t = 0; t < 20; t++) free(tynames[t]);
    free(idx); free(idx2); free(tpix); free(seed); free(pat); free(names); free(nlen);
    free(samp); free(out); free(off); free(noff);
    graph_close(g); st_close(st);
    return 0;
}
