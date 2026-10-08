/*
 * bench_regex.c — compile + match microbench for the regex engine.
 *
 *   make bench_regex           # -O2 -march=native, NO ASan
 *
 * Same methodology as op_bench.c (Bench_AllocatorHarness): rdtsc cycles, core-
 * pinned, adaptive batch-mean sampling until the relative standard error of the
 * mean drops below TARGET_RE. Emits per-case cycle stats as JSON. Cases cover
 * compile cost and match cost across pattern shapes (literal / class / anchored
 * / alternation / catch-all) and text sizes, including a non-matching full-scan
 * (the worst case for an unanchored search).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <math.h>
#include "regex.h"

static inline unsigned long long tsc_begin(void) {
    unsigned a, d; __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d)); return ((unsigned long long)d << 32) | a;
}
static inline unsigned long long tsc_end(void) {
    unsigned a, d; __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx");
    __asm__ __volatile__("lfence" ::: "memory"); return ((unsigned long long)d << 32) | a;
}

static const double TARGET_RE    = 0.02;
static const double TARGET_BATCH = 20000.0;
static const size_t MIN_SAMPLES  = 30;
static const size_t MAX_SAMPLES  = 2000;
static const unsigned long long BUDGET_CYC = 80000000ull;

static double samp[2048];
static int    g_first = 1;

static int cmp_d(const void *a, const void *b) { double x = *(const double *)a, y = *(const double *)b; return (x > y) - (x < y); }

static void emit(const char *name, size_t n) {
    double mean = 0; for (size_t i = 0; i < n; i++) mean += samp[i]; mean /= (double)n;
    double var = 0; for (size_t i = 0; i < n; i++) { double d = samp[i] - mean; var += d * d; }
    var /= (n > 1) ? (double)(n - 1) : 1.0;
    double re = (mean > 0 ? sqrt(var) / mean : 0) / sqrt((double)n);
    qsort(samp, n, sizeof(double), cmp_d);
    size_t p90 = (size_t)(n * 0.90); if (p90 >= n) p90 = n - 1;
    printf("%s    \"%s\": {\"min\": %.1f, \"p50\": %.1f, \"p90\": %.1f, \"mean\": %.1f, \"n\": %zu, \"re\": %.4f}",
           g_first ? "" : ",\n", name, samp[0], samp[n / 2], samp[p90], mean, n, re);
    g_first = 0;
}

/* Sample STMT in auto-sized batches until RE <= TARGET_RE / budget hit. */
#define ADAPT(NAME, STMT) do {                                                                  \
    unsigned long long _wmin = ~0ull, _wtot = 0; int _wi = 0;                                   \
    while (_wi < 256 && _wtot < 2000000ull) {                                                   \
        unsigned long long _a = tsc_begin(); STMT; unsigned long long _e = tsc_end() - _a;      \
        _wtot += _e; if (_e < _wmin) _wmin = _e; _wi++; }                                       \
    double _c = (double)_wmin; if (_c < 1.0) _c = 1.0;                                          \
    size_t _B = (size_t)(TARGET_BATCH / _c); if (_B < 1) _B = 1; if (_B > 4096) _B = 4096;      \
    double _mean = 0, _m2 = 0; size_t _n = 0; unsigned long long _tot = 0;                      \
    while (_n < MAX_SAMPLES) {                                                                  \
        unsigned long long _t0 = tsc_begin();                                                   \
        for (size_t _i = 0; _i < _B; _i++) { STMT; }                                            \
        unsigned long long _t1 = tsc_end(); _tot += (_t1 - _t0);                                \
        double _s = (double)(_t1 - _t0) / (double)_B; samp[_n] = _s;                            \
        double _d = _s - _mean; _mean += _d / (double)(_n + 1); _m2 += _d * (_s - _mean); _n++; \
        if (_n >= MIN_SAMPLES) {                                                                \
            double _var = _m2 / (double)(_n - 1);                                               \
            double _re = (_mean > 0 ? sqrt(_var) / _mean : 0) / sqrt((double)_n);               \
            if (_re <= TARGET_RE || _tot >= BUDGET_CYC) break;                                  \
        }                                                                                       \
    }                                                                                           \
    emit(NAME, _n);                                                                             \
} while (0)

/* a realistic ~140-byte observation, and a longer non-matching haystack */
static const char *OBS  = "Node log = dense [count,cap][u64 offsets]. Used by pagerank/MERW sweeps, graph_orphaned, regex/type scans. NOT random_walk (that walks edges)";

int main(void) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set); sched_setaffinity(0, sizeof set, &set);

    /* a 1KB haystack with the needle only near the end (worst-ish for a scan) */
    static char big[1024];
    memset(big, 'x', sizeof big);
    memcpy(big + 1000, "needle_here", 11);
    size_t biglen = sizeof big;
    size_t obslen = strlen(OBS);

    struct { const char *name, *pat, *text; size_t len; } M[] = {
        { "match_literal_hit",   "MERW",                     OBS, obslen },
        { "match_literal_miss",  "ZZZZ",                     OBS, obslen },
        { "match_class_digits",  "[0-9]+",                   OBS, obslen },
        { "match_word_run",      "\\w+",                     OBS, obslen },
        { "match_anchored_name", "^Node .*edges\\)$",        OBS, obslen },
        { "match_alternation",   "pagerank|MERW|walker",     OBS, obslen },
        { "match_catchall",      ".*scans.*",                OBS, obslen },
        { "match_miss_1kb_scan", "need![0-9]",               big, biglen },
        { "match_hit_1kb_scan",  "needle_here",              big, biglen },
    };
    int NM = (int)(sizeof M / sizeof M[0]);

    const char *CP[] = { "abc", "[a-fA-F0-9]+", "Insight_[A-Za-z0-9]+_2026",
                         "(a|b|c|d)+", "a{2,50}", "^Node .*edges\\)$" };
    int NC = (int)(sizeof CP / sizeof CP[0]);

    printf("{\n  \"compile\": {\n");
    g_first = 1;
    for (int i = 0; i < NC; i++) {
        char nm[64]; snprintf(nm, sizeof nm, "c%d", i);
        const char *err = NULL; const char *pat = CP[i];
        ADAPT(nm, { const char *e = NULL; Regex *r = re_compile(pat, &e); re_free(r); });
        (void)err;
    }
    printf("\n  },\n  \"match\": {\n");
    g_first = 1;
    for (int i = 0; i < NM; i++) {
        const char *err = NULL;
        Regex *re = re_compile(M[i].pat, &err);
        if (!re) { fprintf(stderr, "bench: bad pattern /%s/: %s\n", M[i].pat, err ? err : "?"); return 1; }
        const char *text = M[i].text; size_t len = M[i].len;
        volatile int sink = 0;
        ADAPT(M[i].name, { sink = re_nfa_search(re, text, len); });
        (void)sink;
        re_free(re);
    }
    printf("\n  }\n}\n");
    return 0;
}
