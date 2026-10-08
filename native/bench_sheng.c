/*
 * bench_sheng.c — quiet-Sheng vs scalar-DFA throughput (replicates the blog's
 * BasicDFA-vs-Sheng comparison; Concept_ShengSpeedResult ~6.5x on Skylake).
 *
 *   make bench_sheng          # -O2 -march=native -mssse3
 *
 * Both runners are QUIET (transitions only, no accept detection), so neither
 * early-outs and both scan the whole buffer — a pure transition-rate probe.
 * BasicDFA is the blog's baseline: u8 t2[16][256], scalar `s = t2[s][c]`, which
 * is memory-latency bound even from L1. Sheng is one PSHUFB per byte.
 *
 * Reported per pattern: cycles/byte (frequency-independent) + bytes/cycle +
 * speedup. Min over repeats (Heuristic_MicrobenchAreStatistical: min is the
 * least-noisy estimator of the underlying cost).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <time.h>
#include "regex.h"
#include "re_dfa.h"
#include "re_sheng.h"

static inline unsigned long long rd0(void) {
    unsigned a, d; __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d)); return ((unsigned long long)d << 32) | a;
}
static inline unsigned long long rd1(void) {
    unsigned a, d; __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx");
    __asm__ __volatile__("lfence" ::: "memory"); return ((unsigned long long)d << 32) | a;
}

static unsigned long long rng = 0x9e3779b97f4a7c15ULL;
static unsigned xr(void) { unsigned long long x = rng; x ^= x << 13; x ^= x >> 7; x ^= x << 17; rng = x; return (unsigned)(x >> 33); }

/* Invariant TSC ticks at a fixed rate = wall-clock time, so ticks/rate gives
 * real seconds regardless of turbo; GiB/s below is therefore true throughput. */
static double calibrate_tsc_hz(void) {
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    unsigned long long c0 = rd0();
    nanosleep(&(struct timespec){ 0, 100000000L }, NULL);   /* 100 ms */
    unsigned long long c1 = rd1();
    clock_gettime(CLOCK_MONOTONIC, &b);
    double secs = (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) * 1e-9;
    return (double)(c1 - c0) / secs;
}

/* BasicDFA: the blog's scalar baseline (u8 2D table, scalar table walk). */
static unsigned char g_t2[16][256];
static int basicdfa_run(unsigned char start, const unsigned char *p, size_t len) {
    unsigned char s = start;
    for (size_t i = 0; i < len; i++) s = g_t2[s][p[i]];
    return s;
}

int main(void) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set); sched_setaffinity(0, sizeof set, &set);

    const size_t LEN = 1u << 20;      /* 1 MiB */
    unsigned char *buf = malloc(LEN);
    for (size_t i = 0; i < LEN; i++) buf[i] = (unsigned char)('a' + (xr() % 26));   /* lowercase */

    /* Patterns that NEVER match a lowercase-only buffer, so the noisy runner
     * scans the whole thing (measuring the per-byte accept-check cost) instead
     * of stopping at a match. */
    const char *pats[] = { "[0-9]+", "[A-Z0-9_]+", "(FOO|BAR|BAZ|QUX)", "a[bcd]*Z", "[a-z]X[0-9]" };
    const int NP = (int)(sizeof pats / sizeof pats[0]);
    const int REPS = 40;
    volatile int sink = 0;

    double tsc_hz = calibrate_tsc_hz();
    printf("# TSC ~%.3f GHz (invariant TSC -> true wall-clock GiB/s). Non-matching patterns (full scan).\n", tsc_hz / 1e9);
    printf("%-18s %6s  %10s %10s %10s   %9s %9s %9s\n",
           "pattern", "states", "basic c/B", "quiet c/B", "noisy c/B",
           "basicGiB/s", "quietGiB/s", "noisyGiB/s");
    for (int pi = 0; pi < NP; pi++) {
        const char *err = NULL;
        Regex *re = re_compile(pats[pi], &err);
        if (!re) { printf("%-18s  compile error: %s\n", pats[pi], err ? err : "?"); continue; }
        ReDfa *d = re_dfa_build(re);
        if (!d) { printf("%-18s  (no DFA / hard anchor)\n", pats[pi]); re_free(re); continue; }
        int n = re_dfa_state_count(d);
        ReSheng *sh = re_sheng_build(d);
        if (!sh) { printf("%-18s %8d  (>16 states — not Sheng-able)\n", pats[pi], n); re_dfa_free(d); re_free(re); continue; }

        /* pack BasicDFA table */
        unsigned char start = (unsigned char)re_dfa_start(d);
        for (int st = 0; st < 16; st++)
            for (int b = 0; b < 256; b++)
                g_t2[st][b] = (unsigned char)(st < n ? re_dfa_trans(d, st, b) : 0);

        /* warm + time BasicDFA */
        sink += basicdfa_run(start, buf, LEN);
        unsigned long long bmin = ~0ull;
        for (int r = 0; r < REPS; r++) {
            unsigned long long t0 = rd0(); sink += basicdfa_run(start, buf, LEN); unsigned long long t1 = rd1();
            if (t1 - t0 < bmin) bmin = t1 - t0;
        }
        /* warm + time quiet Sheng */
        sink += re_sheng_run_quiet(sh, (const char *)buf, LEN);
        unsigned long long qmin = ~0ull;
        for (int r = 0; r < REPS; r++) {
            unsigned long long t0 = rd0(); sink += re_sheng_run_quiet(sh, (const char *)buf, LEN); unsigned long long t1 = rd1();
            if (t1 - t0 < qmin) qmin = t1 - t0;
        }
        /* noisy Sheng — must NOT match (else it early-outs and the timing is bogus) */
        if (re_sheng_search(sh, (const char *)buf, LEN)) {
            printf("%-18s %6d  (pattern matched the buffer — noisy timing invalid, skipped)\n", pats[pi], n);
            re_sheng_free(sh); re_dfa_free(d); re_free(re); continue;
        }
        unsigned long long nmin = ~0ull;
        for (int r = 0; r < REPS; r++) {
            unsigned long long t0 = rd0(); sink += re_sheng_search(sh, (const char *)buf, LEN); unsigned long long t1 = rd1();
            if (t1 - t0 < nmin) nmin = t1 - t0;
        }

        double bcb = (double)bmin / (double)LEN, qcb = (double)qmin / (double)LEN, ncb = (double)nmin / (double)LEN;
        double bg = (double)LEN * tsc_hz / (double)bmin / 1073741824.0;
        double qg = (double)LEN * tsc_hz / (double)qmin / 1073741824.0;
        double ng = (double)LEN * tsc_hz / (double)nmin / 1073741824.0;
        printf("%-18s %6d  %10.3f %10.3f %10.3f   %9.2f %9.2f %9.2f\n",
               pats[pi], n, bcb, qcb, ncb, bg, qg, ng);

        re_sheng_free(sh); re_dfa_free(d); re_free(re);
    }
    free(buf);
    return sink == 0x7fffffff ? 1 : 0;   /* keep sink live */
}
