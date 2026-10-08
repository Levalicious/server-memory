/*
 * bench_widesheng.c — measured cost of scaling Sheng past 16 states via the
 * portable general-gather (Mytkowicz §4): an N-state transition is N/16 PSHUFBs
 * (one per 16-state block) combined by a block-select on the state's high
 * nibble. No AVX-512 needed. Answers "how much net negative is >16 states" by
 * benchmarking N = 16/32/48/64 (B = 1..4 blocks) at quiet (transition-only)
 * throughput, each verified against a scalar reference so we time a CORRECT
 * wide-Sheng, not a fast broken loop.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <time.h>
#include <stdint.h>
#include <immintrin.h>

static inline unsigned long long rd0(void) {
    unsigned a, d; __asm__ __volatile__("lfence\n\trdtsc" : "=a"(a), "=d"(d)); return ((unsigned long long)d << 32) | a;
}
static inline unsigned long long rd1(void) {
    unsigned a, d; __asm__ __volatile__("rdtscp" : "=a"(a), "=d"(d) :: "ecx");
    __asm__ __volatile__("lfence" ::: "memory"); return ((unsigned long long)d << 32) | a;
}
static double calibrate_tsc_hz(void) {
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a); unsigned long long c0 = rd0();
    nanosleep(&(struct timespec){ 0, 100000000L }, NULL);
    unsigned long long c1 = rd1(); clock_gettime(CLOCK_MONOTONIC, &b);
    double s = (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) * 1e-9;
    return (double)(c1 - c0) / s;
}
static unsigned long long rng = 0x9e3779b97f4a7c15ULL;
static unsigned xr(void) { unsigned long long x = rng; x ^= x << 13; x ^= x >> 7; x ^= x << 17; rng = x; return (unsigned)(x >> 33); }

/* general-gather step, specialized on B (compile-time). state is broadcast to
 * all lanes; each block j contributes PSHUFB(block_j, state) selected where the
 * state's high nibble == 16*j. */
#define GEN(NAME, B)                                                                              \
static int NAME(const __m128i (*tab)[4], const unsigned char *p, size_t len, unsigned char start){\
    __m128i sv = _mm_set1_epi8((char)start); const __m128i HM = _mm_set1_epi8((char)0xF0);        \
    for (size_t i = 0; i < len; i++) {                                                            \
        const __m128i *blk = tab[p[i]];                                                           \
        __m128i acc = _mm_shuffle_epi8(blk[0], sv);                                               \
        if (B >= 2) { __m128i m = _mm_cmpeq_epi8(_mm_and_si128(sv, HM), _mm_set1_epi8(16));        \
                      acc = _mm_blendv_epi8(acc, _mm_shuffle_epi8(blk[1], sv), m); }              \
        if (B >= 3) { __m128i m = _mm_cmpeq_epi8(_mm_and_si128(sv, HM), _mm_set1_epi8(32));        \
                      acc = _mm_blendv_epi8(acc, _mm_shuffle_epi8(blk[2], sv), m); }              \
        if (B >= 4) { __m128i m = _mm_cmpeq_epi8(_mm_and_si128(sv, HM), _mm_set1_epi8(48));        \
                      acc = _mm_blendv_epi8(acc, _mm_shuffle_epi8(blk[3], sv), m); }              \
        sv = acc;                                                                                 \
    }                                                                                             \
    return (unsigned char)_mm_cvtsi128_si32(sv);                                                  \
}
GEN(ws1, 1) GEN(ws2, 2) GEN(ws3, 3) GEN(ws4, 4)

static unsigned char nxt[256][64];      /* scalar transition table: next[byte][state] */
static __m128i        tab[256][4];       /* packed into 16-state blocks */

static int scalar_run(const unsigned char *p, size_t len, unsigned char start) {
    unsigned char s = start;
    for (size_t i = 0; i < len; i++) s = nxt[p[i]][s];
    return s;
}

int main(void) {
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set); sched_setaffinity(0, sizeof set, &set);
    double tsc_hz = calibrate_tsc_hz();

    const size_t LEN = 1u << 20;
    unsigned char *buf = malloc(LEN);
    for (size_t i = 0; i < LEN; i++) buf[i] = (unsigned char)xr();

    printf("# TSC ~%.3f GHz. General-gather quiet Sheng, N-state transition = N/16 PSHUFB + block-select.\n", tsc_hz / 1e9);
    printf("%-8s %4s  %10s %10s   %s\n", "states", "B", "cyc/byte", "GiB/s", "slowdown vs 16");

    const int REPS = 40;
    volatile int sink = 0;
    double base_cb = 0;

    int Ns[] = { 16, 32, 48, 64 };
    for (int ni = 0; ni < 4; ni++) {
        int N = Ns[ni], B = N / 16;
        for (int b = 0; b < 256; b++) {
            for (int s = 0; s < N; s++) nxt[b][s] = (unsigned char)(xr() % (unsigned)N);
            for (int j = 0; j < 4; j++) {
                unsigned char row[16];
                for (int k = 0; k < 16; k++) { int idx = 16 * j + k; row[k] = (idx < N) ? nxt[b][idx] : 0; }
                tab[b][j] = _mm_loadu_si128((const __m128i *)row);
            }
        }
        int (*fn)(const __m128i (*)[4], const unsigned char *, size_t, unsigned char) =
            (B == 1) ? ws1 : (B == 2) ? ws2 : (B == 3) ? ws3 : ws4;

        int simd = fn(tab, buf, LEN, 0);
        int ref  = scalar_run(buf, LEN, 0);
        if (simd != ref) { printf("states=%d  CORRECTNESS FAIL: simd=%d scalar=%d\n", N, simd, ref); continue; }

        sink += fn(tab, buf, LEN, 0);
        unsigned long long mn = ~0ull;
        for (int r = 0; r < REPS; r++) {
            unsigned long long t0 = rd0(); sink += fn(tab, buf, LEN, 0); unsigned long long t1 = rd1();
            if (t1 - t0 < mn) mn = t1 - t0;
        }
        double cb = (double)mn / (double)LEN;
        double g  = (double)LEN * tsc_hz / (double)mn / 1073741824.0;
        if (ni == 0) base_cb = cb;
        printf("%-8d %4d  %10.3f %10.2f   %6.2fx\n", N, B, cb, g, cb / base_cb);
    }
    free(buf);
    return sink == 0x7fffffff ? 1 : 0;
}
