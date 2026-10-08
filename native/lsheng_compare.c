/*
 * lsheng_compare.c — flush vs LRU vs scalar-fallback for the lazy Sheng, on a
 * KB-shaped corpus: a long-literal query over many entries that share deep
 * prefixes (the case that churns the flush policy). Also shows the scalar DFA
 * baselines. Reports steady-state cycles/byte + telemetry (fills/flushes/evicts)
 * after a warm-up pass, so a policy that keeps the cache warm shows near-zero
 * steady fills.
 *   cc -O2 -march=native -mssse3 lsheng_compare.c regex.c re_dfa.c re_lsheng.c
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
#include "re_lsheng.h"

static inline unsigned long long rd0(void){ unsigned a,d; __asm__ __volatile__("lfence\n\trdtsc":"=a"(a),"=d"(d)); return ((unsigned long long)d<<32)|a; }
static inline unsigned long long rd1(void){ unsigned a,d; __asm__ __volatile__("rdtscp":"=a"(a),"=d"(d)::"ecx"); __asm__ __volatile__("lfence":::"memory"); return ((unsigned long long)d<<32)|a; }
static double tsc_hz(void){ struct timespec a,b; clock_gettime(CLOCK_MONOTONIC,&a); unsigned long long c0=rd0(); nanosleep(&(struct timespec){0,100000000L},NULL); unsigned long long c1=rd1(); clock_gettime(CLOCK_MONOTONIC,&b); double s=(double)(b.tv_sec-a.tv_sec)+(double)(b.tv_nsec-a.tv_nsec)*1e-9; return (double)(c1-c0)/s; }
static unsigned long long rng=0x1234567ULL; static unsigned xr(void){ unsigned long long x=rng; x^=x<<13; x^=x>>7; x^=x<<17; rng=x; return (unsigned)(x>>33); }

#define NENT 128
static char   ent[NENT][80];
static size_t elen[NENT];
static size_t total_bytes;

static const char *LIT = "Insight_ByteEngineUtf8ViaCompiler";  /* 33 chars -> 34 DFA states */

static void build_corpus(void) {
    size_t L = strlen(LIT);
    total_bytes = 0;
    for (int i = 0; i < NENT; i++) {
        unsigned r = xr() % 100, d;
        if (r < 40) d = xr() % 3;                 /* 40% no/short prefix */
        else if (r < 75) d = 8 + xr() % 8;        /* 35% shallow prefix (<=16 states) */
        else if (r < 95) d = 18 + xr() % 11;      /* 20% deep prefix (>16 states) */
        else d = (unsigned)L;                     /* 5% full match */
        if (d > L) d = (unsigned)L;
        size_t n = 0;
        for (unsigned k = 0; k < d; k++) ent[i][n++] = LIT[k];
        int tail = 6 + (int)(xr() % 30);
        for (int k = 0; k < tail && n < sizeof ent[i] - 1; k++) ent[i][n++] = (char)('a' + xr() % 26);
        ent[i][n] = 0; elen[i] = n; total_bytes += n;
    }
}

/* scan the whole corpus once; returns total matches (keeps work live) */
static int scan_lsheng(ReLsheng *s){ int m=0; for(int i=0;i<NENT;i++) m+=re_lsheng_search(s,ent[i],elen[i]); return m; }
static int scan_ldfa(ReLdfa *s){ int m=0; for(int i=0;i<NENT;i++) m+=re_ldfa_search(s,ent[i],elen[i]); return m; }
static int scan_dfa(ReDfa *s){ int m=0; for(int i=0;i<NENT;i++) m+=re_dfa_search(s,ent[i],elen[i]); return m; }

int main(void){
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0,&set); sched_setaffinity(0,sizeof set,&set);
    double hz = tsc_hz();
    build_corpus();
    const char *err=NULL; Regex *re=re_compile(LIT,&err);
    ReDfa *ed = re_dfa_build(re);
    printf("# corpus %d entries, %zu bytes; pattern /%s/ = %d total DFA states; TSC ~%.2f GHz\n",
           NENT, total_bytes, LIT, re_dfa_state_count(ed), hz/1e9);
    printf("%-16s %10s %9s  %10s %8s %8s\n","engine","cyc/byte","GiB/s","fills/pass","flushes","evicts");

    const int REPS = 200;
    volatile int sink=0;

    /* eager scalar DFA (baseline) */
    { sink+=scan_dfa(ed); unsigned long long mn=~0ull;
      for(int r=0;r<REPS;r++){ unsigned long long t0=rd0(); sink+=scan_dfa(ed); unsigned long long t1=rd1(); if(t1-t0<mn)mn=t1-t0; }
      double cb=(double)mn/(double)total_bytes; printf("%-16s %10.3f %9.2f  %10s %8s %8s\n","eager scalar DFA",cb,(double)total_bytes*hz/(double)mn/1073741824.0,"-","-","-"); }

    /* lazy scalar DFA */
    { ReLdfa *L=re_ldfa_build(re,0); sink+=scan_ldfa(L); long f0=re_ldfa_peak_states(L);(void)f0;
      unsigned long long mn=~0ull; for(int r=0;r<REPS;r++){ unsigned long long t0=rd0(); sink+=scan_ldfa(L); unsigned long long t1=rd1(); if(t1-t0<mn)mn=t1-t0; }
      double cb=(double)mn/(double)total_bytes; printf("%-16s %10.3f %9.2f  %10s %8s %8s\n","lazy scalar DFA",cb,(double)total_bytes*hz/(double)mn/1073741824.0,"-","-","-");
      re_ldfa_free(L); }

    /* the three lazy-Sheng policies */
    const char *names[3]={"lsheng FLUSH","lsheng LRU","lsheng SCALAR"};
    ReLshengPolicy pols[3]={LSH_FLUSH,LSH_LRU,LSH_SCALAR};
    for(int p=0;p<3;p++){
        ReLsheng *s=re_lsheng_build(re,pols[p]);
        sink+=scan_lsheng(s);                                   /* warm-up pass */
        long fw=re_lsheng_fills(s), xw=re_lsheng_flushes(s), ew=re_lsheng_evicts(s);
        unsigned long long mn=~0ull;
        for(int r=0;r<REPS;r++){ unsigned long long t0=rd0(); sink+=scan_lsheng(s); unsigned long long t1=rd1(); if(t1-t0<mn)mn=t1-t0; }
        double cb=(double)mn/(double)total_bytes;
        double fpp=(double)(re_lsheng_fills(s)-fw)/(double)REPS;   /* steady fills per pass */
        printf("%-16s %10.3f %9.2f  %10.1f %8ld %8ld\n", names[p], cb,
               (double)total_bytes*hz/(double)mn/1073741824.0, fpp,
               (re_lsheng_flushes(s)-xw), (re_lsheng_evicts(s)-ew));
        re_lsheng_free(s);
    }
    re_dfa_free(ed); re_free(re);
    return sink==0x7fffffff;
}
