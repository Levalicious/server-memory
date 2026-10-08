/*
 * test_regex.c — validation harness for the regex engine. Hand-written truth
 * table (verdicts cross-checked against Python `re.search` with re.ASCII),
 * covering every v1 feature + edge cases, plus a parse-error table. Run under
 * ASan+UBSan via `make test_regex`. The exhaustive differential fuzz lives in
 * scripts/regex_diff.py (this is the fast always-on gate).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "regex.h"
#include "re_dfa.h"
#include "re_sheng.h"
#include "re_lsheng.h"

static int fails = 0;

/* Assert `pattern` matches/doesn't match `text` (unanchored), AND that the DFA
 * agrees with the NFA verdict (the DFA must never diverge from its reference). */
static void want(const char *pattern, const char *text, int expect) {
    const char *err = NULL;
    Regex *re = re_compile(pattern, &err);
    if (!re) { printf("  FAIL: /%s/ failed to compile (%s)\n", pattern, err ? err : "?"); fails++; return; }
    size_t len = strlen(text);
    int got = re_nfa_search(re, text, len);
    if (got != expect) {
        printf("  FAIL: /%s/ vs \"%s\": NFA got %d want %d\n", pattern, text, got, expect);
        fails++;
    }
    ReDfa *d = re_dfa_build(re);
    if (d) {
        int dgot = re_dfa_search(d, text, len);
        if (dgot != got) { printf("  FAIL: /%s/ vs \"%s\": DFA %d != NFA %d\n", pattern, text, dgot, got); fails++; }
        ReSheng *sh = re_sheng_build(d);   /* <=16 states => Sheng-able */
        if (sh) {
            int sgot = re_sheng_search(sh, text, len);
            if (sgot != got) { printf("  FAIL: /%s/ vs \"%s\": Sheng %d != NFA %d\n", pattern, text, sgot, got); fails++; }
            re_sheng_free(sh);
        }
        re_dfa_free(d);
    } else {
        printf("  NOTE: /%s/ DFA not built (state-cap fallback)\n", pattern);
    }
    /* lazy DFA: default budget, and a tiny budget to force constant flushing */
    for (int bud = 0; bud <= 8; bud += 8) {
        ReLdfa *ld = re_ldfa_build(re, bud);
        if (ld) {
            int lg = re_ldfa_search(ld, text, len);
            if (lg != got) { printf("  FAIL: /%s/ vs \"%s\": Ldfa(bud=%d) %d != NFA %d\n", pattern, text, bud, lg, got); fails++; }
            re_ldfa_free(ld);
        }
    }
    /* lazy Sheng — all three eviction policies must agree with the NFA */
    for (int pol = 0; pol < 3; pol++) {
        ReLsheng *ls = re_lsheng_build(re, (ReLshengPolicy)pol);
        if (ls) {
            int lsg = re_lsheng_search(ls, text, len);
            if (lsg != got) { printf("  FAIL: /%s/ vs \"%s\": Lsheng(pol=%d) %d != NFA %d\n", pattern, text, pol, lsg, got); fails++; }
            re_lsheng_free(ls);
        }
    }
    re_free(re);
}

/* In-process differential fuzz: DFA verdict MUST equal NFA verdict over random
 * patterns × random texts. Fast (no subprocess), so we can run a lot. */
static uint64_t rng = 0x243f6a8885a308d3ULL;
static uint64_t xr(void) { uint64_t x = rng; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return rng = x; }

static void fuzz_dfa_vs_nfa(int rounds) {
    static const char *frag[] = {
        "a", "b", "c", "[0-9]", "[a-z]", "[^a]", "\\d", "\\w", "\\s", ".",
        "(a|b)", "a*", "b+", "c?", "foo", "\\.", "^", "$", "a{1,3}", "[a-c]+",
    };
    const int NF = (int)(sizeof frag / sizeof frag[0]);
    static const char alpha[] = "abcd0123 \t\n.";
    const int AL = (int)(sizeof alpha - 1);

    int built = 0, skipped = 0, bad = 0, cmp = 0, shengable = 0;
    for (int r = 0; r < rounds; r++) {
        char pat[80]; size_t pl = 0;
        int nfrag = 1 + (int)(xr() % 5);
        for (int i = 0; i < nfrag; i++) {
            const char *f = frag[xr() % NF];
            size_t fl = strlen(f);
            if (pl + fl + 1 >= sizeof pat) break;
            memcpy(pat + pl, f, fl); pl += fl;
        }
        pat[pl] = 0;

        const char *err = NULL;
        Regex *re = re_compile(pat, &err);
        if (!re) continue;                 /* not a valid pattern; skip */
        ReDfa *d = re_dfa_build(re);
        if (!d) { skipped++; re_free(re); continue; }
        built++;
        ReSheng *sh = re_sheng_build(d);
        if (sh) shengable++;
        ReLdfa *ld = re_ldfa_build(re, 0);      /* default-budget lazy DFA */
        ReLdfa *lt = re_ldfa_build(re, 8);      /* tiny budget: constant flushing */
        ReLsheng *lsf = re_lsheng_build(re, LSH_FLUSH);
        ReLsheng *lsl = re_lsheng_build(re, LSH_LRU);
        ReLsheng *lss = re_lsheng_build(re, LSH_SCALAR);

        for (int t = 0; t < 16; t++) {
            char txt[24]; int tl = (int)(xr() % (sizeof txt - 1));
            for (int i = 0; i < tl; i++) txt[i] = alpha[xr() % AL];
            int nfa   = re_nfa_search(re, txt, (size_t)tl);
            int dfa   = re_dfa_search(d, txt, (size_t)tl);
            int sheng = sh  ? re_sheng_search(sh, txt, (size_t)tl) : nfa;
            int lazy  = ld  ? re_ldfa_search(ld, txt, (size_t)tl) : nfa;
            int lazyf = lt  ? re_ldfa_search(lt, txt, (size_t)tl) : nfa;
            int lsff  = lsf ? re_lsheng_search(lsf, txt, (size_t)tl) : nfa;
            int lslr  = lsl ? re_lsheng_search(lsl, txt, (size_t)tl) : nfa;
            int lssc  = lss ? re_lsheng_search(lss, txt, (size_t)tl) : nfa;
            cmp++;
            if (nfa != dfa || nfa != sheng || nfa != lazy || nfa != lazyf || nfa != lsff || nfa != lslr || nfa != lssc) {
                bad++;
                if (bad <= 20) {
                    printf("  FUZZ MISMATCH /%s/: NFA=%d DFA=%d Sheng=%d Lazy=%d LzF=%d LshF=%d LshLRU=%d LshSc=%d text=[",
                           pat, nfa, dfa, sheng, lazy, lazyf, lsff, lslr, lssc);
                    for (int i = 0; i < tl; i++) printf("%02x ", (unsigned char)txt[i]);
                    printf("]\n");
                }
            }
        }
        if (sh) re_sheng_free(sh);
        if (ld) re_ldfa_free(ld);
        if (lt) re_ldfa_free(lt);
        if (lsf) re_lsheng_free(lsf);
        if (lsl) re_lsheng_free(lsl);
        if (lss) re_lsheng_free(lss);
        re_dfa_free(d);
        re_free(re);
    }
    printf("  fuzz DFA==NFA: compared=%d mismatches=%d (built=%d shengable<=16st=%d skipped=%d)\n",
           cmp, bad, built, shengable, skipped);
    if (bad) fails += bad;
}

/* Assert that `pattern` is a syntax error. */
static void want_err(const char *pattern) {
    const char *err = NULL;
    Regex *re = re_compile(pattern, &err);
    if (re) { printf("  FAIL: /%s/ compiled but should be a parse error\n", pattern); fails++; re_free(re); }
}

int main(void) {
    /* literals + unanchored search */
    want("abc", "xabcy", 1);
    want("abc", "ab", 0);
    want("abc", "abc", 1);
    want("", "anything", 1);
    want("", "", 1);

    /* anchors */
    want("^abc", "abc", 1);
    want("^abc", "xabc", 0);
    want("abc$", "abc", 1);
    want("abc$", "abcd", 0);
    want("^b", "abc", 0);
    want("c$", "abc", 1);
    want("$", "", 1);
    want("^$", "", 1);
    want("^$", "x", 0);
    /* $ matches at end-of-text AND before a single trailing newline (Python
     * default, non-MULTILINE) — regression from a differential-fuzz find */
    want("a$", "a\n", 1);
    want("a$", "a", 1);
    want("a$", "a\nb", 0);
    want(".$", "x\n", 1);
    want("a$", "a\n\n", 0);   /* only ONE trailing newline counts */
    want("^$", "\n", 1);

    /* dot (excludes newline, byte-level) */
    want("a.c", "axc", 1);
    want("a.c", "abc", 1);
    want("a.c", "a\nc", 0);
    want("^.{3}$", "abc", 1);
    want("^.{3}$", "abcd", 0);
    want(".*", "anything", 1);

    /* character classes */
    want("[abc]", "zzb", 1);
    want("[abc]", "xyz", 0);
    want("[^abc]", "abcx", 1);
    want("[^abc]", "abc", 0);
    want("[a-f]", "xyze", 1);
    want("[a-f]", "xyz", 0);
    want("[]a]", "]", 1);
    want("[]a]", "a", 1);
    want("[]a]", "b", 0);
    want("[a-]", "-", 1);
    want("[a-]", "b", 0);
    want("[\\d]", "a5", 1);
    want("[^\\d]", "123", 0);
    want("[^\\d]", "12a", 1);

    /* predefined classes (ASCII) */
    want("\\d+", "abc123", 1);
    want("\\d+", "abc", 0);
    want("\\w+", "   foo_1 ", 1);
    want("\\s", "abc def", 1);
    want("\\s", "abcdef", 0);
    want("\\D", "123", 0);
    want("\\D", "12a3", 1);

    /* alternation + groups */
    want("a|b", "xby", 1);
    want("a|b", "xyz", 0);
    want("(ab)+", "xababy", 1);
    want("(ab)+", "xba", 0);
    want("(a|b)*c", "ababc", 1);
    want("(a|b)*c", "ababd", 0);

    /* quantifiers */
    want("a*", "", 1);
    want("a+", "", 0);
    want("a?b", "b", 1);
    want("a?b", "ab", 1);
    want("colou?r", "color", 1);
    want("colou?r", "colour", 1);
    want("colou?r", "colr", 0);
    want("a{2,3}", "xaaay", 1);
    want("a{2,3}", "xay", 0);
    want("a{2}", "aa", 1);
    want("a{2}", "a", 0);
    want("a{2,}", "aaaa", 1);
    want("a{0}", "xyz", 1);

    /* escapes of metacharacters */
    want("\\.", "a.b", 1);
    want("\\.", "axb", 0);
    want("\\*", "a*b", 1);
    want("a\\+b", "a+b", 1);
    want("\\(", "f(x)", 1);

    /* literal UTF-8 (byte-level, already correct for literals) */
    want("caf\xc3\xa9", "a caf\xc3\xa9 here", 1);   /* "café" */
    want("caf\xc3\xa9", "cafe", 0);
    want("\xe6\x97\xa5\xe6\x9c\xac", "x\xe6\x97\xa5\xe6\x9c\xac y", 1);  /* "日本" */

    /* epsilon-cycle termination (the reason for per-position dedup) */
    want("(a*)*b", "b", 1);
    want("(a*)*b", "aaab", 1);
    want("(a*)*b", "aaa", 0);
    want("(a*)*", "aaa", 1);
    want("^a.*z$", "abcz", 1);
    want("^a.*z$", "abc", 0);

    /* parse errors (rejected, not silently reinterpreted) */
    want_err("(");
    want_err(")");
    want_err("*a");
    want_err("a\\");
    want_err("[a");
    want_err("[z-a]");
    want_err("\\q");

    /* in-process DFA==NFA differential fuzz */
    fuzz_dfa_vs_nfa(30000);

    if (fails == 0) printf("ALL PASS\n");
    else printf("%d FAIL(s)\n", fails);
    return fails ? 1 : 0;
}
