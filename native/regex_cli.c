/*
 * regex_cli.c — thin driver for the differential fuzzer (scripts/regex_diff.py).
 *
 *   regex_cli <pattern>        # text is read from stdin (binary-safe)
 *
 * Exit code:  0 = matched,  1 = did not match,  2 = compile error / usage.
 * Reading text from stdin (rather than argv) keeps arbitrary bytes — newlines,
 * NULs, high UTF-8 — intact through the harness.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "regex.h"
#include "re_dfa.h"
#include "re_sheng.h"
#include "re_lsheng.h"

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: %s <pattern>   (text on stdin)\n", argv[0]); return 2; }

    /* REGEX_ENGINE = nfa (default) | dfa | sheng. The fuzzer flips this to
     * oracle all three engines against Python re. dfa/sheng fall back to the
     * NFA exactly as production would when they can't be built. */
    const char *eng = getenv("REGEX_ENGINE");
    int use_dfa   = eng && strcmp(eng, "dfa") == 0;
    int use_sheng = eng && strcmp(eng, "sheng") == 0;
    int use_ldfa  = eng && strcmp(eng, "ldfa") == 0;
    int use_lshg  = eng && strcmp(eng, "lsheng") == 0;

    const char *err = NULL;
    Regex *re = re_compile(argv[1], &err);
    if (!re) { fprintf(stderr, "compile error: %s\n", err ? err : "?"); return 2; }

    /* slurp all of stdin */
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { re_free(re); return 2; }
    for (;;) {
        if (len == cap) { cap *= 2; char *nb = realloc(buf, cap); if (!nb) { free(buf); re_free(re); return 2; } buf = nb; }
        size_t got = fread(buf + len, 1, cap - len, stdin);
        len += got;
        if (got == 0) break;
    }

    int matched;
    if (use_lshg) {
        const char *pol = getenv("REGEX_LSH_POLICY");   /* flush (default) | lru | scalar */
        ReLshengPolicy P = (pol && !strcmp(pol, "lru")) ? LSH_LRU
                         : (pol && !strcmp(pol, "scalar")) ? LSH_SCALAR : LSH_FLUSH;
        ReLsheng *s = re_lsheng_build(re, P);
        if (s) { matched = re_lsheng_search(s, buf, len); re_lsheng_free(s); }
        else   { matched = re_nfa_search(re, buf, len); }               /* hard anchor: NFA */
    } else if (use_ldfa) {
        ReLdfa *L = re_ldfa_build(re, 0);
        if (L) { matched = re_ldfa_search(L, buf, len); re_ldfa_free(L); }
        else   { matched = re_nfa_search(re, buf, len); }               /* hard anchor: NFA */
    } else if (use_sheng) {
        ReDfa *d = re_dfa_build(re);
        ReSheng *sh = d ? re_sheng_build(d) : NULL;
        if (sh)      { matched = re_sheng_search(sh, buf, len); re_sheng_free(sh); }
        else if (d)  { matched = re_dfa_search(d, buf, len); }       /* >16 states: scalar DFA */
        else         { matched = re_nfa_search(re, buf, len); }         /* hard anchor: NFA */
        if (d) re_dfa_free(d);
    } else if (use_dfa) {
        ReDfa *d = re_dfa_build(re);
        if (d) { matched = re_dfa_search(d, buf, len); re_dfa_free(d); }
        else   { matched = re_nfa_search(re, buf, len); }   /* production falls back to the NFA */
    } else {
        matched = re_nfa_search(re, buf, len);
    }
    free(buf);
    re_free(re);
    return matched ? 0 : 1;
}
