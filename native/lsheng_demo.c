/* lsheng_demo.c — shows lazy Sheng running a pattern eager Sheng rejects.
 * A 34-state literal is >16 total states, so re_sheng_build() returns NULL; but
 * lazy Sheng populates lanes on demand and runs it, using only as many lanes as
 * the input actually activates. Telemetry (fills/flushes) shows warm-up vs
 * steady-state as the same pattern scans many "entities".
 * cc -mssse3 lsheng_demo.c regex.c re_dfa.c re_sheng.c re_lsheng.c */
#include <stdio.h>
#include <string.h>
#include "regex.h"
#include "re_dfa.h"
#include "re_sheng.h"
#include "re_lsheng.h"

int main(void) {
    const char *lit = "Insight_ByteEngineUtf8ViaCompiler";   /* 33 chars -> 34 DFA states */
    const char *err = NULL;
    Regex *re = re_compile(lit, &err);
    ReDfa *d = re_dfa_build(re);
    printf("pattern /%s/  total DFA states = %d\n", lit, re_dfa_state_count(d));
    printf("eager Sheng build: %s\n\n", re_sheng_build(d) ? "OK" : "NULL (>16 total -> rejected)");

    ReLsheng *s = re_lsheng_build(re);

    /* 200 non-matching "entities" (short KB-ish strings), scanned by ONE lazy
     * Sheng. Warm-up happens on the first few; the rest ride filled masks. */
    const char *ents[] = {
        "graph store v4 single-writer segmented", "trigram prefilter over adjacency",
        "the walk surfaced a vicious cycle node", "melt session decentralize hubs",
        "Insight_ByteEngineUtf8ViaCompiler appears here once",   /* the one that matches */
        "node log eliminated name index sole registry", "pike vm thompson nfa closure",
    };
    int NE = (int)(sizeof ents / sizeof ents[0]);

    long f0 = 0, x0 = 0;
    for (int round = 0; round < 40; round++) {
        for (int e = 0; e < NE; e++) {
            int m = re_lsheng_search(s, ents[e], strlen(ents[e]));
            if (round == 0) printf("  ent[%d] match=%d  (fills=%ld flushes=%ld)\n",
                                   e, m, re_lsheng_fills(s), re_lsheng_flushes(s));
        }
        if (round == 0) { f0 = re_lsheng_fills(s); x0 = re_lsheng_flushes(s); }
    }
    printf("\nafter 40 rounds x %d entities = %d scans:\n", NE, 40 * NE);
    printf("  total fills=%ld flushes=%ld ; after round 0: fills were %ld, flushes %ld\n",
           re_lsheng_fills(s), re_lsheng_flushes(s), f0, x0);
    printf("  => steady-state added fills = %ld over %d later scans (mask cache warm)\n",
           re_lsheng_fills(s) - f0, 39 * NE);

    re_lsheng_free(s); re_dfa_free(d); re_free(re);
    return 0;
}
