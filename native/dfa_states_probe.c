/* dfa_states_probe.c — DFA state counts for realistic KB-search patterns, to
 * see which ones exceed the 16-state Sheng fast path (=> scalar DFA) or fail to
 * build (=> NFA). Throwaway probe: cc dfa_states_probe.c regex.c re_dfa.c */
#include <stdio.h>
#include <string.h>
#include "regex.h"
#include "re_dfa.h"

static void probe(const char *pat) {
    const char *err = NULL;
    Regex *re = re_compile(pat, &err);
    if (!re) { printf("  %-42s  COMPILE ERROR (%s)\n", pat, err ? err : "?"); return; }
    ReDfa *d = re_dfa_build(re);
    if (!d) { printf("  %-42s  %5s   NFA (hard-anchor / >4096 states)\n", pat, "-"); re_free(re); return; }
    int n = re_dfa_state_count(d);
    const char *tier = (n <= 16) ? "Sheng" : "scalar DFA";
    printf("  %-42s  %5d   %s\n", pat, n, tier);
    re_dfa_free(d); re_free(re);
}

int main(void) {
    printf("pattern                                     states  tier\n");

    printf("-- literals by length (unanchored substring) --\n");
    probe("abc");                                    /* 3  */
    probe("needle");                                 /* 6  */
    probe("graphstore");                             /* 10 */
    probe("graphstore-v4");                          /* 13 */
    probe("determinizer");                           /* 12 */
    probe("abcdefghijklmno");                        /* 15 */
    probe("abcdefghijklmnop");                       /* 16 */
    probe("Insight_ByteEngineUtf8");                 /* 22 */
    probe("Insight_ByteEngineUtf8ViaCompiler");      /* 33 */

    printf("-- classes / short regex (typical search) --\n");
    probe("[a-z]+");
    probe("[A-Za-z0-9_]+");
    probe("trigram");
    probe("[Tt]rigram");
    probe("Sheng|Hyperscan|PSHUFB");
    probe("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    probe("^Insight_");
    probe("_2026_07_14$");

    printf("-- alternations / multi-substring --\n");
    probe("(foo|bar|baz)");
    probe("(alpha|bravo|charlie|delta|echo)");
    probe("foo.*bar");
    probe("foo.*bar.*baz");

    printf("-- bounded repetition --\n");
    probe("a{10}");
    probe("a{20}");
    probe("[a-z]{16}");
    probe("x{500}");

    return 0;
}
