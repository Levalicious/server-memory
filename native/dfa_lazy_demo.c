/* dfa_lazy_demo.c — shows the lazy DFA's graceful large-state behavior:
 *   - eager re_dfa hits its 4096-state cap and falls back to the NFA;
 *   - lazy re_ldfa handles the same pattern with NO cliff, building only the
 *     states the input actually visits (peak << total).
 * cc dfa_lazy_demo.c regex.c re_dfa.c */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "regex.h"
#include "re_dfa.h"

static void demo(const char *pat, const char *label, const char *text, size_t len) {
    const char *err = NULL;
    Regex *re = re_compile(pat, &err);
    if (!re) { printf("compile error: %s\n", err); return; }

    ReDfa *e = re_dfa_build(re);
    int nfa = re_nfa_search(re, text, len);

    ReLdfa *L = re_ldfa_build(re, 0);
    int lazy = L ? re_ldfa_search(L, text, len) : -1;
    int peak = L ? re_ldfa_peak_states(L) : -1;

    printf("  %-34s eager=%-11s  lazy: match=%d peak_states=%d (nfa=%d)\n",
           label,
           e ? "built" : "NULL->NFA",
           lazy, peak, nfa);

    if (e) re_dfa_free(e);
    if (L) re_ldfa_free(L);
    re_free(re);
}

int main(void) {
    /* ~5000-state pattern (five 1000-char runs): eager exceeds its 4096 cap. */
    const char *big = "a{1000}b{1000}c{1000}d{1000}e{1000}";

    /* a short non-matching text: lazy visits almost nothing */
    const char *shorttxt = "the quick brown fox jumps over the lazy dog";

    /* a text that drives deep into the pattern: 1000 a's then junk */
    static char deep[1300];
    memset(deep, 'a', 1000);
    memcpy(deep + 1000, "then some other bytes", 21);

    printf("pattern = /%s/  (~5000 DFA states, over the 4096 eager cap)\n", big);
    demo(big, "short non-matching text", shorttxt, strlen(shorttxt));
    demo(big, "1000 a's + junk (drives deep)", deep, 1021);

    printf("\nlong literal /Insight_ByteEngineUtf8ViaCompiler/ (34 states):\n");
    const char *lit = "Insight_ByteEngineUtf8ViaCompiler";
    const char *hay = "a walk surfaced Insight_ByteEngineUtf8ViaCompiler in the graph";
    demo(lit, "matching haystack", hay, strlen(hay));
    demo(lit, "non-matching haystack", shorttxt, strlen(shorttxt));

    return 0;
}
