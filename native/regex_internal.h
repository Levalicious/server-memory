/*
 * regex_internal.h — NFA program layout, shared between the matcher (regex.c)
 * and the determinizer (re_dfa.c) so BOTH consume the exact same compiled
 * program. Not part of the public API (regex.h). This is what keeps the DFA
 * and the NFA from ever disagreeing: one compiled artifact, two runtimes.
 */
#ifndef REGEX_INTERNAL_H
#define REGEX_INTERNAL_H

#include <stdint.h>
#include "regex.h"

typedef enum { OP_CHAR, OP_CLASS, OP_MATCH, OP_JMP, OP_SPLIT, OP_BOL, OP_EOL } OpCode;
typedef struct { OpCode op; unsigned char c; int x, y; int cls; } Inst;

struct Regex {
    Inst          *insts;     int ninst, icap;
    unsigned char (*cls)[32]; int ncls,  ccap;
    int            ok;
};

/* does byte `b` fall in program class `cls`? */
static inline int re_class_test(const struct Regex *re, int cls, unsigned b) {
    return (re->cls[cls][b >> 3] >> (b & 7)) & 1;
}

/* DFA-construction primitives (defined in re_dfa.c), shared with backends that
 * build states on demand (the lazy Sheng). re_closure fills `out` with the
 * non-EOL (or EOL-passing) epsilon-closure of `seeds`, returns 1 if MATCH is in
 * it; `stack` is scratch of >= ninst ints. re_dfa_hard_anchor returns 1 if the
 * pattern has a non-terminal `$` (must fall back to the NFA). */
int re_closure(const struct Regex *re, const int *seeds, int nseed, int bol,
               int allow_eol, uint64_t *out, int nwords, int *stack);
int re_dfa_hard_anchor(const struct Regex *re);

#endif /* REGEX_INTERNAL_H */
