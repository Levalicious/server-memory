/*
 * regex.h — a small, byte-level, linear-time regular-expression engine.
 *
 * Design (Decision_OwnRegexEngineInC_2026_07_14):
 *   - FORMAL regular expressions only: NO backreferences, NO lookaround. Every
 *     pattern therefore has an equivalent NFA and matches in O(n·m), n = text
 *     length, m = program size. No backtracking, so no catastrophic blowup on
 *     adversarial (LLM-generated) patterns.
 *   - Byte-level, case-SENSITIVE (Decision_CaseSensitiveSearch_2026_07_14).
 *     Literal UTF-8 search is already correct (pattern+text are both UTF-8
 *     bytes). `.` / ranges / \w over non-ASCII are byte-naive in v1; rune
 *     correctness is a later compiler-only upgrade (Insight_ByteEngineUtf8ViaCompiler).
 *   - The AST is the SINGLE SOURCE OF TRUTH: the matcher compiles it, and the
 *     (future) Cox trigram extractor will walk the SAME tree, so the prefilter
 *     and the matcher can never disagree (Insight_PrefilterMatcherOneParser).
 *
 * Supported syntax (v1): literals, `.`, char classes `[...]` incl. ranges and
 * negation `[^...]`, the class escapes `\d \w \s \D \W \S`, anchors `^ $`,
 * alternation `|`, non-capturing groups `( )`, quantifiers `* + ? {m,n}`, and
 * `\`-escapes of metacharacters + `\n \t \r \f \v`.
 */
#ifndef REGEX_H
#define REGEX_H

#include <stddef.h>

/* ---- AST -------------------------------------------------------------------
 * The shared artifact. RE_CLASS carries a 256-bit byte set so `[...]`, `.`,
 * and the \d/\w/\s family all reduce to one node kind at match time; RE_CHAR
 * is kept distinct so the trigram extractor can read off required literals. */
typedef enum {
    RE_EMPTY,   /* epsilon (matches the empty string) */
    RE_CHAR,    /* the single byte `c` */
    RE_CLASS,   /* any byte whose bit is set in `set` (covers [...], ., \d\w\s) */
    RE_CONCAT,  /* `a` followed by `b` */
    RE_ALT,     /* `a` or `b` */
    RE_STAR,    /* a*  (zero or more) */
    RE_PLUS,    /* a+  (one or more) */
    RE_QUEST,   /* a?  (zero or one) */
    RE_REPEAT,  /* a{min,max}; max < 0 means unbounded */
    RE_BOL,     /* ^  (zero-width: start of text) */
    RE_EOL      /* $  (zero-width: end of text) */
} ReKind;

typedef struct ReNode ReNode;
struct ReNode {
    ReKind        kind;
    unsigned char c;         /* RE_CHAR */
    unsigned char set[32];   /* RE_CLASS: bit b set  =>  byte b matches */
    ReNode       *a, *b;     /* children (CONCAT/ALT use both; unary uses a) */
    int           min, max;  /* RE_REPEAT bounds */
};

/* Parse a NUL-terminated pattern into an AST. On syntax error returns NULL and,
 * if `err` is non-NULL, sets *err to a static human-readable message. The
 * returned tree is owned by the caller (free with re_ast_free). */
ReNode *re_parse(const char *pattern, const char **err);
void    re_ast_free(ReNode *n);

/* ---- Compiled program (opaque) --------------------------------------------- */
typedef struct Regex Regex;

Regex *re_compile_ast(const ReNode *ast);                  /* compile a parsed AST */
Regex *re_compile(const char *pattern, const char **err);  /* parse + compile */
void   re_free(Regex *re);

/* 1 if the pattern matches ANYWHERE in text[0..len) (unanchored, like
 * re.search); 0 otherwise. Linear time, no allocation beyond two O(m) thread
 * lists. Safe on arbitrary bytes including embedded NULs. */
int    re_search(const Regex *re, const char *text, size_t len);

#endif /* REGEX_H */
