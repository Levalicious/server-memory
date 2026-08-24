/*
 * regex.c — parser + NFA compiler + Thompson/Pike simulation.
 *
 * Pipeline:  pattern --re_parse--> AST --re_compile_ast--> Inst[] --re_search--> bool
 *
 * The matcher is a Pike VM run for a BOOLEAN verdict (does the pattern match
 * anywhere?). Because we never need submatch offsets, there is no thread
 * priority and no capture tracking — just parallel NFA-state stepping with a
 * per-position visited stamp, which is what guarantees linear time and
 * termination even on epsilon-cycles like (a*)*.
 */
#include "regex.h"
#include "regex_internal.h"

#include <stdlib.h>
#include <string.h>

/* Bounds that keep a hostile pattern from exploding the compiled program.
 * {m,n} is expanded by code duplication, so its factor is capped here. */
#define RE_MAX_REPEAT 1000

/* ======================================================================
 * AST construction + byte-class helpers
 * ====================================================================== */

static ReNode *node(ReKind k) {
    ReNode *n = calloc(1, sizeof *n);
    if (n) n->kind = k;
    return n;
}
static ReNode *node_char(unsigned char c) { ReNode *n = node(RE_CHAR); if (n) n->c = c; return n; }
static ReNode *node_bin(ReKind k, ReNode *a, ReNode *b) {
    ReNode *n = node(k);
    if (!n) { re_ast_free(a); re_ast_free(b); return NULL; }
    n->a = a; n->b = b; return n;
}
static ReNode *node_un(ReKind k, ReNode *a) {
    ReNode *n = node(k);
    if (!n) { re_ast_free(a); return NULL; }
    n->a = a; return n;
}

void re_ast_free(ReNode *n) {
    if (!n) return;
    re_ast_free(n->a);
    re_ast_free(n->b);
    free(n);
}

static void set_bit(unsigned char set[32], unsigned b)      { set[b >> 3] |= (unsigned char)(1u << (b & 7)); }
static int  get_bit(const unsigned char set[32], unsigned b) { return (set[b >> 3] >> (b & 7)) & 1; }
static void set_range(unsigned char set[32], unsigned lo, unsigned hi) { for (unsigned b = lo; b <= hi; b++) set_bit(set, b); }
static void set_negate(unsigned char set[32]) { for (int i = 0; i < 32; i++) set[i] = (unsigned char)~set[i]; }

/* the \d \w \s families, as ASCII byte classes (matches Python `re` under the
 * re.ASCII flag, which the differential harness uses as its oracle) */
static void class_digit(unsigned char set[32]) { set_range(set, '0', '9'); }
static void class_word(unsigned char set[32])  { set_range(set, '0', '9'); set_range(set, 'A', 'Z'); set_range(set, 'a', 'z'); set_bit(set, '_'); }
static void class_space(unsigned char set[32]) { set_bit(set, ' '); set_bit(set, '\t'); set_bit(set, '\n'); set_bit(set, '\r'); set_bit(set, '\f'); set_bit(set, '\v'); }

/* ======================================================================
 * Parser (recursive descent)
 *
 *   alt    := concat ('|' concat)*
 *   concat := repeat*
 *   repeat := atom ('*' | '+' | '?' | '{m,n}')*
 *   atom   := '(' alt ')' | '[' class ']' | '.' | '^' | '$' | escape | byte
 * ====================================================================== */

typedef struct { const unsigned char *s; const char *err; } P;

static ReNode *parse_alt(P *p);

static void fail(P *p, const char *msg) { if (!p->err) p->err = msg; }

/* Decode a backslash escape. On a class escape (\d\w\s\D\W\S) fills `set` and
 * sets *is_class=1; otherwise yields a literal byte in *lit. Returns 0 on
 * success, -1 on an unsupported escape (so we never silently diverge). */
static int parse_escape(P *p, unsigned char *lit, unsigned char set[32], int *is_class) {
    unsigned char e = *p->s;
    if (e == 0) { fail(p, "trailing backslash"); return -1; }
    p->s++;
    *is_class = 0;
    switch (e) {
        case 'd': memset(set, 0, 32); class_digit(set); *is_class = 1; return 0;
        case 'w': memset(set, 0, 32); class_word(set);  *is_class = 1; return 0;
        case 's': memset(set, 0, 32); class_space(set); *is_class = 1; return 0;
        case 'D': memset(set, 0, 32); class_digit(set); set_negate(set); *is_class = 1; return 0;
        case 'W': memset(set, 0, 32); class_word(set);  set_negate(set); *is_class = 1; return 0;
        case 'S': memset(set, 0, 32); class_space(set); set_negate(set); *is_class = 1; return 0;
        case 'n': *lit = '\n'; return 0;
        case 't': *lit = '\t'; return 0;
        case 'r': *lit = '\r'; return 0;
        case 'f': *lit = '\f'; return 0;
        case 'v': *lit = '\v'; return 0;
        case '0': *lit = '\0'; return 0;
        default:
            /* Escaped non-alphanumeric = that literal byte (covers every
             * metacharacter: \. \* \\ \( \[ ...). An escaped ASCII letter we
             * don't recognise is an ERROR, not a silent literal, so the
             * matcher can never disagree with the oracle. */
            if ((e >= 'a' && e <= 'z') || (e >= 'A' && e <= 'Z') || (e >= '0' && e <= '9')) {
                fail(p, "unsupported escape");
                return -1;
            }
            *lit = e;
            return 0;
    }
}

/* Parse the body of [...] (cursor is just past '['). */
static ReNode *parse_class(P *p) {
    ReNode *n = node(RE_CLASS);
    if (!n) return NULL;
    int negate = 0;
    if (*p->s == '^') { negate = 1; p->s++; }
    /* a ']' immediately here is a literal member (POSIX rule) */
    int first = 1;
    while (*p->s && !(*p->s == ']' && !first)) {
        first = 0;
        unsigned char lo;
        if (*p->s == '\\') {
            p->s++;
            unsigned char eset[32]; int is_class; unsigned char lit;
            if (parse_escape(p, &lit, eset, &is_class) < 0) { re_ast_free(n); return NULL; }
            if (is_class) { for (int i = 0; i < 32; i++) n->set[i] |= eset[i]; continue; }
            lo = lit;
        } else {
            lo = *p->s++;
        }
        /* range?  lo '-' hi   (a trailing '-' before ']' is a literal '-') */
        if (*p->s == '-' && p->s[1] && p->s[1] != ']') {
            p->s++; /* consume '-' */
            unsigned char hi;
            if (*p->s == '\\') {
                p->s++;
                unsigned char eset[32]; int is_class; unsigned char lit;
                if (parse_escape(p, &lit, eset, &is_class) < 0) { re_ast_free(n); return NULL; }
                if (is_class) { fail(p, "class escape as range endpoint"); re_ast_free(n); return NULL; }
                hi = lit;
            } else {
                hi = *p->s++;
            }
            if (hi < lo) { fail(p, "reversed class range"); re_ast_free(n); return NULL; }
            set_range(n->set, lo, hi);
        } else {
            set_bit(n->set, lo);
        }
    }
    if (*p->s != ']') { fail(p, "unterminated character class"); re_ast_free(n); return NULL; }
    p->s++; /* consume ']' */
    if (negate) set_negate(n->set);
    return n;
}

static ReNode *parse_atom(P *p) {
    unsigned char ch = *p->s;
    switch (ch) {
        case '(': {
            p->s++;
            ReNode *inner = parse_alt(p);
            if (!inner) return NULL;
            if (*p->s != ')') { fail(p, "missing )"); re_ast_free(inner); return NULL; }
            p->s++;
            return inner;
        }
        case '[':
            p->s++;
            return parse_class(p);
        case '.': {
            p->s++;
            ReNode *n = node(RE_CLASS);
            if (!n) return NULL;
            memset(n->set, 0xFF, 32);
            /* `.` matches any byte except newline (matches Python's default) */
            n->set['\n' >> 3] &= (unsigned char)~(1u << ('\n' & 7));
            return n;
        }
        case '^': p->s++; return node(RE_BOL);
        case '$': p->s++; return node(RE_EOL);
        case '\\': {
            p->s++;
            unsigned char eset[32]; int is_class; unsigned char lit;
            if (parse_escape(p, &lit, eset, &is_class) < 0) return NULL;
            if (is_class) { ReNode *n = node(RE_CLASS); if (!n) return NULL; memcpy(n->set, eset, 32); return n; }
            return node_char(lit);
        }
        case ')': case '|': case 0:
            fail(p, "unexpected token");
            return NULL;
        case '*': case '+': case '?':
            fail(p, "nothing to repeat");
            return NULL;
        default:
            p->s++;
            return node_char(ch);
    }
}

/* Try to parse a {m,n} bound at the cursor. Returns 1 and sets *mn on success
 * (cursor advanced past '}'); returns 0 and leaves the cursor untouched if the
 * text is not a well-formed bound (then '{' is treated as a literal). */
static int parse_brace(P *p, int *mn_min, int *mn_max) {
    const unsigned char *save = p->s;
    if (*p->s != '{') return 0;
    p->s++;
    int mn = 0, has_min = 0;
    while (*p->s >= '0' && *p->s <= '9') { mn = mn * 10 + (*p->s - '0'); has_min = 1; p->s++; if (mn > RE_MAX_REPEAT) mn = RE_MAX_REPEAT; }
    int mx;
    if (*p->s == '}') {                 /* {n} */
        if (!has_min) { p->s = save; return 0; }
        mx = mn;
    } else if (*p->s == ',') {
        p->s++;
        if (*p->s == '}') {             /* {n,} unbounded */
            if (!has_min) { p->s = save; return 0; }
            mx = -1;
        } else {                        /* {n,m} */
            int m2 = 0, has_max = 0;
            while (*p->s >= '0' && *p->s <= '9') { m2 = m2 * 10 + (*p->s - '0'); has_max = 1; p->s++; if (m2 > RE_MAX_REPEAT) m2 = RE_MAX_REPEAT; }
            if (!has_max || *p->s != '}') { p->s = save; return 0; }
            mx = m2;
        }
    } else {
        p->s = save; return 0;
    }
    if (*p->s != '}') { p->s = save; return 0; }
    p->s++;
    if (!has_min) mn = 0;
    if (mx >= 0 && mx < mn) { p->s = save; return 0; }
    *mn_min = mn; *mn_max = mx;
    return 1;
}

static ReNode *parse_repeat(P *p) {
    ReNode *a = parse_atom(p);
    if (!a) return NULL;
    for (;;) {
        unsigned char q = *p->s;
        if (q == '*')      { p->s++; a = node_un(RE_STAR, a); }
        else if (q == '+') { p->s++; a = node_un(RE_PLUS, a); }
        else if (q == '?') { p->s++; a = node_un(RE_QUEST, a); }
        else if (q == '{') {
            int mn, mx;
            if (!parse_brace(p, &mn, &mx)) { a = node_bin(RE_CONCAT, a, node_char('{')); p->s++; }
            else { ReNode *r = node(RE_REPEAT); if (!r) { re_ast_free(a); return NULL; } r->a = a; r->min = mn; r->max = mx; a = r; }
        }
        else break;
        if (!a) return NULL;
    }
    return a;
}

static ReNode *parse_concat(P *p) {
    /* empty concatenation = epsilon */
    if (*p->s == 0 || *p->s == '|' || *p->s == ')') return node(RE_EMPTY);
    ReNode *a = parse_repeat(p);
    if (!a) return NULL;
    while (*p->s && *p->s != '|' && *p->s != ')') {
        ReNode *b = parse_repeat(p);
        if (!b) { re_ast_free(a); return NULL; }
        a = node_bin(RE_CONCAT, a, b);
        if (!a) return NULL;
    }
    return a;
}

static ReNode *parse_alt(P *p) {
    ReNode *a = parse_concat(p);
    if (!a) return NULL;
    while (*p->s == '|') {
        p->s++;
        ReNode *b = parse_concat(p);
        if (!b) { re_ast_free(a); return NULL; }
        a = node_bin(RE_ALT, a, b);
        if (!a) return NULL;
    }
    return a;
}

ReNode *re_parse(const char *pattern, const char **err) {
    P p = { (const unsigned char *)pattern, NULL };
    ReNode *n = parse_alt(&p);
    if (n && *p.s != 0) { fail(&p, "unexpected trailing input"); re_ast_free(n); n = NULL; }
    if (!n && err) *err = p.err ? p.err : "parse error";
    return n;
}

/* ======================================================================
 * NFA compiler:  AST -> instruction program
 * ====================================================================== */

/* OpCode / Inst / struct Regex now live in regex_internal.h (shared with the
 * determinizer). */

static int emit(Regex *re, OpCode op) {
    if (re->ninst == re->icap) {
        int nc = re->icap ? re->icap * 2 : 32;
        Inst *ni = realloc(re->insts, (size_t)nc * sizeof(Inst));
        if (!ni) { re->ok = 0; return 0; }
        re->insts = ni; re->icap = nc;
    }
    int i = re->ninst++;
    re->insts[i].op = op; re->insts[i].c = 0; re->insts[i].x = re->insts[i].y = 0; re->insts[i].cls = 0;
    return i;
}
static int add_class(Regex *re, const unsigned char set[32]) {
    if (re->ncls == re->ccap) {
        int nc = re->ccap ? re->ccap * 2 : 8;
        unsigned char (*na)[32] = realloc(re->cls, (size_t)nc * 32);
        if (!na) { re->ok = 0; return 0; }
        re->cls = na; re->ccap = nc;
    }
    memcpy(re->cls[re->ncls], set, 32);
    return re->ncls++;
}

static void compile(Regex *re, const ReNode *n) {
    if (!re->ok || !n) return;
    switch (n->kind) {
        case RE_EMPTY: break;
        case RE_CHAR: { int i = emit(re, OP_CHAR); if (re->ok) re->insts[i].c = n->c; break; }
        case RE_CLASS: { int c = add_class(re, n->set); int i = emit(re, OP_CLASS); if (re->ok) re->insts[i].cls = c; break; }
        case RE_BOL: emit(re, OP_BOL); break;
        case RE_EOL: emit(re, OP_EOL); break;
        case RE_CONCAT: compile(re, n->a); compile(re, n->b); break;
        case RE_ALT: {
            int s = emit(re, OP_SPLIT);
            if (!re->ok) break;
            re->insts[s].x = re->ninst; compile(re, n->a);
            int j = emit(re, OP_JMP);
            if (!re->ok) break;
            re->insts[s].y = re->ninst; compile(re, n->b);
            re->insts[j].x = re->ninst;
            break;
        }
        case RE_STAR: {
            int s = emit(re, OP_SPLIT);
            if (!re->ok) break;
            re->insts[s].x = re->ninst; compile(re, n->a);
            int j = emit(re, OP_JMP);
            if (!re->ok) break;
            re->insts[j].x = s;
            re->insts[s].y = re->ninst;
            break;
        }
        case RE_PLUS: {
            int l1 = re->ninst; compile(re, n->a);
            int s = emit(re, OP_SPLIT);
            if (!re->ok) break;
            re->insts[s].x = l1; re->insts[s].y = re->ninst;
            break;
        }
        case RE_QUEST: {
            int s = emit(re, OP_SPLIT);
            if (!re->ok) break;
            re->insts[s].x = re->ninst; compile(re, n->a);
            re->insts[s].y = re->ninst;
            break;
        }
        case RE_REPEAT: {
            int lo = n->min, hi = n->max;
            for (int i = 0; i < lo && re->ok; i++) compile(re, n->a);      /* mandatory copies */
            if (hi < 0) {                                                  /* {lo,} -> a* tail */
                int s = emit(re, OP_SPLIT);
                if (!re->ok) break;
                re->insts[s].x = re->ninst; compile(re, n->a);
                int j = emit(re, OP_JMP);
                if (!re->ok) break;
                re->insts[j].x = s; re->insts[s].y = re->ninst;
            } else {                                                       /* (hi-lo) optional copies */
                for (int i = lo; i < hi && re->ok; i++) {
                    int s = emit(re, OP_SPLIT);
                    if (!re->ok) break;
                    re->insts[s].x = re->ninst; compile(re, n->a);
                    re->insts[s].y = re->ninst;
                }
            }
            break;
        }
    }
}

Regex *re_compile_ast(const ReNode *ast) {
    Regex *re = calloc(1, sizeof *re);
    if (!re) return NULL;
    re->ok = 1;
    compile(re, ast);
    emit(re, OP_MATCH);
    if (!re->ok) { re_free(re); return NULL; }
    return re;
}

Regex *re_compile(const char *pattern, const char **err) {
    ReNode *ast = re_parse(pattern, err);
    if (!ast) return NULL;
    Regex *re = re_compile_ast(ast);
    re_ast_free(ast);
    if (!re && err && !*err) *err = "compile failed";
    return re;
}

void re_free(Regex *re) {
    if (!re) return;
    free(re->insts);
    free(re->cls);
    free(re);
}

/* ======================================================================
 * Pike VM (boolean, unanchored)
 * ====================================================================== */

typedef struct { int *pc; int n; } TList;

/* Add pc (and, via epsilon-closure, everything reachable through JMP/SPLIT and
 * the zero-width anchors) to `l`, deduped by `seen[pc] == gen`. `sp` is the
 * current text position, needed to evaluate ^ and $.
 *
 * `$` semantics match Python's default (non-MULTILINE): it asserts at the end
 * of the text OR immediately before a single trailing newline. `^` asserts only
 * at the very start. */
static void addthread(TList *l, int *seen, int gen, const Regex *re, int pc,
                      size_t sp, const unsigned char *s, size_t len) {
    if (seen[pc] == gen) return;
    seen[pc] = gen;
    const Inst *in = &re->insts[pc];
    switch (in->op) {
        case OP_JMP:
            addthread(l, seen, gen, re, in->x, sp, s, len);
            break;
        case OP_SPLIT:
            addthread(l, seen, gen, re, in->x, sp, s, len);
            addthread(l, seen, gen, re, in->y, sp, s, len);
            break;
        case OP_BOL:
            if (sp == 0) addthread(l, seen, gen, re, pc + 1, sp, s, len);
            break;
        case OP_EOL:
            if (sp == len || (sp + 1 == len && s[sp] == '\n'))
                addthread(l, seen, gen, re, pc + 1, sp, s, len);
            break;
        default:
            l->pc[l->n++] = pc;   /* CHAR / CLASS / MATCH */
            break;
    }
}

int re_search(const Regex *re, const char *text, size_t len) {
    const unsigned char *s = (const unsigned char *)text;
    int m = re->ninst;
    int *seen = malloc((size_t)m * sizeof(int));
    int *pca  = malloc((size_t)m * sizeof(int));
    int *pcb  = malloc((size_t)m * sizeof(int));
    if (!seen || !pca || !pcb) { free(seen); free(pca); free(pcb); return 0; }
    for (int i = 0; i < m; i++) seen[i] = -1;

    TList cl = { pca, 0 }, nl = { pcb, 0 };
    int gen = 0, matched = 0;

    /* prime: a thread that could start a match at position 0 */
    gen++; addthread(&cl, seen, gen, re, 0, 0, s, len);

    for (size_t sp = 0; ; sp++) {
        gen++; nl.n = 0;
        int byte = (sp < len) ? s[sp] : -1;
        for (int k = 0; k < cl.n; k++) {
            const Inst *in = &re->insts[cl.pc[k]];
            if (in->op == OP_MATCH) { matched = 1; break; }
            if (byte < 0) continue;
            if (in->op == OP_CHAR) {
                if (byte == in->c) addthread(&nl, seen, gen, re, cl.pc[k] + 1, sp + 1, s, len);
            } else if (in->op == OP_CLASS) {
                if (get_bit(re->cls[in->cls], (unsigned)byte)) addthread(&nl, seen, gen, re, cl.pc[k] + 1, sp + 1, s, len);
            }
        }
        if (matched) break;
        /* unanchored: also let a fresh match begin at the next position */
        if (sp < len) addthread(&nl, seen, gen, re, 0, sp + 1, s, len);
        TList t = cl; cl = nl; nl = t;
        if (sp >= len) break;
    }

    free(seen); free(pca); free(pcb);
    return matched;
}
