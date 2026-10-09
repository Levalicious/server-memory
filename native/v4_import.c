/*
 * v4_import.c — one-shot v3 (memfile/graph.c) -> v4 (segstore/graph4) import.
 *
 * Decision_V4ImportShape_2026_10_08 (docs: Finding_MigratorStatus): offline
 * C tool shaped like test_parity — v3 readers + graph4 in one binary, with
 * copy where compare was. Preserves the rank payload exactly for LIVE data:
 * dual mtimes, structural/walker visits, psi per entity, and edge mtimes.
 * TOTALS: v3 persists totals in its file header (orphaned visits of deleted
 * entities included); per Decision_Lev_ReconciliationRulings_2026_10_09
 * ruling 4-i, v4 persists totals verbatim in META too — the orphaned
 * component transfers exactly and is also reported separately. The v3 store
 * is READ-ONLY here: run it against a quiesced copy, never the live files.
 *
 * Usage:  v4_import <v3_base> <v4_dir>
 *   v3_base = path minus .graph/.strings (or with either extension);
 *   v4_dir  = created if absent; REFUSED if it already contains a v4 store.
 *
 * Order mirrors src/migrate.ts: create entities + observations, create
 * relations, THEN restore per-entity fields verbatim (the ops clobber
 * mtime/obsMtime), then totals — all in one mstore txn. Validation reopens
 * the written store and compares field-for-field (incl. edge mtimes and the
 * rank payload); any mismatch exits 1.
 */
#include "graph.h"       /* v3: graph.c on memfile */
#include "segstore.h"    /* v4: segstore + graph4  */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <errno.h>

typedef struct {
    u64 entities;
    u64 relations_in, relations_created, relations_dedup;
    u64 orphaned_sv, orphaned_wv;      /* src totals minus live-entity sums */
    u64 mismatches;
    int ok;
} v4i_report_t;

#define V4I_MAX_PRINT 50

static void v4i_mismatch(v4i_report_t *rep, const char *fmt, ...) {
    rep->mismatches++;
    if (rep->mismatches <= V4I_MAX_PRINT) {
        va_list ap;
        va_start(ap, fmt);
        fputs("v4_import MISMATCH: ", stderr);
        vfprintf(stderr, fmt, ap);
        fputc('\n', stderr);
        va_end(ap);
        if (rep->mismatches == V4I_MAX_PRINT)
            fprintf(stderr, "v4_import: (further mismatches elided)\n");
    }
}

/* progress + phase timing (stderr; the import runs minutes at real scale) */
static double v4i_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + 1e-9 * (double)ts.tv_nsec;
}
static void v4i_prog(const char *phase, u32 done, u32 total, double t0) {
    double el = v4i_now() - t0;
    fprintf(stderr, "v4_import: %s %u/%u  %.1fs  %.0f/s\n",
            phase, done, total, el, el > 0 ? (double)done / el : 0.0);
    fflush(stderr);
}

/* adjacency multiset rows "rel|dir|target|mtime", sorted, for byte-equality */
static int v4i_cmp_row(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
static char **v4i_edge_rows_v3(graph_t *g, stringtable_t *st, u64 off, u32 *n_out) {
    u32 ec = graph_edge_count(g, off);
    *n_out = ec;
    if (!ec) return NULL;
    adj_entry_t *es = (adj_entry_t *)malloc((size_t)ec * sizeof *es);
    char **rows = (char **)malloc((size_t)ec * sizeof *rows);
    if (!es || !rows) { free(es); free(rows); *n_out = 0; return NULL; }
    graph_read_edges(g, off, es, ec);
    for (u32 k = 0; k < ec; k++) {
        u16 rl = 0, tnl = 0;
        const u8 *rb = st_get(st, es[k].rel_type_id, &rl);
        const u8 *tn = graph_entity_name(g, es[k].target_offset, &tnl);
        size_t cap = (size_t)(rl ? rl : 1) + (size_t)tnl + 48;
        rows[k] = (char *)malloc(cap);
        snprintf(rows[k], cap, "%.*s|%u|%.*s|%llu", (int)(rb ? rl : 0), rb ? (const char *)rb : "",
                 es[k].direction, (int)tnl, tn ? (const char *)tn : "",
                 (unsigned long long)es[k].mtime);
    }
    free(es);
    qsort(rows, ec, sizeof *rows, v4i_cmp_row);
    return rows;
}
static char **v4i_edge_rows_g4(graph4_t *g, u32 eid, u32 *n_out) {
    u32 ec = g4_edge_count(g, eid);
    *n_out = ec;
    if (!ec) return NULL;
    g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
    char **rows = (char **)malloc((size_t)ec * sizeof *rows);
    if (!es || !rows) { free(es); free(rows); *n_out = 0; return NULL; }
    g4_edges(g, eid, es, ec);
    for (u32 k = 0; k < ec; k++) {
        u16 rl = 0, tnl = 0;
        const u8 *rb = g4_str(g, es[k].rel_sid, &rl);
        const u8 *tn = NULL;
        g4_entity_t te;
        if (g4_read_entity(g, es[k].target_eid, &te)) tn = g4_str(g, te.name_sid, &tnl);
        size_t cap = (size_t)(rl ? rl : 1) + (size_t)tnl + 48;
        rows[k] = (char *)malloc(cap);
        snprintf(rows[k], cap, "%.*s|%u|%.*s|%llu", (int)(rb ? rl : 0), rb ? (const char *)rb : "",
                 es[k].direction, (int)tnl, tn ? (const char *)tn : "",
                 (unsigned long long)es[k].mtime);
    }
    free(es);
    qsort(rows, ec, sizeof *rows, v4i_cmp_row);
    return rows;
}
static void v4i_free_rows(char **rows, u32 n) {
    if (!rows) return;
    for (u32 i = 0; i < n; i++) free(rows[i]);
    free(rows);
}

/* ---------------------------------------------------------------- validate */

static void v4i_validate(graph_t *src, stringtable_t *src_st, graph4_t *dst,
                         v4i_report_t *rep) {
    u32 nev = graph_entity_count(src);
    if ((u64)nev != rep->entities)
        v4i_mismatch(rep, "entity count: src %u, imported %llu", nev, (unsigned long long)rep->entities);
    if (graph_relation_count(src) != g4_relation_count(dst))
        v4i_mismatch(rep, "relation count: src %u vs dst %u",
                     graph_relation_count(src), g4_relation_count(dst));
    /* Totals are persisted verbatim (META; ruling 4-i): expect the SRC's
     * all-time totals, orphaned visits included. */
    if (graph_structural_total(src) != g4_structural_total(dst))
        v4i_mismatch(rep, "structural total: src %llu vs dst %llu",
                     (unsigned long long)graph_structural_total(src),
                     (unsigned long long)g4_structural_total(dst));
    if (graph_walker_total(src) != g4_walker_total(dst))
        v4i_mismatch(rep, "walker total: src %llu vs dst %llu",
                     (unsigned long long)graph_walker_total(src),
                     (unsigned long long)g4_walker_total(dst));

    u64 *offs = (u64 *)malloc((size_t)(nev ? nev : 1) * 8);
    u32 got = graph_list_entities(src, offs, nev);
    for (u32 i = 0; i < got; i++) {
        entity_t e;
        graph_read_entity(src, offs[i], &e);
        u16 nl = 0;
        const u8 *nm = graph_entity_name(src, offs[i], &nl);
        if (!nm) { v4i_mismatch(rep, "src entity %u has no name", i); continue; }
        u32 eid = g4_lookup(dst, nm, nl);
        if (!eid) { v4i_mismatch(rep, "missing entity %.*s", (int)nl, nm); continue; }
        g4_entity_t eg;
        if (!g4_read_entity(dst, eid, &eg)) { v4i_mismatch(rep, "%.*s: unreadable", (int)nl, nm); continue; }
        u16 tl = 0;
        const u8 *ty = st_get(src_st, e.type_id, &tl);
        u16 tl2 = 0;
        const u8 *ty2 = g4_str(dst, eg.type_sid, &tl2);
        if (!ty || !ty2 || tl != tl2 || memcmp(ty, ty2, tl) != 0)
            v4i_mismatch(rep, "%.*s: type differs", (int)nl, nm);
        if (e.mtime != eg.mtime) v4i_mismatch(rep, "%.*s: mtime %llu vs %llu", (int)nl, nm,
                                              (unsigned long long)e.mtime, (unsigned long long)eg.mtime);
        if (e.obs_mtime != eg.obs_mtime) v4i_mismatch(rep, "%.*s: obs_mtime %llu vs %llu", (int)nl, nm,
                                                      (unsigned long long)e.obs_mtime, (unsigned long long)eg.obs_mtime);
        if (e.structural_visits != eg.structural_visits || e.walker_visits != eg.walker_visits)
            v4i_mismatch(rep, "%.*s: visits %llu/%llu vs %llu/%llu", (int)nl, nm,
                         (unsigned long long)e.structural_visits, (unsigned long long)e.walker_visits,
                         (unsigned long long)eg.structural_visits, (unsigned long long)eg.walker_visits);
        {
            double d = e.psi - eg.psi;
            if (d < 0) d = -d;
            if (d > 1e-12) v4i_mismatch(rep, "%.*s: psi %.17g vs %.17g", (int)nl, nm, e.psi, eg.psi);
        }
        if (e.obs_count != eg.obs_count) {
            v4i_mismatch(rep, "%.*s: obs_count %u vs %u", (int)nl, nm, e.obs_count, eg.obs_count);
        } else {
            if (e.obs_count >= 1) {
                u16 l1 = 0, l2 = 0;
                const u8 *o1 = st_get(src_st, e.obs0_id, &l1);
                const u8 *o2 = g4_str(dst, eg.obs0_sid, &l2);
                if (!o1 || !o2 || l1 != l2 || memcmp(o1, o2, l1) != 0)
                    v4i_mismatch(rep, "%.*s: obs0 differs", (int)nl, nm);
            }
            if (e.obs_count >= 2) {
                u16 l1 = 0, l2 = 0;
                const u8 *o1 = st_get(src_st, e.obs1_id, &l1);
                const u8 *o2 = g4_str(dst, eg.obs1_sid, &l2);
                if (!o1 || !o2 || l1 != l2 || memcmp(o1, o2, l1) != 0)
                    v4i_mismatch(rep, "%.*s: obs1 differs", (int)nl, nm);
            }
        }
        /* adjacency multiset incl. edge mtimes */
        u32 nva = 0, nga = 0;
        char **rv = v4i_edge_rows_v3(src, src_st, offs[i], &nva);
        char **rg = v4i_edge_rows_g4(dst, eid, &nga);
        if (nva != nga) {
            v4i_mismatch(rep, "%.*s: edge count %u vs %u", (int)nl, nm, nva, nga);
        } else {
            for (u32 k = 0; k < nva; k++)
                if (strcmp(rv[k], rg[k]) != 0) {
                    v4i_mismatch(rep, "%.*s: edge[%u] '%s' vs '%s'", (int)nl, nm, k, rv[k], rg[k]);
                    break;
                }
        }
        v4i_free_rows(rv, nva);
        v4i_free_rows(rg, nga);
    }
    free(offs);
}

/* ------------------------------------------------------------------ import */

int v4_import_run(const char *v3_graph, const char *v3_strings,
                  const char *dst_dir, v4i_report_t *rep) {
    memset(rep, 0, sizeof *rep);
    char mp[1024], gp[1024], sp[1024];
    snprintf(mp, sizeof mp, "%s/manifest.kb", dst_dir);
    snprintf(gp, sizeof gp, "%s/graph.kb", dst_dir);
    snprintf(sp, sizeof sp, "%s/strings.kb", dst_dir);
    if (access(mp, F_OK) == 0 || access(gp, F_OK) == 0 || access(sp, F_OK) == 0) {
        fprintf(stderr, "v4_import: refusing: %s already contains a v4 store "
                        "(manifest.kb/graph.kb/strings.kb)\n", dst_dir);
        return 1;
    }
    if (mkdir(dst_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "v4_import: cannot create %s: %s\n", dst_dir, strerror(errno));
        return 1;
    }

    stringtable_t *src_st = st_open(v3_strings, 1 << 20);
    graph_t *src = src_st ? graph_open(v3_graph, src_st, 1 << 20) : NULL;
    if (!src) { fprintf(stderr, "v4_import: cannot open v3 store %s / %s\n", v3_graph, v3_strings); return 1; }

    seg_io_t *mio = seg_io_posix_open(mp, 1);
    seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
    mstore_t *ms = (mio && sios[0] && sios[1]) ? mstore_create(mio, sios, 2, 4) : NULL;
    graph4_t *dst = ms ? graph4_open(ms) : NULL;
    if (!dst) { fprintf(stderr, "v4_import: cannot create v4 store in %s\n", dst_dir); return 1; }
    if (!mstore_txn_begin(ms)) { fprintf(stderr, "v4_import: txn begin failed\n"); return 1; }

    u32 nev = graph_entity_count(src);
    u64 *offs = (u64 *)malloc((size_t)(nev ? nev : 1) * 8);
    u32 got = graph_list_entities(src, offs, nev);
    fprintf(stderr, "v4_import: src entities=%u; phases: entities, relations, fields, commit, validate\n", got);
    fflush(stderr);

    double t_ent = v4i_now(), t_r = 0, t_w = 0, tb0, tb1;
    for (u32 i = 0; i < got; i++) {                        /* entities + obs */
        entity_t e;
        tb0 = v4i_now();
        graph_read_entity(src, offs[i], &e);
        u16 nl = 0, tl = 0;
        const u8 *nm = graph_entity_name(src, offs[i], &nl);
        const u8 *ty = st_get(src_st, e.type_id, &tl);
        tb1 = v4i_now(); t_r += tb1 - tb0;
        if (!nm || !ty) { v4i_mismatch(rep, "src entity %u unreadable", i); continue; }
        tb0 = v4i_now();
        u32 eid = g4_create_entity(dst, nm, nl, ty, tl, e.mtime);
        if (!eid) { v4i_mismatch(rep, "import create failed for %.*s", (int)nl, nm); continue; }
        if (e.obs_count >= 1) { u16 l = 0; const u8 *o = st_get(src_st, e.obs0_id, &l); if (o) g4_add_observation(dst, eid, o, l, e.obs_mtime); }
        if (e.obs_count >= 2) { u16 l = 0; const u8 *o = st_get(src_st, e.obs1_id, &l); if (o) g4_add_observation(dst, eid, o, l, e.obs_mtime); }
        tb1 = v4i_now(); t_w += tb1 - tb0;
        rep->entities++;
        if ((i + 1) % 25000 == 0) v4i_prog("entities", i + 1, got, t_ent);
    }
    fprintf(stderr, "v4_import: entities: v3read=%.1fs v4write=%.1fs wall=%.1fs\n",
            t_r, t_w, v4i_now() - t_ent);
    fflush(stderr);

    double t_rel = v4i_now(), tr_r = 0, tr_w = 0;
    for (u32 i = 0; i < got; i++) {                        /* relations */
        tb0 = v4i_now();
        u32 ec = graph_edge_count(src, offs[i]);
        double tw2 = 0;
        if (!ec) { tr_r += v4i_now() - tb0; continue; }
        adj_entry_t *es = (adj_entry_t *)malloc((size_t)ec * sizeof *es);
        if (!es) continue;
        graph_read_edges(src, offs[i], es, ec);
        u16 fnl = 0;
        const u8 *fn = graph_entity_name(src, offs[i], &fnl);
        tb1 = v4i_now(); tr_r += tb1 - tb0;
        u32 fe = fn ? g4_lookup(dst, fn, fnl) : 0;
        for (u32 k = 0; k < ec && fe; k++) {
            if (es[k].direction != DIR_FORWARD) continue;  /* import each logical relation once */
            u16 tnl = 0, rl = 0;
            const u8 *tn = graph_entity_name(src, es[k].target_offset, &tnl);
            const u8 *rt = st_get(src_st, es[k].rel_type_id, &rl);
            if (!tn || !rt) continue;
            u32 te = g4_lookup(dst, tn, tnl);
            if (!te) { v4i_mismatch(rep, "relation target %.*s missing", (int)tnl, tn); continue; }
            rep->relations_in++;
            if (g4_create_relation(dst, fe, te, rt, rl, es[k].mtime))
                rep->relations_created++;
            else
                rep->relations_dedup++;
        }
        free(es);
        tr_w += v4i_now() - tb1;
        if ((i + 1) % 25000 == 0) v4i_prog("relations", i + 1, got, t_rel);
    }
    fprintf(stderr, "v4_import: relations: v3read=%.1fs wall=%.1fs\n", tr_r, v4i_now() - t_rel);
    fflush(stderr);

    u64 live_sv = 0, live_wv = 0;
    double t_fld = v4i_now(), tf_r = 0, tf_w = 0;
    for (u32 i = 0; i < got; i++) {                        /* restore fields LAST */
        entity_t e;
        tb0 = v4i_now();
        graph_read_entity(src, offs[i], &e);
        live_sv += e.structural_visits;
        live_wv += e.walker_visits;
        u16 nl = 0;
        const u8 *nm = graph_entity_name(src, offs[i], &nl);
        tb1 = v4i_now(); tf_r += tb1 - tb0;
        if (!nm) continue;
        u32 eid = g4_lookup(dst, nm, nl);
        if (eid)
            g4_set_entity_fields(dst, eid, e.mtime, e.obs_mtime,
                                 e.structural_visits, e.walker_visits, e.psi);
        tf_w += v4i_now() - tb1;
        if ((i + 1) % 25000 == 0) v4i_prog("fields", i + 1, got, t_fld);
    }
    fprintf(stderr, "v4_import: fields: v3read=%.1fs v4write=%.1fs wall=%.1fs\n",
            tf_r, tf_w, v4i_now() - t_fld);
    fflush(stderr);
    /* v4 totals are persisted store state (META; ruling 4-i): restore the
     * source's all-time totals verbatim, orphaned visits included. */
    g4_set_totals(dst, graph_structural_total(src), graph_walker_total(src));
    {
        u64 stv = graph_structural_total(src), wv = graph_walker_total(src);
        rep->orphaned_sv = stv > live_sv ? stv - live_sv : 0;
        rep->orphaned_wv = wv > live_wv ? wv - live_wv : 0;
    }

    double t_cmt = v4i_now();
    int commit_ok = mstore_txn_commit(ms);
    fprintf(stderr, "v4_import: commit: %.1fs\n", v4i_now() - t_cmt);
    fflush(stderr);
    free(offs);
    if (!commit_ok) { fprintf(stderr, "v4_import: commit failed\n"); return 1; }

    /* ---- validate: reopen the written store and compare field-for-field */
    graph4_close(dst);
    mstore_close(ms);
    seg_io_t *mio2 = seg_io_posix_open(mp, 1);
    seg_io_t *sios2[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
    mstore_t *ms2 = (mio2 && sios2[0] && sios2[1]) ? mstore_open(mio2, sios2, 2) : NULL;
    graph4_t *dst2 = ms2 ? graph4_open(ms2) : NULL;
    if (!dst2) { fprintf(stderr, "v4_import: cannot reopen %s for validation\n", dst_dir); return 1; }
    double t_val = v4i_now();
    v4i_validate(src, src_st, dst2, rep);
    fprintf(stderr, "v4_import: validate: %.1fs (mismatches=%llu)\n",
            v4i_now() - t_val, (unsigned long long)rep->mismatches);
    fflush(stderr);
    graph4_close(dst2);
    mstore_close(ms2);
    graph_close(src);
    st_close(src_st);

    rep->ok = rep->mismatches == 0;
    return rep->ok ? 0 : 1;
}

#ifndef V4I_NO_MAIN
int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <v3_base> <v4_dir>\n"
                        "  v3_base: <base>.graph + <base>.strings (READ-ONLY; use a quiesced copy)\n"
                        "  v4_dir:  created if absent; refused if it already holds a v4 store\n",
                argv[0]);
        return 2;
    }
    char base[1024];
    snprintf(base, sizeof base, "%s", argv[1]);
    char *dot = strrchr(base, '.');
    if (dot && (strcmp(dot, ".graph") == 0 || strcmp(dot, ".strings") == 0)) *dot = 0;
    char v3g[1100], v3s[1100];
    snprintf(v3g, sizeof v3g, "%s.graph", base);
    snprintf(v3s, sizeof v3s, "%s.strings", base);
    if (access(v3g, F_OK) != 0 || access(v3s, F_OK) != 0) {
        fprintf(stderr, "v4_import: missing %s and/or %s\n", v3g, v3s);
        return 2;
    }
    v4i_report_t rep;
    int rc = v4_import_run(v3g, v3s, argv[2], &rep);
    printf("v4_import: entities=%llu relations_in=%llu created=%llu dedup=%llu "
           "orphaned_visits=%llu/%llu mismatches=%llu %s\n",
           (unsigned long long)rep.entities, (unsigned long long)rep.relations_in,
           (unsigned long long)rep.relations_created, (unsigned long long)rep.relations_dedup,
           (unsigned long long)rep.orphaned_sv, (unsigned long long)rep.orphaned_wv,
           (unsigned long long)rep.mismatches, rc ? "FAILED" : "ok");
    return rc ? 1 : 0;
}
#endif
