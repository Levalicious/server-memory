/*
 * Unified graph store harness: fuzz create/delete entity+relation + observations,
 * validate name-index/lookup + scan ops every so often, then tear down and assert
 * the string table empties (any name/type/relType/observation leak shows up here).
 * Run under ASan+UBSan.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "stringtable.h"
#include "graph.h"

static int fails = 0;
#define CHECK(c, m) do { if (!(c)) { printf("  FAIL: %s\n", m); fails++; } else printf("  ok:   %s\n", m); } while (0)

static u64 rs = 0xcafef00d1234ull;
static u64 xs(void) { u64 x = rs; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return rs = x; }

#define NENT 400
#define NRT  8

typedef struct { char name[16]; char type[16]; u64 off; int alive; } Ent;
typedef struct { int from, to, rt; } Rel;

static Ent ents[NENT];
static u32 obsn[NENT];               /* observation count per live entity (0..2) */
static Rel *rels; static size_t nrel, relcap;

static void rel_push(int f, int t, int r) {
    if (nrel == relcap) { relcap = relcap ? relcap * 2 : 1024; rels = realloc(rels, relcap * sizeof(Rel)); }
    rels[nrel++] = (Rel){ f, t, r };
}
static int rel_find(int f, int t, int r) {
    for (size_t i = 0; i < nrel; i++) if (rels[i].from == f && rels[i].to == t && rels[i].rt == r) return (int)i;
    return -1;
}
static void rel_del_idx(size_t i) { rels[i] = rels[--nrel]; }
static void rel_drop_incident(int e) {
    for (size_t i = 0; i < nrel; ) { if (rels[i].from == e || rels[i].to == e) rel_del_idx(i); else i++; }
}
static void rtname(int r, char *buf) { snprintf(buf, 16, "rel-%d", r); }

static graph_t *gr; static stringtable_t *st;

static int cmp_u64t(const void *a, const void *b) { u64 x = *(const u64 *)a, y = *(const u64 *)b; return (x > y) - (x < y); }

static int count_alive(void) { int c = 0; for (int i = 0; i < NENT; i++) if (ents[i].alive) c++; return c; }

static int validate(void) {
    int bad = 0;
    if ((int)graph_entity_count(gr) != count_alive()) bad++;
    for (int i = 0; i < NENT; i++) {
        u64 found = graph_lookup(gr, (const u8 *)ents[i].name, (u16)strlen(ents[i].name));
        if (ents[i].alive) { if (found != ents[i].off) bad++; }
        else { if (found != 0) bad++; }
    }
    return bad;
}

static int pick_alive(void) {
    int n = count_alive(); if (!n) return -1;
    int k = (int)(xs() % (u64)n);
    for (int i = 0; i < NENT; i++) if (ents[i].alive && k-- == 0) return i;
    return -1;
}

int main(void) {
    const char *gp = "/tmp/graph_test.dat", *sp = "/tmp/graph_test.strings.dat";
    unlink(gp); unlink(sp);
    st = st_open(sp, 1u << 16);
    gr = graph_open(gp, st, 1u << 16);
    if (!st || !gr) { printf("open failed\n"); return 2; }

    for (int i = 0; i < NENT; i++) {
        snprintf(ents[i].name, 16, "ent-%d", i);
        snprintf(ents[i].type, 16, "type-%d", i % 16);
        ents[i].off = 0; ents[i].alive = 0; obsn[i] = 0;
    }

    size_t bad = 0;
    char rb[16];
    for (size_t it = 0; it < 200000; it++) {
        u32 op = (u32)(xs() % 100);
        if (op < 42) {                                   /* create entity */
            int i = (int)(xs() % NENT);
            u64 off = graph_create_entity(gr, (const u8 *)ents[i].name, (u16)strlen(ents[i].name),
                                          (const u8 *)ents[i].type, (u16)strlen(ents[i].type), it);
            if (ents[i].alive) { if (off != ents[i].off) bad++; }
            else { ents[i].alive = 1; ents[i].off = off; obsn[i] = 0; }
        } else if (op < 56) {                            /* delete entity */
            int i = pick_alive(); if (i < 0) continue;
            rel_drop_incident(i);
            graph_delete_entity(gr, ents[i].off);
            ents[i].alive = 0; obsn[i] = 0;
        } else if (op < 64) {                            /* add observation */
            int i = pick_alive(); if (i < 0) continue;
            if (obsn[i] < 2) {
                char ob[24]; int l = snprintf(ob, sizeof ob, "o-%d-%u", i, obsn[i]);
                graph_add_observation(gr, ents[i].off, (const u8 *)ob, (u16)l, it);
                obsn[i]++;
            }
        } else if (op < 88) {                            /* create relation */
            int a = pick_alive(), b = pick_alive();
            if (a < 0 || b < 0 || a == b) continue;
            int r = (int)(xs() % NRT);
            if (rel_find(a, b, r) < 0) {
                rtname(r, rb);
                graph_create_relation(gr, ents[a].off, ents[b].off, (const u8 *)rb, (u16)strlen(rb), it);
                rel_push(a, b, r);
            }
        } else {                                         /* delete relation */
            if (nrel == 0) continue;
            size_t k = xs() % nrel;
            Rel rr = rels[k]; rtname(rr.rt, rb);
            graph_delete_relation(gr, ents[rr.from].off, ents[rr.to].off, (const u8 *)rb, (u16)strlen(rb));
            rel_del_idx(k);
        }
        if ((it & 0xFFF) == 0 && validate() != 0) bad++;
    }
    CHECK(bad == 0, "per-op model: entity count + name-index lookups consistent");

    /* scan-op spot checks against the model */
    CHECK((int)graph_entity_count(gr) == count_alive(), "entity_count == model");
    CHECK(graph_relation_count(gr) == nrel, "relation_count == model relation set size");
    {
        int model = 0; for (int i = 0; i < NENT; i++) if (ents[i].alive && (i % 16) == 3) model++;
        u64 *buf = malloc((size_t)NENT * sizeof(u64));
        u32 got = graph_entities_by_type(gr, (const u8 *)"type-3", 6, buf, NENT);
        CHECK((int)got == model, "entities_by_type == model");
        free(buf);
    }
    {
        u32 *tb = malloc(64 * sizeof(u32));
        u32 net = graph_entity_types(gr, tb, 64);
        u32 nrt2 = graph_relation_types(gr, tb, 64);
        printf("  distinct entity_types=%u relation_types=%u orphaned=%u\n",
               net, nrt2, graph_orphaned(gr, (u64 *)tb, 0));
        free(tb);
    }
    printf("  mid-run: live=%d relations=%zu strings=%u\n", count_alive(), nrel, st_count(st));

    /* traversal: get_neighbors (fwd depth-1) vs model, find_path on a direct edge */
    {
        int nb_ok = 1, samples = 0;
        for (int i = 0; i < NENT && samples < 40; i++) {
            if (!ents[i].alive) continue;
            samples++;
            u64 mset[NENT]; u32 mn = 0;
            for (size_t k = 0; k < nrel; k++) if (rels[k].from == i) {
                u64 o = ents[rels[k].to].off; int dup = 0;
                for (u32 j = 0; j < mn; j++) if (mset[j] == o) { dup = 1; break; }
                if (!dup) mset[mn++] = o;
            }
            u64 gset[NENT]; u32 gn = graph_neighbors(gr, ents[i].off, 1, DIR_FORWARD, gset, NENT);
            if (gn != mn) { nb_ok = 0; continue; }
            qsort(mset, mn, 8, cmp_u64t); qsort(gset, gn, 8, cmp_u64t);
            for (u32 j = 0; j < mn; j++) if (mset[j] != gset[j]) { nb_ok = 0; break; }
        }
        CHECK(nb_ok, "get_neighbors (fwd depth-1) matches model over 40 samples");

        int fp_ok = 1;
        if (nrel > 0) {
            Rel rr = rels[0];
            u64 path[64];
            u32 pl = graph_find_path(gr, ents[rr.from].off, ents[rr.to].off, 4, DIR_FORWARD, path, 64);
            if (!(pl == 2 && path[0] == ents[rr.from].off && path[1] == ents[rr.to].off)) fp_ok = 0;
            if (graph_find_path(gr, ents[rr.from].off, ents[rr.from].off, 4, DIR_ANY, path, 64) != 1) fp_ok = 0;
        }
        CHECK(fp_ok, "find_path: direct edge -> len 2, self -> len 1");
    }

    /* search: POSIX ERE over name/type/obs, full result set */
    {
        u64 *sb = malloc((size_t)NENT * sizeof(u64));
        u32 s1 = graph_search(gr, "^ent-7$", sb, NENT);
        CHECK((int)s1 == (ents[7].alive ? 1 : 0), "search ^ent-7$ matches the name exactly");
        int model = 0; for (int i = 0; i < NENT; i++) if (ents[i].alive && (i % 16) == 3) model++;
        u32 s2 = graph_search(gr, "type-3", sb, NENT);
        CHECK((int)s2 == model, "search type-3 (type field) == model");
        free(sb);
    }

    /* ranking: structural sampling, MERW psi, random walk, walker counting */
    {
        graph_seed_rng(12345);
        u32 sv = graph_structural_sample(gr, 1, 0.85);
        CHECK(sv > 0 && graph_structural_total(gr) == sv, "structural sample: visits recorded == structural_total");

        u32 it = graph_compute_merw_psi(gr, 0.85, 200, 1e-8);
        CHECK(it > 0 && it <= 200, "MERW psi power-iteration converged");
        double psisum = 0;
        for (int i = 0; i < NENT; i++) if (ents[i].alive) psisum += graph_get_psi(gr, ents[i].off);
        CHECK(psisum > 0, "MERW psi populated across live entities");

        u64 wt0 = graph_walker_total(gr);
        int wi = pick_alive();
        if (wi >= 0) {
            graph_inc_walker_visit(gr, ents[wi].off);
            CHECK(graph_walker_total(gr) == wt0 + 1, "walker visit bumps walker_total");
            CHECK(graph_walker_rank(gr, ents[wi].off) > 0.0, "walker rank > 0 after a visit");
        }

        if (nrel > 0) {
            u64 path[16];
            u32 pl = graph_random_walk(gr, ents[rels[0].from].off, 5, DIR_FORWARD, 1, 999, path, 16);
            CHECK(pl >= 1 && pl <= 6 && path[0] == ents[rels[0].from].off, "random_walk: valid path (start + <=depth steps)");
        }
    }

    /* validate_graph: obs limits + dangling edges */
    {
        u64 *vo = malloc((size_t)NENT * 8); u8 *vc = malloc(NENT), *vov = malloc(NENT);
        u64 *ds = malloc((size_t)NENT * 8), *dt = malloc((size_t)NENT * 8);
        CHECK(graph_validate_obs(gr, vo, vc, vov, NENT) == 0, "validate: no obs violations in fuzz data");
        CHECK(graph_validate_dangling(gr, ds, dt, NENT) == 0, "validate: no dangling edges (integrity holds)");
        u64 t = graph_create_entity(gr, (const u8 *)"validate-test", 13, (const u8 *)"vtype", 5, 999);
        char big[160]; memset(big, 'x', 150); big[150] = 0;
        graph_add_observation(gr, t, (const u8 *)big, 150, 999);
        CHECK(graph_validate_obs(gr, vo, vc, vov, NENT) == 1, "validate: flags the >140-byte observation");
        graph_delete_entity(gr, t);
        CHECK(graph_validate_obs(gr, vo, vc, vov, NENT) == 0, "validate: clean after removing it");
        free(vo); free(vc); free(vov); free(ds); free(dt);
    }

    /* teardown: delete all relations, then all entities -> string table must empty */
    while (nrel > 0) {
        Rel rr = rels[nrel - 1]; rtname(rr.rt, rb);
        graph_delete_relation(gr, ents[rr.from].off, ents[rr.to].off, (const u8 *)rb, (u16)strlen(rb));
        nrel--;
    }
    for (int i = 0; i < NENT; i++) if (ents[i].alive) { graph_delete_entity(gr, ents[i].off); ents[i].alive = 0; }

    CHECK(graph_entity_count(gr) == 0, "all entities deleted");
    CHECK(st_count(st) == 0, "string table empty after teardown (no name/type/relType/observation leak)");
    printf("  final strings=%u entity_count=%u\n", st_count(st), graph_entity_count(gr));

    graph_close(gr);
    st_close(st);
    free(rels);
    printf(fails ? "\nFAILED (%d)\n" : "\nALL PASS\n", fails);
    return fails ? 1 : 0;
}
