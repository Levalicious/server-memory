/*
 * test_parity.c — v3 (graph.c on memfile) vs graph4 (on segstores), one
 * seeded op stream, observable behavior diffed BY NAME after every round.
 *
 * This is phase 3's gate (Principle_V3TestsUnmodified transposed to C): the
 * v3 engine is the behavioral oracle; any divergence is a graph4 bug until
 * proven otherwise (D_BlameOurImplFirst).
 *
 * Compared per round: entity/relation counts, per-entity type + obs sets,
 * edge multisets (name,rel,dir,target), lookups, neighbors name-sets at
 * depths 1..3 x {ANY,FWD,BWD}, find_path lengths, search result name-sets
 * over a pattern battery, entities_by_type, entity/relation type string
 * sets, orphan sets. Walks: exact path parity (same seed) while every
 * degree < 128 (identical candidate order); validity otherwise.
 */
#include "graph.h"
#include "segstore.h"
#include "regex.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static u64 rng_state = 0x50415249ull;   /* "PARI" */
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

enum { NENT = 80, NREL = 4, ROUNDS = 30, MUTS = 14 };
static const char *RELS[NREL] = { "SUPPORTS", "REFINES", "PART_OF", "CHALLENGES" };
static const char *TYPES[3] = { "Insight", "Finding", "Decision" };

typedef struct {
    graph_t *v3; stringtable_t *v3st;
    mstore_t *ms; graph4_t *g4;
    char name[NENT][24];
    int live[NENT];
    u64 off[NENT];        /* v3 offsets */
    u32 eid[NENT];        /* g4 eids */
    int obs[NENT];        /* obs count */
    int edge[NENT][NENT][NREL];
} world_t;

/* ---- name-set compare helpers (order-independent) ---- */

static void nm_of_v3(world_t *w, u64 off, char *buf) {
    u16 len = 0;
    const u8 *b = graph_entity_name(w->v3, off, &len);
    assert(b && len < 24);
    memcpy(buf, b, len); buf[len] = 0;
}
static void nm_of_g4(world_t *w, u32 eid, char *buf) {
    g4_entity_t e;
    assert(g4_read_entity(w->g4, eid, &e));
    u16 len = 0;
    const u8 *b = g4_str(w->g4, e.name_sid, &len);
    assert(b && len < 24);
    memcpy(buf, b, len); buf[len] = 0;
}

static int cmp_str(const void *a, const void *b) { return strcmp((const char *)a, (const char *)b); }

/* collect names into sorted fixed rows, compare */
typedef struct { char rows[NENT + 8][24]; u32 n; } nameset_t;
static void ns_sort(nameset_t *s) { qsort(s->rows, s->n, 24, cmp_str); }
static void ns_assert_eq(const nameset_t *a, const nameset_t *b, const char *what) {
    if (a->n != b->n) { fprintf(stderr, "PARITY[%s]: count %u vs %u\n", what, a->n, b->n); abort(); }
    for (u32 i = 0; i < a->n; i++)
        if (strcmp(a->rows[i], b->rows[i]) != 0) {
            fprintf(stderr, "PARITY[%s]: row %u '%s' vs '%s'\n", what, i, a->rows[i], b->rows[i]);
            abort();
        }
}

/* ---- comparison passes ---- */

static void compare_entities(world_t *w) {
    assert(graph_entity_count(w->v3) == g4_entity_count(w->g4));
    assert(graph_relation_count(w->v3) == g4_relation_count(w->g4));
    for (u32 i = 0; i < NENT; i++) {
        if (!w->live[i]) {
            assert(graph_lookup(w->v3, (const u8 *)w->name[i], (u16)strlen(w->name[i])) == 0);
            assert(g4_lookup(w->g4, (const u8 *)w->name[i], (u16)strlen(w->name[i])) == 0);
            continue;
        }
        u64 off = graph_lookup(w->v3, (const u8 *)w->name[i], (u16)strlen(w->name[i]));
        u32 eid = g4_lookup(w->g4, (const u8 *)w->name[i], (u16)strlen(w->name[i]));
        assert(off == w->off[i] && eid == w->eid[i]);
        entity_t ev; g4_entity_t eg;
        graph_read_entity(w->v3, off, &ev);
        assert(g4_read_entity(w->g4, eid, &eg));
        assert(ev.obs_count == eg.obs_count);
        assert(ev.mtime == eg.mtime);
        /* type strings equal */
        u16 l1 = 0, l2 = 0;
        const u8 *t1 = st_get(w->v3st, ev.type_id, &l1);
        const u8 *t2 = g4_str(w->g4, eg.type_sid, &l2);
        assert(t1 && t2 && l1 == l2 && memcmp(t1, t2, l1) == 0);
        /* obs strings equal as a set (order preserved by both: slot0/slot1) */
        if (ev.obs_count >= 1) {
            t1 = st_get(w->v3st, ev.obs0_id, &l1);
            t2 = g4_str(w->g4, eg.obs0_sid, &l2);
            assert(t1 && t2 && l1 == l2 && memcmp(t1, t2, l1) == 0);
        }
        if (ev.obs_count >= 2) {
            t1 = st_get(w->v3st, ev.obs1_id, &l1);
            t2 = g4_str(w->g4, eg.obs1_sid, &l2);
            assert(t1 && t2 && l1 == l2 && memcmp(t1, t2, l1) == 0);
        }
        /* edge multiset as sorted "rel|dir|target" strings */
        u32 ecv = graph_edge_count(w->v3, off), ecg = g4_edge_count(w->g4, eid);
        assert(ecv == ecg);
        if (ecv) {
            adj_entry_t *es = (adj_entry_t *)malloc((size_t)ecv * sizeof *es);
            g4_edge_t   *eg2 = (g4_edge_t *)malloc((size_t)ecg * sizeof *eg2);
            graph_read_edges(w->v3, off, es, ecv);
            g4_edges(w->g4, eid, eg2, ecg);
            nameset_t a, b; a.n = b.n = 0;
            char tn[24];
            for (u32 k = 0; k < ecv; k++) {
                u16 rl = 0; const u8 *rb = st_get(w->v3st, es[k].rel_type_id, &rl);
                nm_of_v3(w, es[k].target_offset, tn);
                snprintf(a.rows[a.n++], 24, "%.*s|%u|%s", rl, rb, es[k].direction, tn);
            }
            for (u32 k = 0; k < ecg; k++) {
                u16 rl = 0; const u8 *rb = g4_str(w->g4, eg2[k].rel_sid, &rl);
                nm_of_g4(w, eg2[k].target_eid, tn);
                snprintf(b.rows[b.n++], 24, "%.*s|%u|%s", rl, rb, eg2[k].direction, tn);
            }
            ns_sort(&a); ns_sort(&b);
            ns_assert_eq(&a, &b, "edges");
            free(es); free(eg2);
        }
    }
}

static void compare_traversal(world_t *w) {
    static const u32 DIRS[3] = { DIR_ANY, DIR_FORWARD, DIR_BACKWARD };
    for (u32 trial = 0; trial < 12; trial++) {
        u32 i = (u32)(rng() % NENT);
        if (!w->live[i]) continue;
        u32 dir = DIRS[rng() % 3];
        u32 depth = 1 + (u32)(rng() % 3);
        u64 ov[NENT + 8]; u32 og[NENT + 8];
        u32 nv = graph_neighbors(w->v3, w->off[i], depth, dir, ov, NENT + 8);
        u32 ng = g4_neighbors(w->g4, w->eid[i], depth, dir, og, NENT + 8);
        assert(nv == ng);
        nameset_t a, b; a.n = b.n = 0;
        for (u32 k = 0; k < nv && k < NENT + 8; k++) nm_of_v3(w, ov[k], a.rows[a.n++]);
        for (u32 k = 0; k < ng && k < NENT + 8; k++) nm_of_g4(w, og[k], b.rows[b.n++]);
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "neighbors");
        /* find_path length parity */
        u32 j = (u32)(rng() % NENT);
        if (!w->live[j]) continue;
        u64 pv[64]; u32 pg[64];
        u32 lv = graph_find_path(w->v3, w->off[i], w->off[j], 12, dir, pv, 64);
        u32 lg = g4_find_path(w->g4, w->eid[i], w->eid[j], 12, dir, pg, 64);
        assert((lv == 0) == (lg == 0));
        if (lv) assert(lv == lg);              /* shortest => equal length */
    }
}

static void compare_queries(world_t *w) {
    static const char *pats[] = { "Ent_", "Ent_1[0-9]", "alpha|beta", "^Ent_7.*a$", "obs_", "zzz_none" };
    for (u32 p = 0; p < sizeof pats / sizeof *pats; p++) {
        u64 ov[NENT + 8]; u32 og[NENT + 8];
        u32 nv = graph_search(w->v3, pats[p], ov, NENT + 8);
        u32 ng = g4_search(w->g4, pats[p], og, NENT + 8);
        assert(nv == ng);
        nameset_t a, b; a.n = b.n = 0;
        for (u32 k = 0; k < nv; k++) nm_of_v3(w, ov[k], a.rows[a.n++]);
        for (u32 k = 0; k < ng; k++) nm_of_g4(w, og[k], b.rows[b.n++]);
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "search");
    }
    for (u32 t = 0; t < 3; t++) {
        u64 ov[NENT + 8]; u32 og[NENT + 8];
        u32 nv = graph_entities_by_type(w->v3, (const u8 *)TYPES[t], (u16)strlen(TYPES[t]), ov, NENT + 8);
        u32 ng = g4_entities_by_type(w->g4, (const u8 *)TYPES[t], (u16)strlen(TYPES[t]), og, NENT + 8);
        assert(nv == ng);
        nameset_t a, b; a.n = b.n = 0;
        for (u32 k = 0; k < nv; k++) nm_of_v3(w, ov[k], a.rows[a.n++]);
        for (u32 k = 0; k < ng; k++) nm_of_g4(w, og[k], b.rows[b.n++]);
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "by_type");
    }
    /* distinct type/rel string sets */
    {
        u32 sv[16]; u32 sg[16];
        u32 nv = graph_entity_types(w->v3, sv, 16);
        u32 ng = g4_entity_types(w->g4, sg, 16);
        assert(nv == ng);
        nameset_t a, b; a.n = b.n = 0;
        for (u32 k = 0; k < nv; k++) { u16 l; const u8 *s = st_get(w->v3st, sv[k], &l); snprintf(a.rows[a.n++], 24, "%.*s", l, s); }
        for (u32 k = 0; k < ng; k++) { u16 l; const u8 *s = g4_str(w->g4, sg[k], &l); snprintf(b.rows[b.n++], 24, "%.*s", l, s); }
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "entity_types");
        nv = graph_relation_types(w->v3, sv, 16);
        ng = g4_relation_types(w->g4, sg, 16);
        assert(nv == ng);
        a.n = b.n = 0;
        for (u32 k = 0; k < nv; k++) { u16 l; const u8 *s = st_get(w->v3st, sv[k], &l); snprintf(a.rows[a.n++], 24, "%.*s", l, s); }
        for (u32 k = 0; k < ng; k++) { u16 l; const u8 *s = g4_str(w->g4, sg[k], &l); snprintf(b.rows[b.n++], 24, "%.*s", l, s); }
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "relation_types");
    }
    /* orphans */
    {
        u64 ov[NENT + 8]; u32 og[NENT + 8];
        u32 nv = graph_orphaned(w->v3, ov, NENT + 8);
        u32 ng = g4_orphaned(w->g4, og, NENT + 8);
        assert(nv == ng);
        nameset_t a, b; a.n = b.n = 0;
        for (u32 k = 0; k < nv; k++) nm_of_v3(w, ov[k], a.rows[a.n++]);
        for (u32 k = 0; k < ng; k++) nm_of_g4(w, og[k], b.rows[b.n++]);
        ns_sort(&a); ns_sort(&b);
        ns_assert_eq(&a, &b, "orphaned");
    }
}

static void compare_walks(world_t *w) {
    /* all degrees < 128 in this world => candidate order matches => exact
     * path parity under the same explicit seed */
    for (u32 t = 0; t < 8; t++) {
        u32 i = (u32)(rng() % NENT);
        if (!w->live[i]) continue;
        u64 seed = rng() | 1;
        u32 dir = (u32)(rng() % 2) ? DIR_ANY : DIR_FORWARD;
        u64 pv[24]; u32 pg[24];
        u32 nv = graph_random_walk(w->v3, w->off[i], 10, dir, 0, seed, 0, pv, 24, NULL);
        u32 ng = g4_random_walk(w->g4, w->eid[i], 10, dir, 0, seed, 0, pg, 24, NULL);
        assert(nv == ng);
        char n1[24], n2[24];
        for (u32 k = 0; k < nv; k++) {
            nm_of_v3(w, pv[k], n1);
            nm_of_g4(w, pg[k], n2);
            assert(strcmp(n1, n2) == 0);       /* EXACT step parity */
        }
    }
}

int main(void) {
    printf("test_parity: v3 oracle vs graph4, %u rounds x %u muts\n", ROUNDS, MUTS);
    char v3g[] = "/tmp/parity_v3g_XXXXXX", v3s[] = "/tmp/parity_v3s_XXXXXX";
    int f;
    f = mkstemp(v3g); assert(f >= 0); close(f); unlink(v3g);
    f = mkstemp(v3s); assert(f >= 0); close(f); unlink(v3s);

    world_t *w = (world_t *)calloc(1, sizeof *w);
    assert(w);
    w->v3st = st_open(v3s, 1 << 20);
    w->v3 = graph_open(v3g, w->v3st, 1 << 20);
    assert(w->v3st && w->v3);
    seg_io_t *sios[2] = { seg_io_sim_open(), seg_io_sim_open() };
    w->ms = mstore_create(seg_io_sim_open(), sios, 2, 2);
    w->g4 = graph4_open(w->ms);
    assert(w->ms && w->g4);
    assert(mstore_txn_begin(w->ms));

    for (u32 i = 0; i < NENT; i++) {
        const char *fl[3] = { "alpha", "beta", "gamma" };
        snprintf(w->name[i], sizeof w->name[i], "Ent_%u_%s", i, fl[i % 3]);
    }

    for (u32 r = 0; r < ROUNDS; r++) {
        for (u32 m = 0; m < MUTS; m++) {
            u32 i = (u32)(rng() % NENT), j = (u32)(rng() % NENT), rr = (u32)(rng() % NREL);
            u32 kind = (u32)(rng() % 100);
            u16 nl = (u16)strlen(w->name[i]);
            if (kind < 30) {                                 /* create entity */
                const char *ty = TYPES[i % 3];
                u64 off = graph_create_entity(w->v3, (const u8 *)w->name[i], nl,
                                              (const u8 *)ty, (u16)strlen(ty), r * 100 + m);
                u32 eid = g4_create_entity(w->g4, (const u8 *)w->name[i], nl,
                                           (const u8 *)ty, (u16)strlen(ty), r * 100 + m);
                assert(off && eid);
                if (!w->live[i]) { w->live[i] = 1; w->off[i] = off; w->eid[i] = eid; w->obs[i] = 0; }
                else assert(off == w->off[i] && eid == w->eid[i]);
            } else if (kind < 45 && w->live[i]) {            /* delete entity */
                assert(graph_delete_entity(w->v3, w->off[i]) == 1);
                assert(g4_delete_entity(w->g4, w->eid[i]) == 1);
                w->live[i] = 0;
                for (u32 k = 0; k < NENT; k++)
                    for (u32 x = 0; x < NREL; x++) w->edge[i][k][x] = w->edge[k][i][x] = 0;
            } else if (kind < 65 && w->live[i] && w->live[j] && i != j) {  /* relation */
                int rv = graph_create_relation(w->v3, w->off[i], w->off[j],
                                               (const u8 *)RELS[rr], (u16)strlen(RELS[rr]), r);
                int rg = g4_create_relation(w->g4, w->eid[i], w->eid[j],
                                            (const u8 *)RELS[rr], (u16)strlen(RELS[rr]), r);
                assert(rv == rg);
                if (rv) w->edge[i][j][rr] = 1;
            } else if (kind < 78 && w->live[i] && w->live[j]) {  /* delete relation */
                int rv = graph_delete_relation(w->v3, w->off[i], w->off[j],
                                               (const u8 *)RELS[rr], (u16)strlen(RELS[rr]));
                int rg = g4_delete_relation(w->g4, w->eid[i], w->eid[j],
                                            (const u8 *)RELS[rr], (u16)strlen(RELS[rr]));
                assert(rv == rg);
                if (rv) w->edge[i][j][rr] = 0;
            } else if (kind < 90 && w->live[i]) {            /* add obs */
                char ob[32];
                snprintf(ob, sizeof ob, "obs_%u_%u", i, w->obs[i]);
                int rv = graph_add_observation(w->v3, w->off[i], (const u8 *)ob, (u16)strlen(ob), r);
                int rg = g4_add_observation(w->g4, w->eid[i], (const u8 *)ob, (u16)strlen(ob), r);
                assert(rv == rg);
                if (rv) w->obs[i]++;
            } else if (w->live[i] && w->obs[i]) {            /* remove obs */
                char ob[32];
                snprintf(ob, sizeof ob, "obs_%u_%u", i, w->obs[i] - 1);
                int rv = graph_remove_observation(w->v3, w->off[i], (const u8 *)ob, (u16)strlen(ob), r);
                int rg = g4_remove_observation(w->g4, w->eid[i], (const u8 *)ob, (u16)strlen(ob), r);
                assert(rv == rg);
                if (rv) w->obs[i]--;
            }
        }
        compare_entities(w);
        compare_traversal(w);
        compare_queries(w);
        compare_walks(w);
        if (r % 5 == 4) {                                    /* commit boundary */
            assert(mstore_txn_commit(w->ms));
            assert(mstore_txn_begin(w->ms));
        }
    }
    assert(mstore_txn_commit(w->ms));
    printf("test_parity: ALL PASSES over %u rounds — v4 == v3 observable behavior\n", ROUNDS);
    graph4_close(w->g4);
    mstore_close(w->ms);
    graph_close(w->v3);
    st_close(w->v3st);
    unlink(v3g); unlink(v3s);
    free(w);
    return 0;
}
