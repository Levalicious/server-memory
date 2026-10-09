/*
 * test_import.c — v4_import self-test. Builds a v3 store carrying the full
 * preserved payload (distinct dual mtimes, visits, psi, and totals that
 * EXCEED the live per-entity sums via a later-deleted entity), imports it,
 * asserts the report is clean, then reopens the v4 store and spot-checks
 * the preserved fields.
 */
#define V4I_NO_MAIN
#include "v4_import.c"
#include <assert.h>

int main(void) {
    printf("test_import:\n");

    char v3g[] = "/tmp/v4imp_v3g_XXXXXX", v3s[] = "/tmp/v4imp_v3s_XXXXXX";
    int f = mkstemp(v3g); assert(f >= 0); close(f); unlink(v3g);
    f = mkstemp(v3s); assert(f >= 0); close(f); unlink(v3s);
    stringtable_t *st = st_open(v3s, 1 << 20);
    graph_t *g = graph_open(v3g, st, 1 << 20);
    assert(st && g);

    u64 a = graph_create_entity(g, (const u8 *)"ImpA", 4, (const u8 *)"impT", 4, 100);
    u64 b = graph_create_entity(g, (const u8 *)"ImpB", 4, (const u8 *)"impT", 4, 101);
    u64 c = graph_create_entity(g, (const u8 *)"ImpC", 4, (const u8 *)"impT", 4, 102);
    u64 z = graph_create_entity(g, (const u8 *)"ImpGone", 7, (const u8 *)"impT", 4, 103);
    assert(a && b && c && z);
    assert(graph_add_observation(g, a, (const u8 *)"obs one", 7, 110) == 1);
    assert(graph_create_relation(g, a, b, (const u8 *)"LINK", 4, 120) == 1);
    assert(graph_create_relation(g, b, c, (const u8 *)"LINK", 4, 121) == 1);
    /* preserved payload: distinct dual mtimes + visits + psi */
    graph_set_entity_fields(g, a, 200, 210, 7, 3, 0.125);
    graph_set_entity_fields(g, b, 201, 0, 0, 0, 0.0);
    graph_set_entity_fields(g, z, 300, 0, 40, 40, 0.0);   /* visits die with z */
    graph_set_totals(g, 47, 43);                          /* deliberately > live sums */
    assert(graph_delete_entity(g, z) == 1);
    graph_sync(g);

    char dst[] = "/tmp/v4imp_dst_XXXXXX";
    assert(mkdtemp(dst));
    v4i_report_t rep;
    int rc = v4_import_run(v3g, v3s, dst, &rep);
    assert(rc == 0 && rep.ok);
    assert(rep.entities == 3);
    assert(rep.relations_in == 2 && rep.relations_created == 2 && rep.relations_dedup == 0);

    /* reopen and spot-check the preserved payload */
    char mp[1200], gp[1200], sp[1200];
    snprintf(mp, sizeof mp, "%s/manifest.kb", dst);
    snprintf(gp, sizeof gp, "%s/graph.kb", dst);
    snprintf(sp, sizeof sp, "%s/strings.kb", dst);
    seg_io_t *mio = seg_io_posix_open(mp, 1);
    seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
    mstore_t *ms = mstore_open(mio, sios, 2);
    graph4_t *g4 = ms ? graph4_open(ms) : NULL;
    assert(g4);

    u32 ea = g4_lookup(g4, (const u8 *)"ImpA", 4);
    assert(ea);
    g4_entity_t e;
    assert(g4_read_entity(g4, ea, &e));
    assert(e.mtime == 200 && e.obs_mtime == 210);
    assert(e.structural_visits == 7 && e.walker_visits == 3);
    assert(e.psi > 0.124 && e.psi < 0.126);
    assert(e.obs_count == 1);
    /* Totals are persisted verbatim (META; ruling 4-i): the v3 header's
     * orphaned component (ImpGone's 40/40) transfers exactly. */
    assert(g4_structural_total(g4) == 47 && g4_walker_total(g4) == 43);
    assert(rep.orphaned_sv == 40 && rep.orphaned_wv == 40);
    assert(g4_lookup(g4, (const u8 *)"ImpGone", 7) == 0);
    assert(g4_relation_count(g4) == 2);

    printf("test_import: ALL PASS\n");
    graph4_close(g4);
    mstore_close(ms);
    graph_close(g);
    st_close(st);
    unlink(v3g); unlink(v3s);
    {
        char cmd[1300];
        snprintf(cmd, sizeof cmd, "rm -rf %s", dst);
        assert(system(cmd) == 0);
    }
    return 0;
}
