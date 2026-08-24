/*
 * test_graph4.c — entities + persisted name index: units + fuzz-vs-model
 * with mstore commit/reopen cycles.
 *
 * Semantics contract mirrors v3 graph tests (Principle_V3TestsUnmodified):
 * dup create returns existing, delete releases string refs, obs limit 2,
 * lookup by name, enumeration = live set.
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

static u64 rng_state = 0x47344734ull;
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

static mstore_t *fresh_store(void) {
    seg_io_t *sios[2] = { seg_io_sim_open(), seg_io_sim_open() };
    return mstore_create(seg_io_sim_open(), sios, 2, 2);
}

int main(void) {
    printf("test_graph4:\n");

    TEST(create_lookup_read_dup);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(g && g4_entity_count(g) == 0);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"Self", 4, (const u8 *)"agent", 5, 1000);
        assert(a != 0);
        u32 b = g4_create_entity(g, (const u8 *)"Melting", 7, (const u8 *)"process", 7, 1001);
        assert(b != 0 && b != a);
        /* dup name returns existing eid, does not double-create */
        u32 a2 = g4_create_entity(g, (const u8 *)"Self", 4, (const u8 *)"other", 5, 1002);
        assert(a2 == a && g4_entity_count(g) == 2);
        assert(g4_lookup(g, (const u8 *)"Self", 4) == a);
        assert(g4_lookup(g, (const u8 *)"Absent", 6) == 0);
        g4_entity_t e;
        assert(g4_read_entity(g, a, &e));
        u16 len = 0;
        const u8 *nm = g4_str(g, e.name_sid, &len);
        assert(nm && len == 4 && memcmp(nm, "Self", 4) == 0);
        const u8 *ty = g4_str(g, e.type_sid, &len);
        assert(ty && len == 5 && memcmp(ty, "agent", 5) == 0);
        assert(e.mtime == 1000 && e.obs_count == 0);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(observations_lifecycle_and_limit);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"E", 1, (const u8 *)"t", 1, 1);
        assert(a);
        assert(g4_add_observation(g, a, (const u8 *)"obs one", 7, 10) == 1);
        assert(g4_add_observation(g, a, (const u8 *)"obs one", 7, 11) == 0);  /* dup */
        assert(g4_add_observation(g, a, (const u8 *)"obs two", 7, 12) == 1);
        assert(g4_add_observation(g, a, (const u8 *)"obs three", 9, 13) == 0); /* limit 2 */
        g4_entity_t e;
        assert(g4_read_entity(g, a, &e));
        assert(e.obs_count == 2 && e.obs_mtime == 12);
        u16 len = 0;
        assert(memcmp(g4_str(g, e.obs0_sid, &len), "obs one", 7) == 0 && len == 7);
        /* remove first: second shifts down */
        assert(g4_remove_observation(g, a, (const u8 *)"obs one", 7, 20) == 1);
        assert(g4_read_entity(g, a, &e));
        assert(e.obs_count == 1 && e.obs1_sid == 0);
        assert(memcmp(g4_str(g, e.obs0_sid, &len), "obs two", 7) == 0);
        assert(g4_remove_observation(g, a, (const u8 *)"obs one", 7, 21) == 0); /* gone */
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(delete_releases_strings_and_slot);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"Doomed", 6, (const u8 *)"kind", 4, 1);
        u32 b = g4_create_entity(g, (const u8 *)"Keeper", 6, (const u8 *)"kind", 4, 2);
        assert(a && b);
        assert(g4_add_observation(g, a, (const u8 *)"note", 4, 3) == 1);
        assert(g4_delete_entity(g, a) == 1);
        assert(g4_delete_entity(g, a) == 0);              /* double delete */
        assert(g4_lookup(g, (const u8 *)"Doomed", 6) == 0);
        assert(g4_entity_count(g) == 1);
        g4_entity_t e;
        assert(g4_read_entity(g, a, &e) == 0);            /* dead eid unreadable */
        assert(g4_read_entity(g, b, &e) == 1);            /* keeper intact */
        /* shared type string survives with keeper's ref */
        u16 len = 0;
        assert(memcmp(g4_str(g, e.type_sid, &len), "kind", 4) == 0);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(rehash_growth_2000_entities);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        char nm[64];
        static u32 eids[2000];
        for (u32 i = 0; i < 2000; i++) {
            int n = snprintf(nm, sizeof nm, "Entity_%u", i);
            eids[i] = g4_create_entity(g, (const u8 *)nm, (u16)n,
                                       (const u8 *)"T", 1, i);
            assert(eids[i] != 0);
        }
        assert(g4_entity_count(g) == 2000);
        /* every entity findable through (multiple) rehashes */
        for (u32 i = 0; i < 2000; i += 37) {
            int n = snprintf(nm, sizeof nm, "Entity_%u", i);
            assert(g4_lookup(g, (const u8 *)nm, (u16)n) == eids[i]);
        }
        static u32 out[2100];
        u32 cnt = g4_list_entities(g, out, 2100);
        assert(cnt == 2000);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(fuzz_vs_model_8k_ops_with_commits);
    {
        enum { NENT = 300, OPS = 8000 };
        static char mname[NENT][32];
        static int  mlive[NENT]; static u32 meid[NENT];
        static int  mobs[NENT];                            /* obs count */
        for (u32 i = 0; i < NENT; i++) {
            snprintf(mname[i], sizeof mname[i], "N%u_%llu", i,
                     (unsigned long long)(rng() & 0xFFFF));
            mlive[i] = 0; meid[i] = 0; mobs[i] = 0;
        }
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 commits = 0;
        char obsbuf[64];
        for (u32 op = 0; op < OPS; op++) {
            u32 i = (u32)(rng() % NENT);
            u32 kind = (u32)(rng() % 100);
            u16 nlen = (u16)strlen(mname[i]);
            if (kind < 40) {                               /* create */
                u32 eid = g4_create_entity(g, (const u8 *)mname[i], nlen,
                                           (const u8 *)"Type", 4, op);
                assert(eid != 0);
                if (mlive[i]) assert(eid == meid[i]);      /* dup -> existing */
                else { mlive[i] = 1; meid[i] = eid; mobs[i] = 0; }
            } else if (kind < 60) {                        /* delete */
                int ok = g4_delete_entity(g, meid[i]);
                assert(ok == (mlive[i] ? 1 : 0));
                if (ok) { mlive[i] = 0; meid[i] = 0; }
            } else if (kind < 75) {                        /* add obs */
                snprintf(obsbuf, sizeof obsbuf, "obs_%u_%u", i, mobs[i]);
                int ok = g4_add_observation(g, meid[i], (const u8 *)obsbuf,
                                            (u16)strlen(obsbuf), op);
                if (!mlive[i]) assert(ok == 0);
                else if (mobs[i] >= 2) assert(ok == 0);
                else { assert(ok == 1); mobs[i]++; }
            } else if (kind < 85) {                        /* verify read */
                g4_entity_t e;
                int ok = g4_read_entity(g, meid[i], &e);
                assert(ok == (mlive[i] ? 1 : 0));
                if (ok) {
                    assert(e.obs_count == mobs[i]);
                    u16 len = 0;
                    const u8 *nm2 = g4_str(g, e.name_sid, &len);
                    assert(nm2 && len == nlen && memcmp(nm2, mname[i], len) == 0);
                    assert(g4_lookup(g, (const u8 *)mname[i], nlen) == meid[i]);
                }
            } else if (kind < 97) {                        /* lookup absent-safe */
                u32 got = g4_lookup(g, (const u8 *)mname[i], nlen);
                assert(got == (mlive[i] ? meid[i] : 0));
            } else {                                       /* commit boundary */
                assert(mstore_txn_commit(ms));
                assert(mstore_txn_begin(ms));
                commits++;
            }
        }
        assert(mstore_txn_commit(ms));
        u32 live = 0;
        for (u32 i = 0; i < NENT; i++) if (mlive[i]) live++;
        assert(g4_entity_count(g) == live);
        printf("(commits %u live %u) ", commits, live);
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(posix_reopen_recovers_graph);
    {
        char mp[] = "/tmp/g4_manifest_XXXXXX", gp[] = "/tmp/g4_graph_XXXXXX",
             sp[] = "/tmp/g4_strings_XXXXXX";
        int f;
        f = mkstemp(mp); assert(f >= 0); close(f);
        f = mkstemp(gp); assert(f >= 0); close(f);
        f = mkstemp(sp); assert(f >= 0); close(f);

        seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
        mstore_t *ms = mstore_create(seg_io_posix_open(mp, 1), sios, 2, 2);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32];
        static u32 eids[500];
        for (u32 i = 0; i < 500; i++) {
            int n = snprintf(nm, sizeof nm, "Persist_%u", i);
            eids[i] = g4_create_entity(g, (const u8 *)nm, (u16)n,
                                       (const u8 *)"K", 1, i);
            assert(eids[i]);
        }
        assert(g4_add_observation(g, eids[7], (const u8 *)"kept obs", 8, 999));
        assert(g4_delete_entity(g, eids[13]));
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);                       /* closes all files */

        seg_io_t *rios[2] = { seg_io_posix_open(gp, 0), seg_io_posix_open(sp, 0) };
        mstore_t *ms2 = mstore_open(seg_io_posix_open(mp, 0), rios, 2);
        assert(ms2);
        graph4_t *g2 = graph4_open(ms2);
        assert(g2 && g4_entity_count(g2) == 499);
        for (u32 i = 0; i < 500; i += 41) {
            int n = snprintf(nm, sizeof nm, "Persist_%u", i);
            u32 want = (i == 13) ? 0 : eids[i];
            assert(g4_lookup(g2, (const u8 *)nm, (u16)n) == want);   /* SAME eids */
        }
        g4_entity_t e;
        assert(g4_read_entity(g2, eids[7], &e) && e.obs_count == 1);
        u16 len = 0;
        assert(memcmp(g4_str(g2, e.obs0_sid, &len), "kept obs", 8) == 0 && len == 8);
        /* mutate after reopen: index + strings fully functional */
        assert(mstore_txn_begin(ms2));
        u32 fresh = g4_create_entity(g2, (const u8 *)"PostReopen", 10,
                                     (const u8 *)"K", 1, 5000);
        assert(fresh && g4_lookup(g2, (const u8 *)"PostReopen", 10) == fresh);
        assert(mstore_txn_commit(ms2));
        graph4_close(g2);
        mstore_close(ms2);
        unlink(mp); unlink(gp); unlink(sp);
    }
    PASS();

    TEST(relations_bidir_dup_delete);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"A", 1, (const u8 *)"t", 1, 1);
        u32 b = g4_create_entity(g, (const u8 *)"B", 1, (const u8 *)"t", 1, 2);
        assert(a && b);
        assert(g4_create_relation(g, a, b, (const u8 *)"LIKES", 5, 10) == 1);
        assert(g4_create_relation(g, a, b, (const u8 *)"LIKES", 5, 11) == 0);  /* dup */
        assert(g4_create_relation(g, b, a, (const u8 *)"LIKES", 5, 12) == 1);  /* reverse != dup */
        assert(g4_edge_count(g, a) == 2 && g4_edge_count(g, b) == 2);
        g4_edge_t ed[4];
        u32 n = g4_edges(g, a, ed, 4);
        assert(n == 2);
        u32 fwd = 0, bwd = 0;
        for (u32 i = 0; i < n; i++) {
            assert(ed[i].target_eid == b);
            u16 len = 0;
            assert(memcmp(g4_str(g, ed[i].rel_sid, &len), "LIKES", 5) == 0 && len == 5);
            if (ed[i].direction == G4_DIR_FORWARD) fwd++; else bwd++;
        }
        assert(fwd == 1 && bwd == 1);
        /* delete one direction; the other survives */
        assert(g4_delete_relation(g, a, b, (const u8 *)"LIKES", 5) == 1);
        assert(g4_delete_relation(g, a, b, (const u8 *)"LIKES", 5) == 0);
        assert(g4_edge_count(g, a) == 1 && g4_edge_count(g, b) == 1);
        assert(g4_delete_relation(g, b, a, (const u8 *)"LIKES", 5) == 1);
        assert(g4_edge_count(g, a) == 0 && g4_edge_count(g, b) == 0);
        /* rel string fully released: interning it again starts fresh */
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(hub_chain_spill_400_edges);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 hub = g4_create_entity(g, (const u8 *)"Hub", 3, (const u8 *)"t", 1, 1);
        assert(hub);
        char nm[32];
        static u32 spokes[400];
        for (u32 i = 0; i < 400; i++) {
            int n = snprintf(nm, sizeof nm, "S%u", i);
            spokes[i] = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)"t", 1, i);
            assert(spokes[i]);
            assert(g4_create_relation(g, hub, spokes[i], (const u8 *)"SPOKE", 5, i) == 1);
        }
        assert(g4_edge_count(g, hub) == 400);              /* chained records */
        static g4_edge_t ed[500];
        assert(g4_edges(g, hub, ed, 500) == 400);
        /* delete from the middle of the chain */
        assert(g4_delete_relation(g, hub, spokes[200], (const u8 *)"SPOKE", 5) == 1);
        assert(g4_edge_count(g, hub) == 400 - 1);
        assert(g4_edge_count(g, spokes[200]) == 0);
        /* delete the hub: every spoke's mirror must vanish */
        assert(g4_delete_entity(g, hub) == 1);
        for (u32 i = 0; i < 400; i += 23)
            assert(g4_edge_count(g, spokes[i]) == 0);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(self_loop_and_reopen_edges);
    {
        char mp[] = "/tmp/g4e_manifest_XXXXXX", gp[] = "/tmp/g4e_graph_XXXXXX",
             sp[] = "/tmp/g4e_strings_XXXXXX";
        int f;
        f = mkstemp(mp); assert(f >= 0); close(f);
        f = mkstemp(gp); assert(f >= 0); close(f);
        f = mkstemp(sp); assert(f >= 0); close(f);
        seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
        mstore_t *ms = mstore_create(seg_io_posix_open(mp, 1), sios, 2, 2);
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"Loop", 4, (const u8 *)"t", 1, 1);
        u32 b = g4_create_entity(g, (const u8 *)"Peer", 4, (const u8 *)"t", 1, 2);
        assert(g4_create_relation(g, a, a, (const u8 *)"SELF", 4, 5) == 1);
        assert(g4_edge_count(g, a) == 2);                  /* both mirrors on a */
        assert(g4_create_relation(g, a, b, (const u8 *)"OUT", 3, 6) == 1);
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);

        seg_io_t *rios[2] = { seg_io_posix_open(gp, 0), seg_io_posix_open(sp, 0) };
        mstore_t *ms2 = mstore_open(seg_io_posix_open(mp, 0), rios, 2);
        graph4_t *g2 = graph4_open(ms2);
        assert(g2);
        u32 a2 = g4_lookup(g2, (const u8 *)"Loop", 4);
        assert(a2 == a && g4_edge_count(g2, a2) == 3);     /* edges persisted */
        assert(mstore_txn_begin(ms2));
        assert(g4_delete_entity(g2, a2) == 1);             /* self-loop cleanup */
        assert(g4_edge_count(g2, b) == 0);                 /* mirror on peer gone */
        assert(mstore_txn_commit(ms2));
        graph4_close(g2); mstore_close(ms2);
        unlink(mp); unlink(gp); unlink(sp);
    }
    PASS();

    TEST(fuzz_edges_vs_model_6k_ops);
    {
        enum { NE = 60, NREL = 3, OPS = 6000 };
        static const char *rels[NREL] = { "R_ALPHA", "R_BETA", "R_GAMMA" };
        static int medge[NE][NE][NREL];                    /* fwd edge model */
        static int mlive[NE]; static u32 meid[NE];
        char nm[16];
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        memset(medge, 0, sizeof medge);
        for (u32 i = 0; i < NE; i++) { mlive[i] = 0; meid[i] = 0; }
        u32 commits = 0;
        for (u32 op = 0; op < OPS; op++) {
            u32 i = (u32)(rng() % NE), j = (u32)(rng() % NE), r = (u32)(rng() % NREL);
            u32 kind = (u32)(rng() % 100);
            if (kind < 25) {                               /* ensure entity */
                if (!mlive[i]) {
                    snprintf(nm, sizeof nm, "F%u", i);
                    meid[i] = g4_create_entity(g, (const u8 *)nm, (u16)strlen(nm),
                                               (const u8 *)"t", 1, op);
                    assert(meid[i]); mlive[i] = 1;
                }
            } else if (kind < 55) {                        /* create relation */
                int ok = g4_create_relation(g, meid[i], meid[j],
                                            (const u8 *)rels[r], (u16)strlen(rels[r]), op);
                if (!mlive[i] || !mlive[j]) assert(ok == 0);
                else if (medge[i][j][r]) assert(ok == 0);
                else { assert(ok == 1); medge[i][j][r] = 1; }
            } else if (kind < 75) {                        /* delete relation */
                int ok = g4_delete_relation(g, meid[i], meid[j],
                                            (const u8 *)rels[r], (u16)strlen(rels[r]));
                if (!mlive[i] || !mlive[j] || !medge[i][j][r]) assert(ok == 0);
                else { assert(ok == 1); medge[i][j][r] = 0; }
            } else if (kind < 85) {                        /* delete entity + incident */
                int ok = g4_delete_entity(g, meid[i]);
                assert(ok == (mlive[i] ? 1 : 0));
                if (ok) {
                    mlive[i] = 0; meid[i] = 0;
                    for (u32 k = 0; k < NE; k++)
                        for (u32 rr = 0; rr < NREL; rr++)
                            medge[i][k][rr] = medge[k][i][rr] = 0;
                }
            } else if (kind < 97) {                        /* verify edge counts */
                if (!mlive[i]) { assert(g4_edge_count(g, meid[i]) == 0); continue; }
                u32 want = 0;
                for (u32 k = 0; k < NE; k++)
                    for (u32 rr = 0; rr < NREL; rr++) {
                        if (medge[i][k][rr]) want++;                 /* fwd */
                        if (medge[k][i][rr]) want++;                 /* mirror */
                    }
                assert(g4_edge_count(g, meid[i]) == want);
            } else {
                assert(mstore_txn_commit(ms));
                assert(mstore_txn_begin(ms));
                commits++;
            }
        }
        assert(mstore_txn_commit(ms));
        printf("(commits %u) ", commits);
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    printf("test_graph4: %d tests passed\n", tests_run);
    return 0;
}
