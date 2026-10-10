/*
 * test_graph4.c — entities + persisted name index: units + fuzz-vs-model
 * with mstore commit/reopen cycles.
 *
 * Semantics contract mirrors v3 graph tests (Principle_V3TestsUnmodified):
 * dup create returns existing, delete releases string refs, obs limit 2,
 * lookup by name, enumeration = live set.
 */
#include "segstore.h"
#include "regex.h"
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
        assert(g4_add_observation(g, a, (const u8 *)"obs two", 7, 12) == 1);
        /* v3 semantics: no dup refusal; the LIMIT is what refuses now */
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

    TEST(logical_ids_dead_on_delete_never_alias);
    {
        /* build step 2 (docs/shard-seam-design-note.md §3): ids crossing this
         * API are logical node ids. A deleted id is retired — stale refs
         * resolve dead instead of aliasing a reused physical slot (the v3
         * reuse-aliasing failure class). */
        mstore_t *ms = fresh_store();
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"IdA", 3, (const u8 *)"T", 1, 1);
        u32 b = g4_create_entity(g, (const u8 *)"IdB", 3, (const u8 *)"T", 1, 2);
        assert(a == 1 && b == 2);                          /* dense from 1 */
        assert(g4_delete_entity(g, a));
        u32 c = g4_create_entity(g, (const u8 *)"IdC", 3, (const u8 *)"T", 1, 3);
        assert(c == 3);                                    /* retired, not reused */
        assert(g4_lookup(g, (const u8 *)"IdA", 3) == 0);
        g4_entity_t e;
        assert(g4_read_entity(g, a, &e) == 0);             /* dead id stays dead */
        assert(g4_read_entity(g, b, &e) == 1);
        assert(g4_read_entity(g, c, &e) == 1);
        assert(g4_delete_entity(g, a) == 0);               /* double-delete: no-op */
        assert(g4_entity_count(g) == 2);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    PASS();

    TEST(node_id_page_growth_and_reopen_continuation);
    {
        /* 1500 nodes cross the 1019-slot data-page boundary (dir npages=2);
         * the mapping survives reopen and numbering continues monotonically
         * (META v2 next_node). */
        char mp[] = "/tmp/g4nid_m_XXXXXX", gp[] = "/tmp/g4nid_g_XXXXXX",
             sp[] = "/tmp/g4nid_s_XXXXXX";
        int f;
        f = mkstemp(mp); assert(f >= 0); close(f);
        f = mkstemp(gp); assert(f >= 0); close(f);
        f = mkstemp(sp); assert(f >= 0); close(f);
        static u32 ids[1500];
        {
            seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
            mstore_t *ms = mstore_create(seg_io_posix_open(mp, 1), sios, 2, 2);
            assert(ms);
            graph4_t *g = graph4_open(ms);
            assert(g);
            assert(mstore_txn_begin(ms));
            char nm[32];
            for (u32 i = 0; i < 1500; i++) {
                int n = snprintf(nm, sizeof nm, "Nid_%u", i);
                ids[i] = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)"K", 1, i);
                assert(ids[i] == i + 1);
            }
            assert(mstore_txn_commit(ms));
            graph4_close(g);
            mstore_close(ms);
        }
        {
            seg_io_t *rios[2] = { seg_io_posix_open(gp, 0), seg_io_posix_open(sp, 0) };
            mstore_t *ms2 = mstore_open(seg_io_posix_open(mp, 0), rios, 2);
            assert(ms2);
            graph4_t *g2 = graph4_open(ms2);
            assert(g2);
            char nm[32];
            for (u32 i = 0; i < 1500; i += 137) {
                int n = snprintf(nm, sizeof nm, "Nid_%u", i);
                assert(g4_lookup(g2, (const u8 *)nm, (u16)n) == ids[i]);
            }
            g4_entity_t e;
            assert(g4_read_entity(g2, ids[1018], &e) == 1);   /* page 0, last slot */
            assert(g4_read_entity(g2, ids[1019], &e) == 1);   /* page 1, first slot */
            assert(mstore_txn_begin(ms2));
            u32 fresh = g4_create_entity(g2, (const u8 *)"Nid_1500", 8, (const u8 *)"K", 1, 1500);
            assert(fresh == 1501);                            /* monotone continuation */
            assert(mstore_txn_commit(ms2));
            graph4_close(g2);
            mstore_close(ms2);
        }
        unlink(mp); unlink(gp); unlink(sp);
    }
    PASS();

    TEST(dir_growth_past_43_pages);
    {
        /* regression guard: the first ind_append implementation grew the dir
         * record via seg_page_update's relocate-per-grow path, which
         * exhausted the dir page at 43 data pages (43,817 nodes) — caught by
         * the real-KB import gate, not by small tests. The record is now
         * written at full size once and poked in place; this crosses 44
         * data pages (45 dir entries). */
        mstore_t *ms = fresh_store();
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[16];
        enum { NG = 45000 };
        for (u32 i = 0; i < NG; i++) {
            int n = snprintf(nm, sizeof nm, "G%05u", i);
            u32 node = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)"K", 1, i);
            assert(node == i + 1);
        }
        for (u32 i = 0; i < NG; i += 997) {           /* sample every dir page */
            int n = snprintf(nm, sizeof nm, "G%05u", i);
            assert(g4_lookup(g, (const u8 *)nm, (u16)n) == i + 1);
        }
        g4_entity_t e;
        assert(g4_read_entity(g, 1, &e) == 1);
        assert(g4_read_entity(g, NG, &e) == 1);
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
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

    TEST(neighbors_hops_and_direction);
    {
        /* chain A->B->C->D plus C->A back edge */
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 A = g4_create_entity(g, (const u8 *)"A", 1, (const u8 *)"t", 1, 1);
        u32 B = g4_create_entity(g, (const u8 *)"B", 1, (const u8 *)"t", 1, 2);
        u32 C = g4_create_entity(g, (const u8 *)"C", 1, (const u8 *)"t", 1, 3);
        u32 D = g4_create_entity(g, (const u8 *)"D", 1, (const u8 *)"t", 1, 4);
        assert(g4_create_relation(g, A, B, (const u8 *)"r", 1, 1));
        assert(g4_create_relation(g, B, C, (const u8 *)"r", 1, 2));
        assert(g4_create_relation(g, C, D, (const u8 *)"r", 1, 3));
        assert(g4_create_relation(g, C, A, (const u8 *)"r", 1, 4));
        u32 out[8]; u32 n;
        /* depth 1 (C hop-count) = immediate */
        n = g4_neighbors(g, A, 1, G4_DIR_ANY, out, 8);
        assert(n == 2);                        /* B (fwd) + C (backward mirror) */
        n = g4_neighbors(g, A, 1, G4_DIR_FORWARD, out, 8);
        assert(n == 1 && out[0] == B);         /* only A->B */
        n = g4_neighbors(g, A, 1, G4_DIR_BACKWARD, out, 8);
        assert(n == 1 && out[0] == C);         /* C->A seen backward */
        /* depth 2 FORWARD: B then C */
        n = g4_neighbors(g, A, 2, G4_DIR_FORWARD, out, 8);
        assert(n == 2);
        /* depth 3 FORWARD: B, C, D (start excluded even though reachable via cycle) */
        n = g4_neighbors(g, A, 3, G4_DIR_FORWARD, out, 8);
        assert(n == 3);
        /* depth 0 = nothing (internal hop-count contract) */
        assert(g4_neighbors(g, A, 0, G4_DIR_ANY, out, 8) == 0);
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(find_path_bidir_and_filters);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        enum { NCH = 9 };
        u32 ch[NCH];
        char nm[8];
        for (u32 i = 0; i < NCH; i++) {
            snprintf(nm, sizeof nm, "P%u", i);
            ch[i] = g4_create_entity(g, (const u8 *)nm, (u16)strlen(nm),
                                     (const u8 *)"t", 1, i);
            assert(ch[i]);
            if (i) assert(g4_create_relation(g, ch[i-1], ch[i], (const u8 *)"n", 1, i));
        }
        u32 path[16]; u32 n;
        n = g4_find_path(g, ch[0], ch[8], 10, G4_DIR_ANY, path, 16);
        assert(n == 9);
        assert(path[0] == ch[0] && path[8] == ch[8]);
        for (u32 i = 0; i < 9; i++) assert(path[i] == ch[i]);   /* exact chain */
        /* FORWARD works along the chain; BACKWARD from far end works */
        assert(g4_find_path(g, ch[0], ch[8], 10, G4_DIR_FORWARD, path, 16) == 9);
        assert(g4_find_path(g, ch[8], ch[0], 10, G4_DIR_BACKWARD, path, 16) == 9);
        /* FORWARD from far end: impossible */
        assert(g4_find_path(g, ch[8], ch[0], 10, G4_DIR_FORWARD, path, 16) == 0);
        /* depth-limited: chain needs 8 levels; 4 = fail */
        assert(g4_find_path(g, ch[0], ch[8], 4, G4_DIR_ANY, path, 16) == 0);
        /* trivial + disconnected */
        assert(g4_find_path(g, ch[3], ch[3], 5, G4_DIR_ANY, path, 16) == 1);
        u32 iso = g4_create_entity(g, (const u8 *)"Iso", 3, (const u8 *)"t", 1, 99);
        assert(g4_find_path(g, ch[0], iso, 10, G4_DIR_ANY, path, 16) == 0);
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(fuzz_findpath_vs_reference_bfs);
    {
        /* random graph; bidir result must MATCH a reference unidirectional
         * BFS on (reachability, path length); path itself must be valid. */
        enum { NV = 40, NEDGE = 90, TRIALS = 300 };
        static u32 vid[NV];
        static int adj[NV][NV];                            /* fwd adjacency */
        memset(adj, 0, sizeof adj);
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        char nm[8];
        for (u32 i = 0; i < NV; i++) {
            snprintf(nm, sizeof nm, "V%u", i);
            vid[i] = g4_create_entity(g, (const u8 *)nm, (u16)strlen(nm),
                                      (const u8 *)"t", 1, i);
            assert(vid[i]);
        }
        for (u32 e = 0; e < NEDGE; e++) {
            u32 a = (u32)(rng() % NV), b = (u32)(rng() % NV);
            if (a == b || adj[a][b]) continue;
            assert(g4_create_relation(g, vid[a], vid[b], (const u8 *)"e", 1, e));
            adj[a][b] = 1;
        }
        for (u32 t = 0; t < TRIALS; t++) {
            u32 s = (u32)(rng() % NV), d = (u32)(rng() % NV);
            u32 want = (u32)(rng() % 3);                   /* ANY/FWD/BWD */
            u32 dirv = want == 0 ? G4_DIR_ANY : want == 1 ? G4_DIR_FORWARD : G4_DIR_BACKWARD;
            /* reference BFS on the model */
            int dist[NV]; for (u32 i = 0; i < NV; i++) dist[i] = -1;
            u32 q[NV]; u32 qh = 0, qt = 0;
            dist[s] = 0; q[qt++] = s;
            while (qh < qt) {
                u32 x = q[qh++];
                for (u32 y = 0; y < NV; y++) {
                    int ok = (dirv == G4_DIR_ANY) ? (adj[x][y] || adj[y][x])
                           : (dirv == G4_DIR_FORWARD) ? adj[x][y] : adj[y][x];
                    if (ok && dist[y] < 0) { dist[y] = dist[x] + 1; q[qt++] = y; }
                }
            }
            u32 path[64];
            u32 n = g4_find_path(g, vid[s], vid[d], 16, dirv, path, 64);
            if (dist[d] < 0) assert(n == 0);
            else {
                assert(n == (u32)dist[d] + 1);             /* SHORTEST length */
                assert(path[0] == vid[s] && path[n-1] == vid[d]);
                /* every hop must be a real filtered edge */
                for (u32 i = 0; i + 1 < n; i++) {
                    u32 xa = 0, xb = 0;
                    for (u32 k = 0; k < NV; k++) { if (vid[k] == path[i]) xa = k; if (vid[k] == path[i+1]) xb = k; }
                    int ok = (dirv == G4_DIR_ANY) ? (adj[xa][xb] || adj[xb][xa])
                           : (dirv == G4_DIR_FORWARD) ? adj[xa][xb] : adj[xb][xa];
                    assert(ok);
                }
            }
        }
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(search_and_type_index_basics);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"NetworkNotNotepad", 17, (const u8 *)"principle", 9, 1);
        u32 b = g4_create_entity(g, (const u8 *)"Melting", 7, (const u8 *)"process", 7, 2);
        u32 c = g4_create_entity(g, (const u8 *)"MeltingSession_X", 16, (const u8 *)"process", 7, 3);
        assert(a && b && c);
        assert(g4_add_observation(g, a, (const u8 *)"graph structure IS meaning", 26, 4));
        u32 out[8]; u32 n;
        /* literal + prefiltered */
        n = g4_search(g, "Melting", out, 8);
        assert(n == 2);
        n = g4_search(g, "^Melting$", out, 8);
        assert(n == 1 && out[0] == b);
        /* obs text is searchable */
        n = g4_search(g, "structure IS", out, 8);
        assert(n == 1 && out[0] == a);
        /* type text is searchable */
        n = g4_search(g, "principle", out, 8);
        assert(n == 1 && out[0] == a);
        /* unfilterable (short) pattern: full-scan path */
        n = g4_search(g, "Mx|Se", out, 8);
        assert(n == 1 && out[0] == c);
        /* invalid pattern */
        assert(g4_search(g, "([unclosed", out, 8) == 0);
        assert(g4_regex_valid("a(b|c)*d") == 1);
        assert(g4_regex_valid("([bad") == 0);
        /* writes AFTER a search must be caught by the dirty set */
        u32 d2 = g4_create_entity(g, (const u8 *)"LateArrival_Melting", 19, (const u8 *)"late", 4, 9);
        assert(d2);
        n = g4_search(g, "Melting", out, 8);
        assert(n == 3);
        assert(g4_delete_entity(g, b));
        n = g4_search(g, "Melting", out, 8);
        assert(n == 2);
        /* type index */
        n = g4_entities_by_type(g, (const u8 *)"process", 7, out, 8);
        assert(n == 1 && out[0] == c);                     /* b deleted */
        n = g4_entities_by_type(g, (const u8 *)"absent", 6, out, 8);
        assert(n == 0);
        u32 sids[8];
        n = g4_entity_types(g, sids, 8);
        assert(n == 3);                                    /* principle, process, late */
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(relation_types_and_orphans);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        u32 a = g4_create_entity(g, (const u8 *)"a", 1, (const u8 *)"t", 1, 1);
        u32 b = g4_create_entity(g, (const u8 *)"b", 1, (const u8 *)"t", 1, 2);
        u32 c = g4_create_entity(g, (const u8 *)"c", 1, (const u8 *)"t", 1, 3);
        assert(g4_create_relation(g, a, b, (const u8 *)"REL_X", 5, 1));
        assert(g4_create_relation(g, b, a, (const u8 *)"REL_Y", 5, 2));
        u32 sids[8]; u32 out[8];
        assert(g4_relation_types(g, sids, 8) == 2);
        u32 n = g4_orphaned(g, out, 8);
        assert(n == 1 && out[0] == c);
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(fuzz_search_vs_bruteforce);
    {
        /* soundness: trigram-prefiltered search == brute regex over all
         * entities, across mutation churn. Patterns chosen to exercise
         * filterable AND unfilterable paths. */
        enum { NENT2 = 120, ROUNDS = 40 };
        static const char *pats[] = {
            "Node_1", "Node_.[0-9]", "alpha", "beta|gamma", "^Node_7",
            "a.c", "obs.*text", "^x$", "Node_1[0-9]$", "(al|be)pha?"
        };
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        static int  alive[NENT2]; static u32 ids[NENT2];
        static char nm2[NENT2][32];
        memset(alive, 0, sizeof alive);
        for (u32 r = 0; r < ROUNDS; r++) {
            /* mutate a handful */
            for (u32 m = 0; m < 12; m++) {
                u32 i = (u32)(rng() % NENT2);
                if (!alive[i]) {
                    const char *flavors[3] = { "alpha", "beta", "gamma" };
                    snprintf(nm2[i], sizeof nm2[i], "Node_%u_%s", i, flavors[rng() % 3]);
                    ids[i] = g4_create_entity(g, (const u8 *)nm2[i], (u16)strlen(nm2[i]),
                                              (const u8 *)"fz", 2, r);
                    assert(ids[i]); alive[i] = 1;
                    if (rng() % 3 == 0)
                        g4_add_observation(g, ids[i], (const u8 *)"obs some text", 13, r);
                } else if (rng() % 2) {
                    assert(g4_delete_entity(g, ids[i]));
                    alive[i] = 0;
                }
            }
            /* verify one random pattern against brute force */
            const char *pat = pats[rng() % (sizeof pats / sizeof *pats)];
            u32 out2[NENT2]; u32 n = g4_search(g, pat, out2, NENT2);
            /* brute: run engine over each live entity's fields */
            const char *err = NULL;
            Regex *re = re_compile(pat, &err);
            assert(re);
            u32 want = 0;
            for (u32 i = 0; i < NENT2; i++) {
                if (!alive[i]) continue;
                g4_entity_t e;
                assert(g4_read_entity(g, ids[i], &e));
                u16 len; const u8 *bb;
                int hit = 0;
                if ((bb = g4_str(g, e.name_sid, &len)) && re_nfa_search(re, (const char *)bb, len)) hit = 1;
                if (!hit && (bb = g4_str(g, e.type_sid, &len)) && re_nfa_search(re, (const char *)bb, len)) hit = 1;
                if (!hit && e.obs_count >= 1 && (bb = g4_str(g, e.obs0_sid, &len)) && re_nfa_search(re, (const char *)bb, len)) hit = 1;
                if (!hit && e.obs_count >= 2 && (bb = g4_str(g, e.obs1_sid, &len)) && re_nfa_search(re, (const char *)bb, len)) hit = 1;
                if (hit) want++;
            }
            re_free(re);
            assert(n == want);                             /* soundness + completeness */
            if (r % 7 == 0) { assert(mstore_txn_commit(ms)); assert(mstore_txn_begin(ms)); }
        }
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    TEST(rank_walk_psi_surface);
    {
        mstore_t *ms = fresh_store();
        graph4_t *g = graph4_open(ms);
        assert(mstore_txn_begin(ms));
        /* small ring + tail: a->b->c->a, c->d */
        u32 a = g4_create_entity(g, (const u8 *)"ra", 2, (const u8 *)"t", 1, 1);
        u32 b = g4_create_entity(g, (const u8 *)"rb", 2, (const u8 *)"t", 1, 2);
        u32 c = g4_create_entity(g, (const u8 *)"rc", 2, (const u8 *)"t", 1, 3);
        u32 d = g4_create_entity(g, (const u8 *)"rd", 2, (const u8 *)"t", 1, 4);
        assert(g4_create_relation(g, a, b, (const u8 *)"n", 1, 1));
        assert(g4_create_relation(g, b, c, (const u8 *)"n", 1, 2));
        assert(g4_create_relation(g, c, a, (const u8 *)"n", 1, 3));
        assert(g4_create_relation(g, c, d, (const u8 *)"n", 1, 4));
        assert(g4_relation_count(g) == 4);

        /* visits + ranks */
        g4_inc_structural_visit(g, a);
        g4_inc_structural_visit(g, a);
        g4_inc_walker_visit(g, b);
        assert(g4_structural_total(g) == 2 && g4_walker_total(g) == 1);
        assert(g4_structural_rank(g, a) == 1.0);
        assert(g4_walker_rank(g, b) == 1.0);

        /* MC sample: deterministic under a seed; increments visits */
        g4_seed_rng(42);
        u64 before = g4_structural_total(g);
        u32 visits = g4_structural_sample(g, 3, 0.85);
        assert(visits > 0 && g4_structural_total(g) == before + visits);

        /* psi power iteration converges; psi normalized, persisted */
        u32 iters = g4_compute_merw_psi(g, 0.9, 200, 1e-10);
        assert(iters > 0);
        double sum2 = 0;
        u32 all[8]; u32 na = g4_list_entities(g, all, 8);
        assert(na == 4);
        for (u32 i = 0; i < na; i++) { double p = g4_get_psi(g, all[i]); assert(p >= 0); sum2 += p * p; }
        assert(sum2 > 0.99 && sum2 < 1.01);               /* unit norm */

        /* random walk: valid, seeded-deterministic */
        u32 p1[16], p2[16];
        u32 n1 = g4_random_walk(g, a, 8, G4_DIR_FORWARD, 0, 777, 0, p1, 16, NULL);
        u32 n2 = g4_random_walk(g, a, 8, G4_DIR_FORWARD, 0, 777, 0, p2, 16, NULL);
        assert(n1 == n2 && n1 >= 2);
        for (u32 i = 0; i < n1; i++) assert(p1[i] == p2[i]);
        assert(p1[0] == a);
        /* every hop is a real forward edge */
        for (u32 i = 0; i + 1 < n1; i++) {
            g4_edge_t es[8]; u32 ne = g4_edges(g, p1[i], es, 8);
            int found = 0;
            for (u32 k = 0; k < ne; k++)
                if (es[k].direction == G4_DIR_FORWARD && es[k].target_eid == p1[i+1]) found = 1;
            assert(found);
        }
        /* merw mode also valid */
        u32 n3 = g4_random_walk(g, a, 8, G4_DIR_ANY, 1, 123, 0, p1, 16, NULL);
        assert(n3 >= 1 && p1[0] == a);

        /* totals survive reopen via record scan: commit + check set_fields */
        assert(g4_set_entity_fields(g, d, 99, 98, 7, 5, 0.25));
        g4_entity_t e;
        assert(g4_read_entity(g, d, &e) && e.structural_visits == 7 && e.mtime == 99);
        assert(mstore_txn_commit(ms));
        graph4_close(g); mstore_close(ms);
    }
    PASS();

    printf("test_graph4: %d tests passed\n", tests_run);
    return 0;
}
