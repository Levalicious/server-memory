/*
 * N-API binding for the v3 graph store (stringtable + graph over memoryfile).
 * Exposes the high-level C "DB" ops; the TS layer is a thin formatter/paginator.
 *
 * A Store handle wraps {stringtable_t*, graph_t*}. Offsets are passed as BigInt.
 */
#define NAPI_VERSION 8
#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include "stringtable.h"
#include "graph.h"

typedef struct { stringtable_t *st; graph_t *g; } Store;

#define NCALL(call) do { if ((call) != napi_ok) { napi_throw_error(env, NULL, "napi: " #call); return NULL; } } while (0)

static napi_value mkU64(napi_env env, u64 v) { napi_value r; napi_create_bigint_uint64(env, v, &r); return r; }
static napi_value mkU32(napi_env env, u32 v) { napi_value r; napi_create_uint32(env, v, &r); return r; }
static napi_value mkF64(napi_env env, double v){ napi_value r; napi_create_double(env, v, &r); return r; }
static u64 getU64(napi_env env, napi_value v) { uint64_t r = 0; bool l; napi_get_value_bigint_uint64(env, v, &r, &l); return r; }
static u32 getU32(napi_env env, napi_value v) { uint32_t r = 0; napi_get_value_uint32(env, v, &r); return r; }
static double getF64(napi_env env, napi_value v){ double r = 0; napi_get_value_double(env, v, &r); return r; }

/* read a JS string arg into buf, returns byte length */
static u16 getStr(napi_env env, napi_value v, char *buf, size_t cap) {
    size_t len = 0;
    napi_get_value_string_utf8(env, v, buf, cap, &len);
    return (u16)len;
}

/* read a JS string arg into a freshly malloc'd buffer (no fixed-size truncation;
 * names can be up to u16 length). Caller frees. */
static char *getStrA(napi_env env, napi_value v, u16 *len_out) {
    size_t len = 0;
    napi_get_value_string_utf8(env, v, NULL, 0, &len);
    char *buf = malloc(len + 1);
    if (buf) napi_get_value_string_utf8(env, v, buf, len + 1, &len);
    *len_out = (u16)len;
    return buf;
}

static Store *unwrap(napi_env env, napi_value v) { Store *s = NULL; napi_get_value_external(env, v, (void **)&s); return s; }

static void store_finalize(napi_env env, void *data, void *hint) {
    (void)env; (void)hint;
    Store *s = (Store *)data;
    if (s) { if (s->g) graph_close(s->g); if (s->st) st_close(s->st); free(s); }
}

/* ---- args helper ---- */
#define ARGS(n) size_t argc = (n); napi_value argv[(n)]; NCALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL))
#define STORE   Store *s = unwrap(env, argv[0])

/* ---- lifecycle ---- */
static napi_value n_open(napi_env env, napi_callback_info info) {
    ARGS(3);
    char gp[4096], sp[4096];
    getStr(env, argv[0], gp, sizeof gp);
    getStr(env, argv[1], sp, sizeof sp);
    uint32_t initial = getU32(env, argv[2]);
    Store *s = calloc(1, sizeof(Store));
    s->st = st_open(sp, initial);
    s->g  = s->st ? graph_open(gp, s->st, initial) : NULL;
    if (!s->st || !s->g) { if (s->g) graph_close(s->g); if (s->st) st_close(s->st); free(s);
        napi_throw_error(env, NULL, "graph store open failed"); return NULL; }
    napi_value ext; NCALL(napi_create_external(env, s, store_finalize, NULL, &ext));
    return ext;
}
static napi_value n_close(napi_env env, napi_callback_info info) { ARGS(1); STORE; if (s) { if (s->g) graph_close(s->g); if (s->st) st_close(s->st); s->g = NULL; s->st = NULL; } return NULL; }
static napi_value n_sync(napi_env env, napi_callback_info info)  { ARGS(1); STORE; graph_sync(s->g); st_sync(s->st); return NULL; }
static napi_value n_lock_sh(napi_env env, napi_callback_info info){ ARGS(1); STORE; memfile_lock_shared(s->g->mf); st_lock_shared(s->st); return NULL; }
static napi_value n_lock_ex(napi_env env, napi_callback_info info){ ARGS(1); STORE; memfile_lock_exclusive(s->g->mf); st_lock_exclusive(s->st); return NULL; }
static napi_value n_unlock(napi_env env, napi_callback_info info) { ARGS(1); STORE; st_unlock(s->st); memfile_unlock(s->g->mf); return NULL; }
static napi_value n_refresh(napi_env env, napi_callback_info info){ ARGS(1); STORE; memfile_refresh(s->g->mf); memfile_refresh(s->st->mf); return NULL; }

/* ---- entity ops ---- */
static napi_value n_lookup(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; u16 l; char *nm = getStrA(env, argv[1], &l);
    napi_value r = mkU64(env, graph_lookup(s->g, (const u8 *)nm, l));
    free(nm); return r;
}
static napi_value n_create_entity(napi_env env, napi_callback_info info) {
    ARGS(4); STORE;
    u16 nl, tl; char *nm = getStrA(env, argv[1], &nl), *ty = getStrA(env, argv[2], &tl);
    u64 mtime = getU64(env, argv[3]);
    u64 off = graph_create_entity(s->g, (const u8 *)nm, nl, (const u8 *)ty, tl, mtime);
    free(nm); free(ty);
    return mkU64(env, off);
}
static napi_value n_delete_entity(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; napi_value r; napi_get_boolean(env, graph_delete_entity(s->g, getU64(env, argv[1])), &r); return r;
}
/* read_entity -> { name, type, observations[], mtime, obsMtime, structuralVisits, walkerVisits, psi } */
static napi_value n_read_entity(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; u64 off = getU64(env, argv[1]);
    entity_t e; graph_read_entity(s->g, off, &e);
    napi_value o; NCALL(napi_create_object(env, &o));
    u16 l; const u8 *p;
    p = st_get(s->st, e.name_id, &l); napi_value nm; napi_create_string_utf8(env, (const char *)p, l, &nm); napi_set_named_property(env, o, "name", nm);
    p = st_get(s->st, e.type_id, &l); napi_value ty; napi_create_string_utf8(env, (const char *)p, l, &ty); napi_set_named_property(env, o, "type", ty);
    napi_value obs; napi_create_array(env, &obs); u32 oi = 0;
    if (e.obs0_id) { p = st_get(s->st, e.obs0_id, &l); napi_value s0; napi_create_string_utf8(env, (const char *)p, l, &s0); napi_set_element(env, obs, oi++, s0); }
    if (e.obs1_id) { p = st_get(s->st, e.obs1_id, &l); napi_value s1; napi_create_string_utf8(env, (const char *)p, l, &s1); napi_set_element(env, obs, oi++, s1); }
    napi_set_named_property(env, o, "observations", obs);
    napi_set_named_property(env, o, "mtime", mkU64(env, e.mtime));
    napi_set_named_property(env, o, "obsMtime", mkU64(env, e.obs_mtime));
    napi_set_named_property(env, o, "structuralVisits", mkU64(env, e.structural_visits));
    napi_set_named_property(env, o, "walkerVisits", mkU64(env, e.walker_visits));
    napi_set_named_property(env, o, "psi", mkF64(env, e.psi));
    return o;
}
static napi_value n_add_obs(napi_env env, napi_callback_info info) {
    ARGS(4); STORE; char ob[4096]; u16 l = getStr(env, argv[2], ob, sizeof ob);
    napi_value r; napi_get_boolean(env, graph_add_observation(s->g, getU64(env, argv[1]), (const u8 *)ob, l, getU64(env, argv[3])), &r); return r;
}
static napi_value n_remove_obs(napi_env env, napi_callback_info info) {
    ARGS(4); STORE; char ob[4096]; u16 l = getStr(env, argv[2], ob, sizeof ob);
    napi_value r; napi_get_boolean(env, graph_remove_observation(s->g, getU64(env, argv[1]), (const u8 *)ob, l, getU64(env, argv[3])), &r); return r;
}

/* ---- relations ---- */
static napi_value n_create_relation(napi_env env, napi_callback_info info) {
    ARGS(5); STORE; char rt[4096]; u16 l = getStr(env, argv[3], rt, sizeof rt);
    graph_create_relation(s->g, getU64(env, argv[1]), getU64(env, argv[2]), (const u8 *)rt, l, getU64(env, argv[4]));
    return NULL;
}
static napi_value n_delete_relation(napi_env env, napi_callback_info info) {
    ARGS(4); STORE; char rt[4096]; u16 l = getStr(env, argv[3], rt, sizeof rt);
    napi_value r; napi_get_boolean(env, graph_delete_relation(s->g, getU64(env, argv[1]), getU64(env, argv[2]), (const u8 *)rt, l), &r); return r;
}
/* edges(off) -> [{ target, direction, relType, mtime }] */
static napi_value n_edges(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; u64 off = getU64(env, argv[1]);
    u32 ec = graph_edge_count(s->g, off);
    adj_entry_t *es = malloc((ec ? ec : 1) * sizeof(adj_entry_t));
    graph_read_edges(s->g, off, es, ec);
    napi_value arr; napi_create_array(env, &arr);
    for (u32 i = 0; i < ec; i++) {
        napi_value o; napi_create_object(env, &o);
        napi_set_named_property(env, o, "target", mkU64(env, es[i].target_offset));
        napi_set_named_property(env, o, "direction", mkU32(env, es[i].direction));
        u16 l; const u8 *p = st_get(s->st, es[i].rel_type_id, &l);
        napi_value rt; napi_create_string_utf8(env, (const char *)p, l, &rt);
        napi_set_named_property(env, o, "relType", rt);
        napi_set_named_property(env, o, "mtime", mkU64(env, es[i].mtime));
        napi_set_element(env, arr, i, o);
    }
    free(es);
    return arr;
}

/* ---- helper: return a u64[] result via a count-returning op into a BigInt array ---- */
static napi_value u64arr(napi_env env, u64 *buf, u32 n) {
    napi_value arr; napi_create_array(env, &arr);
    for (u32 i = 0; i < n; i++) napi_set_element(env, arr, i, mkU64(env, buf[i]));
    return arr;
}

/* ---- traversal / search / scans (full result sets; TS paginates) ---- */
static napi_value n_neighbors(napi_env env, napi_callback_info info) {
    ARGS(4); STORE; u32 cap = graph_entity_count(s->g) + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_neighbors(s->g, getU64(env, argv[1]), getU32(env, argv[2]), getU32(env, argv[3]), out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}
/* find_path(h, from, to, maxDepth, direction, budgetBytes)
 *   -> { path:[offset], targetReached, budgetExhausted, farthest } */
static napi_value n_find_path(napi_env env, napi_callback_info info) {
    ARGS(6); STORE; u32 cap = graph_entity_count(s->g) + 2; u64 *out = malloc((size_t)cap * 8);
    int tr = 0, be = 0; u64 fa = 0;
    u32 n = graph_find_path_ex(s->g, getU64(env, argv[1]), getU64(env, argv[2]), getU32(env, argv[3]),
                               getU32(env, argv[4]), getU64(env, argv[5]), out, cap, &tr, &be, &fa);
    if (n > cap) n = cap;
    napi_value o; napi_create_object(env, &o);
    napi_set_named_property(env, o, "path", u64arr(env, out, n));
    napi_value b1, b2; napi_get_boolean(env, tr, &b1); napi_get_boolean(env, be, &b2);
    napi_set_named_property(env, o, "targetReached", b1);
    napi_set_named_property(env, o, "budgetExhausted", b2);
    napi_set_named_property(env, o, "farthest", mkU64(env, fa));
    free(out); return o;
}
static napi_value n_search(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; char pat[8192]; getStr(env, argv[1], pat, sizeof pat);
    u32 cap = graph_entity_count(s->g) + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_search(s->g, pat, out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}
/* Pattern validity under the C POSIX ERE engine — the same dialect that matches, so
 * the TS layer keeps its "Invalid regex pattern" contract without JS RegExp. */
static napi_value n_regex_valid(napi_env env, napi_callback_info info) {
    ARGS(1); char pat[8192]; getStr(env, argv[0], pat, sizeof pat);
    napi_value r; napi_get_boolean(env, graph_regex_valid(pat) != 0, &r); return r;
}
static napi_value n_by_type(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; char ty[4096]; u16 l = getStr(env, argv[1], ty, sizeof ty);
    u32 cap = graph_entity_count(s->g) + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_entities_by_type(s->g, (const u8 *)ty, l, out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}
static napi_value n_orphaned(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_entity_count(s->g) + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_orphaned(s->g, out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}
static napi_value n_list_entities(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_entity_count(s->g) + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_list_entities(s->g, out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}
/* distinct type ids -> [string] */
static napi_value n_str_of_ids(napi_env env, Store *s, u32 *ids, u32 n) {
    napi_value arr; napi_create_array(env, &arr);
    for (u32 i = 0; i < n; i++) { u16 l; const u8 *p = st_get(s->st, ids[i], &l); napi_value v; napi_create_string_utf8(env, (const char *)p, l, &v); napi_set_element(env, arr, i, v); }
    return arr;
}
static napi_value n_entity_types(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_entity_count(s->g) + 1; u32 *out = malloc((size_t)cap * 4);
    u32 n = graph_entity_types(s->g, out, cap); napi_value r = n_str_of_ids(env, s, out, n < cap ? n : cap); free(out); return r;
}
static napi_value n_relation_types(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_relation_count(s->g) * 2 + 8; u32 *out = malloc((size_t)cap * 4);
    u32 n = graph_relation_types(s->g, out, cap); napi_value r = n_str_of_ids(env, s, out, n < cap ? n : cap); free(out); return r;
}
static napi_value n_entity_count(napi_env env, napi_callback_info info) { ARGS(1); STORE; return mkU32(env, graph_entity_count(s->g)); }
static napi_value n_relation_count(napi_env env, napi_callback_info info){ ARGS(1); STORE; return mkU32(env, graph_relation_count(s->g)); }
static napi_value n_entity_name(napi_env env, napi_callback_info info) {
    ARGS(2); STORE; u16 l; const u8 *p = graph_entity_name(s->g, getU64(env, argv[1]), &l);
    napi_value v; napi_create_string_utf8(env, (const char *)p, l, &v); return v;
}

/* ---- ranking ---- */
static napi_value n_inc_walker(napi_env env, napi_callback_info info)     { ARGS(2); STORE; graph_inc_walker_visit(s->g, getU64(env, argv[1])); return NULL; }
static napi_value n_inc_structural(napi_env env, napi_callback_info info) { ARGS(2); STORE; graph_inc_structural_visit(s->g, getU64(env, argv[1])); return NULL; }
static napi_value n_structural_total(napi_env env, napi_callback_info info){ ARGS(1); STORE; return mkU64(env, graph_structural_total(s->g)); }
static napi_value n_walker_total(napi_env env, napi_callback_info info)    { ARGS(1); STORE; return mkU64(env, graph_walker_total(s->g)); }
static napi_value n_structural_rank(napi_env env, napi_callback_info info) { ARGS(2); STORE; return mkF64(env, graph_structural_rank(s->g, getU64(env, argv[1]))); }
static napi_value n_walker_rank(napi_env env, napi_callback_info info)     { ARGS(2); STORE; return mkF64(env, graph_walker_rank(s->g, getU64(env, argv[1]))); }
static napi_value n_get_psi(napi_env env, napi_callback_info info)         { ARGS(2); STORE; return mkF64(env, graph_get_psi(s->g, getU64(env, argv[1]))); }
static napi_value n_structural_sample(napi_env env, napi_callback_info info){ ARGS(3); STORE; return mkU32(env, graph_structural_sample(s->g, getU32(env, argv[1]), getF64(env, argv[2]))); }
static napi_value n_merw(napi_env env, napi_callback_info info)            { ARGS(4); STORE; return mkU32(env, graph_compute_merw_psi(s->g, getF64(env, argv[1]), getU32(env, argv[2]), getF64(env, argv[3]))); }
static napi_value n_seed(napi_env env, napi_callback_info info)            { ARGS(2); (void)unwrap(env, argv[0]); graph_seed_rng(getU64(env, argv[1])); return NULL; }
static napi_value n_random_walk(napi_env env, napi_callback_info info) {
    ARGS(6); STORE; u32 depth = getU32(env, argv[2]); u32 cap = depth + 1; u64 *out = malloc((size_t)cap * 8);
    u32 n = graph_random_walk(s->g, getU64(env, argv[1]), depth, getU32(env, argv[3]), getU32(env, argv[4]), getU64(env, argv[5]), out, cap);
    napi_value r = u64arr(env, out, n < cap ? n : cap); free(out); return r;
}

/* ---- validate ---- */
static napi_value n_validate_obs(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_entity_count(s->g) + 1;
    u64 *off = malloc((size_t)cap * 8); u8 *cnt = malloc(cap), *ov = malloc(cap);
    u32 n = graph_validate_obs(s->g, off, cnt, ov, cap); if (n > cap) n = cap;
    napi_value arr; napi_create_array(env, &arr);
    for (u32 i = 0; i < n; i++) { napi_value o; napi_create_object(env, &o);
        napi_set_named_property(env, o, "offset", mkU64(env, off[i]));
        napi_set_named_property(env, o, "count", mkU32(env, cnt[i]));
        napi_set_named_property(env, o, "oversize", mkU32(env, ov[i]));
        napi_set_element(env, arr, i, o); }
    free(off); free(cnt); free(ov); return arr;
}
static napi_value n_validate_dangling(napi_env env, napi_callback_info info) {
    ARGS(1); STORE; u32 cap = graph_entity_count(s->g) + 1;
    u64 *src = malloc((size_t)cap * 8), *tgt = malloc((size_t)cap * 8);
    u32 n = graph_validate_dangling(s->g, src, tgt, cap); if (n > cap) n = cap;
    napi_value arr; napi_create_array(env, &arr);
    for (u32 i = 0; i < n; i++) { napi_value o; napi_create_object(env, &o);
        napi_set_named_property(env, o, "src", mkU64(env, src[i]));
        napi_set_named_property(env, o, "target", mkU64(env, tgt[i]));
        napi_set_element(env, arr, i, o); }
    free(src); free(tgt); return arr;
}

/* ---- migration setters (restore preserved fields after logical rebuild) ---- */
static napi_value n_set_entity_fields(napi_env env, napi_callback_info info) {
    ARGS(7); STORE;
    graph_set_entity_fields(s->g, getU64(env, argv[1]), getU64(env, argv[2]), getU64(env, argv[3]),
                            getU64(env, argv[4]), getU64(env, argv[5]), getF64(env, argv[6]));
    return NULL;
}
static napi_value n_set_totals(napi_env env, napi_callback_info info) {
    ARGS(3); STORE;
    graph_set_totals(s->g, getU64(env, argv[1]), getU64(env, argv[2]));
    return NULL;
}

/* ---- migration serialization: kernel flock on a lock file. Blocks until held;
 *      auto-released on process death (no stale locks). Not Store-bound. ---- */
static napi_value n_lock_path(napi_env env, napi_callback_info info) {
    ARGS(1);
    char p[4096]; getStr(env, argv[0], p, sizeof p);
    int fd = open(p, O_CREAT | O_RDWR, 0644);
    if (fd >= 0 && flock(fd, LOCK_EX) != 0) { close(fd); fd = -1; }
    return mkU32(env, (u32)fd);
}
static napi_value n_unlock_path(napi_env env, napi_callback_info info) {
    ARGS(1);
    int fd = (int)getU32(env, argv[0]);
    if (fd >= 0) { flock(fd, LOCK_UN); close(fd); }
    return NULL;
}

#define EXPORT(name, fn) do { napi_value f; napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, NULL, &f); napi_set_named_property(env, exports, name, f); } while (0)

NAPI_MODULE_INIT() {
    EXPORT("open", n_open); EXPORT("close", n_close); EXPORT("sync", n_sync);
    EXPORT("lockShared", n_lock_sh); EXPORT("lockExclusive", n_lock_ex); EXPORT("unlock", n_unlock); EXPORT("refresh", n_refresh);
    EXPORT("lookup", n_lookup); EXPORT("createEntity", n_create_entity); EXPORT("deleteEntity", n_delete_entity);
    EXPORT("readEntity", n_read_entity); EXPORT("entityName", n_entity_name);
    EXPORT("addObservation", n_add_obs); EXPORT("removeObservation", n_remove_obs);
    EXPORT("createRelation", n_create_relation); EXPORT("deleteRelation", n_delete_relation); EXPORT("edges", n_edges);
    EXPORT("neighbors", n_neighbors); EXPORT("findPath", n_find_path); EXPORT("search", n_search);
    EXPORT("regexValid", n_regex_valid);
    EXPORT("entitiesByType", n_by_type); EXPORT("orphaned", n_orphaned); EXPORT("listEntities", n_list_entities);
    EXPORT("entityTypes", n_entity_types); EXPORT("relationTypes", n_relation_types);
    EXPORT("entityCount", n_entity_count); EXPORT("relationCount", n_relation_count);
    EXPORT("incWalkerVisit", n_inc_walker); EXPORT("incStructuralVisit", n_inc_structural);
    EXPORT("structuralTotal", n_structural_total); EXPORT("walkerTotal", n_walker_total);
    EXPORT("structuralRank", n_structural_rank); EXPORT("walkerRank", n_walker_rank); EXPORT("getPsi", n_get_psi);
    EXPORT("structuralSample", n_structural_sample); EXPORT("computeMerwPsi", n_merw); EXPORT("seedRng", n_seed);
    EXPORT("randomWalk", n_random_walk);
    EXPORT("validateObs", n_validate_obs); EXPORT("validateDangling", n_validate_dangling);
    EXPORT("setEntityFields", n_set_entity_fields); EXPORT("setTotals", n_set_totals);
    EXPORT("lockPath", n_lock_path); EXPORT("unlockPath", n_unlock_path);
    return exports;
}
