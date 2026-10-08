/*
 * Graph store over a v3 MemoryFile + a shared StringTable.
 *
 * Layouts are the v2 graph schema (ported verbatim from graphfile.ts):
 *   EntityRecord: 72 bytes  (name_id, type_id, adj_offset, mtime, obsMtime,
 *                            obs_count, obs0_id, obs1_id, structural/walker visits, psi)
 *   AdjEntry:     24 bytes  (target<<2|dir, relType_id, mtime); bidirectional storage
 *
 * v3 additions:
 *   - Graph header carries a PERSISTENT name index (name_id -> entity offset).
 *   - Graph SCHEMA version lives in the graph header, separate from the memfile
 *     FORMAT version (which memfile.c owns and pins to 3).
 *
 * 2026-07-13 (schema v3): the node log was ELIMINATED. It was a redundant second
 * entity registry; the name index already lists every entity, so it is now the
 * sole registry and all enumeration/scan ops walk its buckets. Enumeration order
 * is therefore bucket order, NOT insertion order.
 *
 * Refcount discipline (string table): an adj entry owns ONE ref on its relType_id;
 * an entity owns one ref each on name_id, type_id, and its observation ids.
 */
#ifndef GRAPH_H
#define GRAPH_H

#include "memoryfile.h"
#include "stringtable.h"

#define GRAPH_SCHEMA_VERSION 3u    /* v3: node log eliminated; name index is the sole registry */
#define ENTITY_RECORD_SIZE   76u   /* [u32 version][72B body] — biscuit-style versioned record */
#define ADJ_ENTRY_SIZE       24u
#define ADJ_HEADER_SIZE      8u
#define INITIAL_ADJ_CAPACITY 4u
#define NI_INITIAL_BUCKETS   4096u

/* direction (low 2 bits of target_and_dir) */
#define DIR_FORWARD  0u
#define DIR_BACKWARD 1u
#define DIR_BIDIR    2u
#define DIR_ANY      255u   /* traversal filter: follow edges of any direction */

/* In-memory trigram prefilter (re_trigram.h); opaque here. Rebuilt per process
 * at graph_open, maintained on writes — NOT persisted. */
typedef struct ReTrigramLive ReTrigramLive;
/* In-memory type index (type_id -> set of entity offsets); opaque here. Lazily
 * built on first entities_by_type, maintained O(1) on create/delete — NOT
 * persisted. Turns entities_by_type from an O(N) bucket scan into O(result). */
typedef struct TypeIndex TypeIndex;

typedef struct {
    memfile_t     *mf;            /* graph file */
    stringtable_t *st;            /* shared string table (not owned) */
    u64            header_offset; /* graph header block */
    ReTrigramLive *tri;           /* trigram prefilter over name+type+obs (lazy) */
    u64           *tri_dirty;     /* open-addr set of entity offsets changed since sync (0=empty) */
    u8            *tri_dirty_op;  /* parallel op: 1=reindex, 2=remove (last write wins) */
    u32            tri_dirty_cap, tri_dirty_cnt;
    int            live_index_enabled; /* single-writer gate for the live trigram index; OFF by default */
    TypeIndex     *type_idx;      /* type_id -> offset postings (lazy; O(1) maintained) */
} graph_t;

typedef struct {
    u64 offset;
    u32 name_id, type_id;
    u64 adj_offset, mtime, obs_mtime;
    u8  obs_count;
    u32 obs0_id, obs1_id;
    u64 structural_visits, walker_visits;
    double psi;
} entity_t;

typedef struct {
    u64 target_offset;
    u32 direction;
    u32 rel_type_id;
    u64 mtime;
} adj_entry_t;

/* lifecycle */
graph_t *graph_open(const char *graph_path, stringtable_t *st, size_t initial_size);
void     graph_close(graph_t *g);
void     graph_sync(graph_t *g);

/* entity ops */
u64  graph_lookup(graph_t *g, const u8 *name, u16 name_len);   /* entity offset, 0 if absent */
u64  graph_create_entity(graph_t *g, const u8 *name, u16 name_len,
                         const u8 *type, u16 type_len, u64 mtime); /* offset (existing if dup) */
int  graph_delete_entity(graph_t *g, u64 offset);              /* 1 if deleted, 0 if absent */
void graph_read_entity(graph_t *g, u64 offset, entity_t *out);

/* relation ops (bidirectional edges) */
int  graph_create_relation(graph_t *g, u64 from, u64 to, const u8 *rt, u16 rt_len, u64 mtime);
int  graph_delete_relation(graph_t *g, u64 from, u64 to, const u8 *rt, u16 rt_len);

/* adjacency primitives */
void graph_add_edge(graph_t *g, u64 entity_off, const adj_entry_t *e);
int  graph_remove_edge(graph_t *g, u64 entity_off, u64 target_off, u32 rel_type_id, u32 direction);
u32  graph_edge_count(graph_t *g, u64 entity_off);
/* read up to `max` edges into out[]; returns the true edge count (may exceed max). */
u32  graph_read_edges(graph_t *g, u64 entity_off, adj_entry_t *out, u32 max);

u32  graph_entity_count(graph_t *g);

/* observations */
int  graph_add_observation(graph_t *g, u64 off, const u8 *obs, u16 len, u64 mtime);
int  graph_remove_observation(graph_t *g, u64 off, const u8 *obs, u16 len, u64 mtime);

/* scans / enumeration */
const u8 *graph_entity_name(graph_t *g, u64 off, u16 *len_out);
u32  graph_list_entities(graph_t *g, u64 *out, u32 max);
u32  graph_entities_by_type(graph_t *g, const u8 *type, u16 len, u64 *out, u32 max);
u32  graph_orphaned(graph_t *g, u64 *out, u32 max);
u32  graph_relation_count(graph_t *g);
u32  graph_entity_types(graph_t *g, u32 *out, u32 max);     /* distinct type ids */
u32  graph_relation_types(graph_t *g, u32 *out, u32 max);   /* distinct relType ids */

/* search: our regex engine over name + type + observations; returns all matches.
 * Trigram-prefiltered (index caught up lazily at search) then DFA/NFA-verified. */
u32  graph_search(graph_t *g, const char *pattern, u64 *out, u32 max);
/* validity of a pattern under the SAME engine used to match (1 = valid) */
int  graph_regex_valid(const char *pattern);
/* bring the trigram prefilter current with the store (deferred from writes;
 * graph_search calls it automatically). Exposed so index-maintenance cost can
 * be benchmarked on its own. */
void graph_index_sync(graph_t *g);

/* traversal */
u32  graph_neighbors(graph_t *g, u64 start, u32 depth, u32 direction, u64 *out, u32 max);
u32  graph_find_path(graph_t *g, u64 from, u64 to, u32 max_depth, u32 direction,
                     u64 *out_path, u32 max_path);   /* node count; 0 = no path */
/* extended: best-effort path to farthest when target unreachable + byte budget.
 * out params: target_reached, budget_exhausted, farthest (deepest BFS node, 0=none). */
u32  graph_find_path_ex(graph_t *g, u64 from, u64 to, u32 max_depth, u32 direction,
                        u64 budget_bytes, u64 *out_path, u32 max_path,
                        int *target_reached, int *budget_exhausted, u64 *farthest);

/* validate_graph: integrity audit */
u32  graph_validate_obs(graph_t *g, u64 *off, u8 *count, u8 *oversize, u32 max);  /* >2 obs or >140-byte obs */
u32  graph_validate_dangling(graph_t *g, u64 *src, u64 *tgt, u32 max);            /* edge target not a live entity */

/* ranking: visit counting (drives pagerank/llmrank), MERW psi, random walk */
void   graph_seed_rng(u64 seed);
void   graph_inc_structural_visit(graph_t *g, u64 off);
void   graph_inc_walker_visit(graph_t *g, u64 off);
u64    graph_structural_total(graph_t *g);
u64    graph_walker_total(graph_t *g);
double graph_structural_rank(graph_t *g, u64 off);   /* structural_visits / total */
double graph_walker_rank(graph_t *g, u64 off);       /* walker_visits / total */
double graph_get_psi(graph_t *g, u64 off);

/* migration: restore preserved entity fields + global totals (logical rebuild). */
void graph_set_entity_fields(graph_t *g, u64 off, u64 mtime, u64 obs_mtime,
                             u64 structural_visits, u64 walker_visits, double psi);
void graph_set_totals(graph_t *g, u64 structural_total, u64 walker_total);
u32    graph_structural_sample(graph_t *g, u32 iterations, double damping);  /* MC pagerank; total visits */
u32    graph_compute_merw_psi(graph_t *g, double alpha, u32 max_iter, double tol);  /* iters run */
/* random walk; mode: 1=merw (weighted by psi), 0=uniform; seed 0 = use global rng.
 * avoid_cycles: 1 = self-avoiding (never revisits a node; stops early when every
 * neighbor is already on the path). Returns path node count.
 * out_uniform_steps (optional): number of merw-requested steps that fell back to
 * uniform sampling because psi weighting was unavailable at that step. */
u32    graph_random_walk(graph_t *g, u64 start, u32 depth, u32 direction, int merw_mode,
                         u64 seed, int avoid_cycles, u64 *out_path, u32 max_path,
                         u32 *out_uniform_steps);

/* Single-writer gate for the live trigram prefilter (KB: trigram accel is
 * gated on single-writer until the owner daemon). Default OFF. */
void   graph_set_live_index(graph_t *g, int on);

#endif /* GRAPH_H */
