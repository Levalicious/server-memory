/*
 * Graph store over a v3 MemoryFile + a shared StringTable.
 *
 * Layouts are the v2 graph schema (ported verbatim from graphfile.ts):
 *   EntityRecord: 72 bytes  (name_id, type_id, adj_offset, mtime, obsMtime,
 *                            obs_count, obs0_id, obs1_id, structural/walker visits, psi)
 *   AdjEntry:     24 bytes  (target<<2|dir, relType_id, mtime); bidirectional storage
 *   NodeLog:      [count,capacity][u64 offsets...]
 *
 * v3 additions:
 *   - Graph header carries a PERSISTENT name index (name_id -> entity offset).
 *   - Graph SCHEMA version lives in the graph header, separate from the memfile
 *     FORMAT version (which memfile.c owns and pins to 3).
 *
 * Refcount discipline (string table): an adj entry owns ONE ref on its relType_id;
 * an entity owns one ref each on name_id, type_id, and its observation ids.
 */
#ifndef GRAPH_H
#define GRAPH_H

#include "memoryfile.h"
#include "stringtable.h"

#define GRAPH_SCHEMA_VERSION 2u
#define ENTITY_RECORD_SIZE   76u   /* [u32 version][72B body] — biscuit-style versioned record */
#define ADJ_ENTRY_SIZE       24u
#define ADJ_HEADER_SIZE      8u
#define NODE_LOG_HEADER_SIZE 8u
#define INITIAL_ADJ_CAPACITY 4u
#define INITIAL_LOG_CAPACITY 256u
#define NI_INITIAL_BUCKETS   4096u

/* direction (low 2 bits of target_and_dir) */
#define DIR_FORWARD  0u
#define DIR_BACKWARD 1u
#define DIR_BIDIR    2u
#define DIR_ANY      255u   /* traversal filter: follow edges of any direction */

typedef struct {
    memfile_t     *mf;            /* graph file */
    stringtable_t *st;            /* shared string table (not owned) */
    u64            header_offset; /* graph header block */
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

/* search: POSIX ERE over name + type + observations; returns all matches */
u32  graph_search(graph_t *g, const char *pattern, u64 *out, u32 max);

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
/* random walk; mode: 1=merw (weighted by psi), 0=uniform; seed 0 = use global rng. Returns path node count. */
u32    graph_random_walk(graph_t *g, u64 start, u32 depth, u32 direction, int merw_mode,
                         u64 seed, u64 *out_path, u32 max_path);

#endif /* GRAPH_H */
