#include "graph.h"

#include <stdlib.h>
#include <string.h>
#include <regex.h>
#include "entity.h"   /* versioned record schema (single source of truth) */

#define GRAPH_HEADER_SIZE 40u   /* node_log_off, structural_total, walker_total, name_index_off, schema_ver, pad */

/* graph header field offsets */
#define GH_NODE_LOG_OFF     0
#define GH_STRUCTURAL_TOTAL 8
#define GH_WALKER_TOTAL     16
#define GH_NAME_INDEX_OFF   24
#define GH_SCHEMA_VERSION   32

/* entity record on-disk layout: [u32 version][Entity_v1 body]. Body fields are
 * at sizeof(u32) + offsetof(Entity, field); the static_assert binds these to the
 * entity.h single-source struct so they cannot silently drift. */
#define E_VERSION 0
#define E_NAME_ID 4
#define E_TYPE_ID 8
#define E_ADJ     12
#define E_MTIME   20
#define E_OBSM    28
#define E_OBSCNT  36
#define E_OBS0    40
#define E_OBS1    44
#define E_SVIS    52
#define E_WVIS    60
#define E_PSI     68
_Static_assert(sizeof(u32) + sizeof(Entity) == ENTITY_RECORD_SIZE, "entity record size");
_Static_assert(E_NAME_ID == sizeof(u32) + offsetof(Entity, name_id), "name_id offset");
_Static_assert(E_ADJ     == sizeof(u32) + offsetof(Entity, adj_offset), "adj offset");
_Static_assert(E_PSI     == sizeof(u32) + offsetof(Entity, psi), "psi offset");

/* adj entry field offsets */
#define AE_TARGET_DIR 0
#define AE_RELTYPE    8
#define AE_MTIME      16

/* name-index block: [u32 bucket_count][u32 ni_count][bucket{u32 name_id,u32 pad,u64 offset}...] */
#define NI_BUCKET_SIZE 16u

/* ---- aliasing-safe field access ---- */
static inline u8  rdu8 (memfile_t *mf, u64 o) { return *(const u8 *)memfile_ptr(mf, o); }
static inline u32 rdu32(memfile_t *mf, u64 o) { u32 v; memcpy(&v, memfile_ptr(mf, o), 4); return v; }
static inline u64 rdu64(memfile_t *mf, u64 o) { u64 v; memcpy(&v, memfile_ptr(mf, o), 8); return v; }
static inline double rdf64(memfile_t *mf, u64 o) { double v; memcpy(&v, memfile_ptr(mf, o), 8); return v; }
static inline void wru8 (memfile_t *mf, u64 o, u8  v) { *(u8 *)memfile_ptr(mf, o) = v; }
static inline void wru32(memfile_t *mf, u64 o, u32 v) { memcpy(memfile_ptr(mf, o), &v, 4); }
static inline void wru64(memfile_t *mf, u64 o, u64 v) { memcpy(memfile_ptr(mf, o), &v, 8); }
static inline void wrf64(memfile_t *mf, u64 o, double v) { memcpy(memfile_ptr(mf, o), &v, 8); }

static inline u32 hash32(u32 x) {
    x ^= x >> 16; x *= 0x7feb352du; x ^= x >> 15; x *= 0x846ca68bu; x ^= x >> 16; return x;
}

/* ---- graph header accessors ---- */
static inline u64 node_log_off(graph_t *g)   { return rdu64(g->mf, g->header_offset + GH_NODE_LOG_OFF); }
static inline void set_node_log_off(graph_t *g, u64 v) { wru64(g->mf, g->header_offset + GH_NODE_LOG_OFF, v); }
static inline u64 name_index_off(graph_t *g) { return rdu64(g->mf, g->header_offset + GH_NAME_INDEX_OFF); }
static inline void set_name_index_off(graph_t *g, u64 v) { wru64(g->mf, g->header_offset + GH_NAME_INDEX_OFF, v); }

/* ======================================================================
 * Persistent name index (name_id -> entity offset)
 * ====================================================================== */

static inline u64 ni_bucket_pos(u64 idx, u32 slot) { return idx + 8 + (u64)slot * NI_BUCKET_SIZE; }

static u64 ni_lookup(graph_t *g, u32 name_id) {
    memfile_t *mf = g->mf;
    u64 idx = name_index_off(g);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash32(name_id) % bc;
    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 base = ni_bucket_pos(idx, slot);
        u64 off = rdu64(mf, base + 8);
        if (off == 0) return 0;
        if (rdu32(mf, base + 0) == name_id) return off;
    }
    return 0;
}

static void ni_rehash(graph_t *g, u32 new_bc);

static void ni_insert(graph_t *g, u32 name_id, u64 offset) {
    memfile_t *mf = g->mf;
    u64 idx = name_index_off(g);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash32(name_id) % bc;
    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 base = ni_bucket_pos(idx, slot);
        if (rdu64(mf, base + 8) == 0) {
            wru32(mf, base + 0, name_id);
            wru64(mf, base + 8, offset);
            u32 cnt = rdu32(mf, idx + 4) + 1;
            wru32(mf, idx + 4, cnt);
            if ((u64)cnt * 10 > (u64)bc * 7) ni_rehash(g, bc * 2);
            return;
        }
    }
}

static int ni_needs_reloc(u32 natural, u32 empty, u32 current) {
    if (natural <= current) return natural <= empty && empty < current;
    return natural <= empty || empty < current;
}
static void ni_fixup(graph_t *g, u32 removed, u32 bc) {
    memfile_t *mf = g->mf;
    u64 idx = name_index_off(g);
    u32 slot = (removed + 1) % bc;
    for (;;) {
        u64 base = ni_bucket_pos(idx, slot);
        if (rdu64(mf, base + 8) == 0) break;
        u32 natural = hash32(rdu32(mf, base + 0)) % bc;
        if (ni_needs_reloc(natural, removed, slot)) {
            u64 rbase = ni_bucket_pos(idx, removed);
            wru32(mf, rbase + 0, rdu32(mf, base + 0));
            wru64(mf, rbase + 8, rdu64(mf, base + 8));
            wru64(mf, base + 8, 0);
            removed = slot;
        }
        slot = (slot + 1) % bc;
    }
}
static void ni_remove(graph_t *g, u32 name_id) {
    memfile_t *mf = g->mf;
    u64 idx = name_index_off(g);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash32(name_id) % bc;
    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 base = ni_bucket_pos(idx, slot);
        if (rdu64(mf, base + 8) == 0) return;
        if (rdu32(mf, base + 0) == name_id) {
            wru64(mf, base + 8, 0);
            wru32(mf, idx + 4, rdu32(mf, idx + 4) - 1);
            ni_fixup(g, slot, bc);
            return;
        }
    }
}
static void ni_rehash(graph_t *g, u32 new_bc) {
    memfile_t *mf = g->mf;
    u64 old_idx = name_index_off(g);
    u32 old_bc = rdu32(mf, old_idx + 0);
    u32 cnt = rdu32(mf, old_idx + 4);
    u64 new_size = 8 + (u64)new_bc * NI_BUCKET_SIZE;
    u64 new_idx = memfile_alloc(mf, new_size);
    if (!new_idx) return;
    memset(memfile_ptr(mf, new_idx), 0, new_size);
    wru32(mf, new_idx + 0, new_bc);
    wru32(mf, new_idx + 4, cnt);
    for (u32 i = 0; i < old_bc; i++) {
        u64 obase = ni_bucket_pos(old_idx, i);
        u64 off = rdu64(mf, obase + 8);
        if (off == 0) continue;
        u32 nid = rdu32(mf, obase + 0);
        u32 b = hash32(nid) % new_bc;
        for (u32 j = 0; j < new_bc; j++) {
            u32 s = (b + j) % new_bc;
            u64 nbase = ni_bucket_pos(new_idx, s);
            if (rdu64(mf, nbase + 8) == 0) { wru32(mf, nbase + 0, nid); wru64(mf, nbase + 8, off); break; }
        }
    }
    set_name_index_off(g, new_idx);
    memfile_free(mf, old_idx, 8 + (u64)old_bc * NI_BUCKET_SIZE);
}

/* ======================================================================
 * Entity records
 * ====================================================================== */

void graph_read_entity(graph_t *g, u64 off, entity_t *e) {
    memfile_t *mf = g->mf;
    e->offset = off;
    e->name_id = rdu32(mf, off + E_NAME_ID);
    e->type_id = rdu32(mf, off + E_TYPE_ID);
    e->adj_offset = rdu64(mf, off + E_ADJ);
    e->mtime = rdu64(mf, off + E_MTIME);
    e->obs_mtime = rdu64(mf, off + E_OBSM);
    e->obs_count = rdu8(mf, off + E_OBSCNT);
    e->obs0_id = rdu32(mf, off + E_OBS0);
    e->obs1_id = rdu32(mf, off + E_OBS1);
    e->structural_visits = rdu64(mf, off + E_SVIS);
    e->walker_visits = rdu64(mf, off + E_WVIS);
    e->psi = rdf64(mf, off + E_PSI);
}

/* ======================================================================
 * Node log
 * ====================================================================== */

static void log_append(graph_t *g, u64 ent_off) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    u32 cap = rdu32(mf, log + 4);
    if (count < cap) {
        wru64(mf, log + NODE_LOG_HEADER_SIZE + (u64)count * 8, ent_off);
        wru32(mf, log + 0, count + 1);
        return;
    }
    u32 newcap = cap * 2;
    u64 newlog = memfile_alloc(mf, NODE_LOG_HEADER_SIZE + (u64)newcap * 8);
    if (!newlog) return;
    wru32(mf, newlog + 0, count + 1);
    wru32(mf, newlog + 4, newcap);
    if (count) memcpy(memfile_ptr(mf, newlog + NODE_LOG_HEADER_SIZE),
                      memfile_ptr(mf, log + NODE_LOG_HEADER_SIZE), (u64)count * 8);
    wru64(mf, newlog + NODE_LOG_HEADER_SIZE + (u64)count * 8, ent_off);
    memfile_free(mf, log, NODE_LOG_HEADER_SIZE + (u64)cap * 8);
    set_node_log_off(g, newlog);
}

static void log_remove(graph_t *g, u64 ent_off) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    for (u32 i = 0; i < count; i++) {
        if (rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8) == ent_off) {
            u32 last = count - 1;
            if (i < last)
                wru64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8,
                      rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)last * 8));
            wru32(mf, log + 0, last);
            return;
        }
    }
}

/* ======================================================================
 * Adjacency
 * ====================================================================== */

static void write_adj_entry(memfile_t *mf, u64 pos, const adj_entry_t *e) {
    wru64(mf, pos + AE_TARGET_DIR, (e->target_offset << 2) | (e->direction & 3u));
    wru32(mf, pos + AE_RELTYPE, e->rel_type_id);
    wru32(mf, pos + 12, 0);
    wru64(mf, pos + AE_MTIME, e->mtime);
}

void graph_add_edge(graph_t *g, u64 ent_off, const adj_entry_t *e) {
    memfile_t *mf = g->mf;
    u64 adj = rdu64(mf, ent_off + E_ADJ);
    if (adj == 0) {
        u64 sz = ADJ_HEADER_SIZE + (u64)INITIAL_ADJ_CAPACITY * ADJ_ENTRY_SIZE;
        u64 noff = memfile_alloc(mf, sz);
        if (!noff) return;
        wru32(mf, noff + 0, 1);
        wru32(mf, noff + 4, INITIAL_ADJ_CAPACITY);
        write_adj_entry(mf, noff + ADJ_HEADER_SIZE, e);
        wru64(mf, ent_off + E_ADJ, noff);
        return;
    }
    u32 count = rdu32(mf, adj + 0);
    u32 cap = rdu32(mf, adj + 4);
    if (count < cap) {
        write_adj_entry(mf, adj + ADJ_HEADER_SIZE + (u64)count * ADJ_ENTRY_SIZE, e);
        wru32(mf, adj + 0, count + 1);
        return;
    }
    u32 newcap = cap * 2;
    u64 nadj = memfile_alloc(mf, ADJ_HEADER_SIZE + (u64)newcap * ADJ_ENTRY_SIZE);
    if (!nadj) return;
    wru32(mf, nadj + 0, count + 1);
    wru32(mf, nadj + 4, newcap);
    if (count) memcpy(memfile_ptr(mf, nadj + ADJ_HEADER_SIZE),
                      memfile_ptr(mf, adj + ADJ_HEADER_SIZE), (u64)count * ADJ_ENTRY_SIZE);
    write_adj_entry(mf, nadj + ADJ_HEADER_SIZE + (u64)count * ADJ_ENTRY_SIZE, e);
    memfile_free(mf, adj, ADJ_HEADER_SIZE + (u64)cap * ADJ_ENTRY_SIZE);
    wru64(mf, ent_off + E_ADJ, nadj);
}

int graph_remove_edge(graph_t *g, u64 ent_off, u64 target_off, u32 rel_type_id, u32 direction) {
    memfile_t *mf = g->mf;
    u64 adj = rdu64(mf, ent_off + E_ADJ);
    if (adj == 0) return 0;
    u32 count = rdu32(mf, adj + 0);
    u64 packed = (target_off << 2) | (direction & 3u);
    for (u32 i = 0; i < count; i++) {
        u64 base = adj + ADJ_HEADER_SIZE + (u64)i * ADJ_ENTRY_SIZE;
        if (rdu64(mf, base + AE_TARGET_DIR) == packed && rdu32(mf, base + AE_RELTYPE) == rel_type_id) {
            u32 last = count - 1;
            if (i < last)
                memcpy(memfile_ptr(mf, base),
                       memfile_ptr(mf, adj + ADJ_HEADER_SIZE + (u64)last * ADJ_ENTRY_SIZE), ADJ_ENTRY_SIZE);
            wru32(mf, adj + 0, last);
            return 1;
        }
    }
    return 0;
}

u32 graph_edge_count(graph_t *g, u64 ent_off) {
    u64 adj = rdu64(g->mf, ent_off + E_ADJ);
    return adj ? rdu32(g->mf, adj + 0) : 0;
}

u32 graph_read_edges(graph_t *g, u64 ent_off, adj_entry_t *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 adj = rdu64(mf, ent_off + E_ADJ);
    if (adj == 0) return 0;
    u32 count = rdu32(mf, adj + 0);
    u32 n = count < max ? count : max;
    for (u32 i = 0; i < n; i++) {
        u64 base = adj + ADJ_HEADER_SIZE + (u64)i * ADJ_ENTRY_SIZE;
        u64 packed = rdu64(mf, base + AE_TARGET_DIR);
        out[i].target_offset = packed >> 2;
        out[i].direction = (u32)(packed & 3u);
        out[i].rel_type_id = rdu32(mf, base + AE_RELTYPE);
        out[i].mtime = rdu64(mf, base + AE_MTIME);
    }
    return count;
}

/* ======================================================================
 * Operations
 * ====================================================================== */

u64 graph_lookup(graph_t *g, const u8 *name, u16 name_len) {
    u64 nid = st_find(g->st, name, name_len);
    if (!nid) return 0;
    return ni_lookup(g, (u32)nid);
}

u64 graph_create_entity(graph_t *g, const u8 *name, u16 name_len,
                        const u8 *type, u16 type_len, u64 mtime) {
    u64 existing = graph_lookup(g, name, name_len);
    if (existing) return existing;                 /* dedup: no new refs */

    u64 nid = st_intern(g->st, name, name_len);
    u64 tid = st_intern(g->st, type, type_len);
    u64 off = memfile_alloc(g->mf, ENTITY_RECORD_SIZE);
    if (!off) return 0;

    memset(memfile_ptr(g->mf, off), 0, ENTITY_RECORD_SIZE);
    wru32(g->mf, off + E_VERSION, ENTITY_CURRENT);
    wru32(g->mf, off + E_NAME_ID, (u32)nid);
    wru32(g->mf, off + E_TYPE_ID, (u32)tid);
    wru64(g->mf, off + E_MTIME, mtime);
    /* obs_mtime stays 0 until an observation is added (matches old no-obs => 0). */

    log_append(g, off);
    ni_insert(g, (u32)nid, off);
    return off;
}

int graph_delete_entity(graph_t *g, u64 off) {
    entity_t e;
    graph_read_entity(g, off, &e);

    /* edges: release every relType ref this entity's edges touch, drop mirrors */
    u32 ec = graph_edge_count(g, off);
    if (ec) {
        u32 cap = rdu32(g->mf, e.adj_offset + 4);
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, off, es, ec);
        for (u32 k = 0; k < ec; k++) {
            st_release(g->st, es[k].rel_type_id);          /* this entity's own entry */
            if (es[k].target_offset != off) {              /* not a self-loop */
                u32 rev = (es[k].direction == DIR_FORWARD) ? DIR_BACKWARD : DIR_FORWARD;
                if (graph_remove_edge(g, es[k].target_offset, off, es[k].rel_type_id, rev))
                    st_release(g->st, es[k].rel_type_id);  /* neighbor's mirror entry */
            }
        }
        free(es);
        memfile_free(g->mf, e.adj_offset, ADJ_HEADER_SIZE + (u64)cap * ADJ_ENTRY_SIZE);
    }

    ni_remove(g, e.name_id);
    log_remove(g, off);

    st_release(g->st, e.name_id);
    st_release(g->st, e.type_id);
    if (e.obs0_id) st_release(g->st, e.obs0_id);
    if (e.obs1_id) st_release(g->st, e.obs1_id);

    memfile_free(g->mf, off, ENTITY_RECORD_SIZE);
    return 1;
}

int graph_create_relation(graph_t *g, u64 from, u64 to, const u8 *rt, u16 rt_len, u64 mtime) {
    u64 rtid_f = st_intern(g->st, rt, rt_len);             /* ref for the forward entry */
    adj_entry_t f = { to, DIR_FORWARD, (u32)rtid_f, mtime };
    graph_add_edge(g, from, &f);

    u64 rtid_b = st_intern(g->st, rt, rt_len);             /* ref for the backward entry */
    adj_entry_t b = { from, DIR_BACKWARD, (u32)rtid_b, mtime };
    graph_add_edge(g, to, &b);
    wru64(g->mf, from + E_MTIME, mtime);   /* a new relation marks the source entity modified */
    return 1;
}

int graph_delete_relation(graph_t *g, u64 from, u64 to, const u8 *rt, u16 rt_len) {
    u64 rtid = st_find(g->st, rt, rt_len);
    if (!rtid) return 0;
    int removed = 0;
    if (graph_remove_edge(g, from, to, (u32)rtid, DIR_FORWARD)) { st_release(g->st, rtid); removed = 1; }
    if (graph_remove_edge(g, to, from, (u32)rtid, DIR_BACKWARD)) { st_release(g->st, rtid); removed = 1; }
    return removed;
}

u32 graph_entity_count(graph_t *g) {
    return rdu32(g->mf, node_log_off(g) + 0);
}

/* ======================================================================
 * Observations
 * ====================================================================== */

int graph_add_observation(graph_t *g, u64 off, const u8 *obs, u16 len, u64 mtime) {
    memfile_t *mf = g->mf;
    u8 cnt = rdu8(mf, off + E_OBSCNT);
    if (cnt >= 2) return 0;
    u64 oid = st_intern(g->st, obs, len);
    if (cnt == 0) wru32(mf, off + E_OBS0, (u32)oid);
    else          wru32(mf, off + E_OBS1, (u32)oid);
    wru8(mf, off + E_OBSCNT, (u8)(cnt + 1));
    wru64(mf, off + E_OBSM, mtime);
    wru64(mf, off + E_MTIME, mtime);
    return 1;
}

int graph_remove_observation(graph_t *g, u64 off, const u8 *obs, u16 len, u64 mtime) {
    memfile_t *mf = g->mf;
    u64 oid = st_find(g->st, obs, len);
    if (!oid) return 0;
    u32 o0 = rdu32(mf, off + E_OBS0), o1 = rdu32(mf, off + E_OBS1);
    if (o0 == (u32)oid) {
        st_release(g->st, o0);
        wru32(mf, off + E_OBS0, o1);
        wru32(mf, off + E_OBS1, 0);
    } else if (o1 == (u32)oid) {
        st_release(g->st, o1);
        wru32(mf, off + E_OBS1, 0);
    } else {
        return 0;
    }
    wru8(mf, off + E_OBSCNT, (u8)(rdu8(mf, off + E_OBSCNT) - 1));
    wru64(mf, off + E_OBSM, mtime);
    wru64(mf, off + E_MTIME, mtime);
    return 1;
}

/* ======================================================================
 * Scans / enumeration
 * ====================================================================== */

const u8 *graph_entity_name(graph_t *g, u64 off, u16 *len_out) {
    return st_get(g->st, rdu32(g->mf, off + E_NAME_ID), len_out);
}

u32 graph_list_entities(graph_t *g, u64 *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    u32 n = count < max ? count : max;
    for (u32 i = 0; i < n; i++) out[i] = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
    return count;
}

u32 graph_entities_by_type(graph_t *g, const u8 *type, u16 len, u64 *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 tid = st_find(g->st, type, len);
    if (!tid) return 0;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0), found = 0;
    for (u32 i = 0; i < count; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        if (rdu32(mf, e + E_TYPE_ID) == (u32)tid) { if (found < max) out[found] = e; found++; }
    }
    return found;
}

u32 graph_orphaned(graph_t *g, u64 *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0), found = 0;
    for (u32 i = 0; i < count; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        if (graph_edge_count(g, e) == 0) { if (found < max) out[found] = e; found++; }
    }
    return found;
}

u32 graph_relation_count(graph_t *g) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    u64 edges = 0;
    for (u32 i = 0; i < count; i++)
        edges += graph_edge_count(g, rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8));
    return (u32)(edges / 2);   /* each relation = forward + backward entry */
}

static int cmp_u32(const void *a, const void *b) {
    u32 x = *(const u32 *)a, y = *(const u32 *)b;
    return (x > y) - (x < y);
}

u32 graph_entity_types(graph_t *g, u32 *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    if (count == 0) return 0;
    u32 *tmp = malloc((size_t)count * sizeof(u32));
    for (u32 i = 0; i < count; i++)
        tmp[i] = rdu32(mf, rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8) + E_TYPE_ID);
    qsort(tmp, count, sizeof(u32), cmp_u32);
    u32 distinct = 0;
    for (u32 i = 0; i < count; i++)
        if (i == 0 || tmp[i] != tmp[i - 1]) { if (distinct < max) out[distinct] = tmp[i]; distinct++; }
    free(tmp);
    return distinct;
}

u32 graph_relation_types(graph_t *g, u32 *out, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0);
    u64 total = 0;
    for (u32 i = 0; i < count; i++)
        total += graph_edge_count(g, rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8));
    if (total == 0) return 0;
    u32 *tmp = malloc((size_t)total * sizeof(u32));
    u64 k = 0;
    for (u32 i = 0; i < count; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        u64 adj = rdu64(mf, e + E_ADJ);
        if (!adj) continue;
        u32 ec = rdu32(mf, adj + 0);
        for (u32 j = 0; j < ec; j++)
            tmp[k++] = rdu32(mf, adj + ADJ_HEADER_SIZE + (u64)j * ADJ_ENTRY_SIZE + AE_RELTYPE);
    }
    qsort(tmp, total, sizeof(u32), cmp_u32);
    u32 distinct = 0;
    for (u64 i = 0; i < total; i++)
        if (i == 0 || tmp[i] != tmp[i - 1]) { if (distinct < max) out[distinct] = tmp[i]; distinct++; }
    free(tmp);
    return distinct;
}

/* ======================================================================
 * Search (POSIX ERE over name + type + observations); full result set
 * ====================================================================== */

static int match_id(graph_t *g, regex_t *re, u32 id) {
    if (!id) return 0;
    u16 len; const u8 *s = st_get(g->st, id, &len);
    regmatch_t pm; pm.rm_so = 0; pm.rm_eo = (regoff_t)len;
    return regexec(re, (const char *)s, 0, &pm, REG_STARTEND) == 0;
}

u32 graph_search(graph_t *g, const char *pattern, u64 *out, u32 max) {
    regex_t re;
    if (regcomp(&re, pattern, REG_EXTENDED) != 0) return 0;   /* POSIX ERE, case-sensitive; invalid pattern -> no matches */
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 count = rdu32(mf, log + 0), found = 0;
    for (u32 i = 0; i < count; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        if (match_id(g, &re, rdu32(mf, e + E_NAME_ID)) ||
            match_id(g, &re, rdu32(mf, e + E_TYPE_ID)) ||
            match_id(g, &re, rdu32(mf, e + E_OBS0))   ||
            match_id(g, &re, rdu32(mf, e + E_OBS1))) {
            if (found < max) out[found] = e;
            found++;
        }
    }
    regfree(&re);
    return found;
}

/* Validity of a search pattern under the SAME engine that matches it (POSIX ERE),
 * so the TS layer can surface "Invalid regex pattern" without a second, divergent
 * regex dialect (JS RegExp). Returns 1 iff regcomp(REG_EXTENDED) accepts it. */
int graph_regex_valid(const char *pattern) {
    regex_t re;
    if (regcomp(&re, pattern, REG_EXTENDED) != 0) return 0;
    regfree(&re);
    return 1;
}

/* ======================================================================
 * Traversal (BFS) — open-addressing u64->u64 map for visited / parent.
 * Entity offsets are never 0 (records live above the header), so 0 = empty slot.
 * ====================================================================== */

typedef struct { u64 *k, *v; u32 cap, cnt; } omap;

static void omap_init(omap *m, u32 cap) { m->cap = cap; m->cnt = 0; m->k = calloc(cap, 8); m->v = calloc(cap, 8); }
static void omap_free(omap *m) { free(m->k); free(m->v); }
static inline u32 omap_h(u64 x, u32 cap) { return (u32)(((x * 0x9e3779b97f4a7c15ull) >> 32) % cap); }
static int omap_has(omap *m, u64 key) {
    for (u32 i = omap_h(key, m->cap); m->k[i]; i = (i + 1) % m->cap) if (m->k[i] == key) return 1;
    return 0;
}
static u64 omap_get(omap *m, u64 key) {
    for (u32 i = omap_h(key, m->cap); m->k[i]; i = (i + 1) % m->cap) if (m->k[i] == key) return m->v[i];
    return 0;
}
static void omap_grow(omap *m) {
    u32 nc = m->cap * 2; u64 *nk = calloc(nc, 8), *nv = calloc(nc, 8);
    for (u32 j = 0; j < m->cap; j++) if (m->k[j]) {
        u32 i = omap_h(m->k[j], nc); while (nk[i]) i = (i + 1) % nc;
        nk[i] = m->k[j]; nv[i] = m->v[j];
    }
    free(m->k); free(m->v); m->k = nk; m->v = nv; m->cap = nc;
}
static void omap_put(omap *m, u64 key, u64 val) {
    if ((u64)(m->cnt + 1) * 10 > (u64)m->cap * 7) omap_grow(m);
    u32 i = omap_h(key, m->cap);
    while (m->k[i]) { if (m->k[i] == key) { m->v[i] = val; return; } i = (i + 1) % m->cap; }
    m->k[i] = key; m->v[i] = val; m->cnt++;
}

static inline int dir_match(u32 want, u32 have) { return want == DIR_ANY || have == want; }

u32 graph_neighbors(graph_t *g, u64 start, u32 depth, u32 direction, u64 *out, u32 max) {
    omap seen; omap_init(&seen, 256);
    omap_put(&seen, start, 1);
    u32 qcap = 256, head = 0, tail = 0;
    u64 *q = malloc(qcap * 8); u32 *qd = malloc(qcap * 4);
    q[tail] = start; qd[tail] = 0; tail++;
    u32 found = 0;
    while (head < tail) {
        u64 f = q[head]; u32 d = qd[head]; head++;
        if (d >= depth) continue;
        u32 ec = graph_edge_count(g, f);
        if (!ec) continue;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, f, es, ec);
        for (u32 k = 0; k < ec; k++) {
            if (!dir_match(direction, es[k].direction)) continue;
            u64 t = es[k].target_offset;
            if (!omap_has(&seen, t)) {
                omap_put(&seen, t, 1);
                if (found < max) out[found] = t;
                found++;
                if (tail == qcap) { qcap *= 2; q = realloc(q, qcap * 8); qd = realloc(qd, qcap * 4); }
                q[tail] = t; qd[tail] = d + 1; tail++;
            }
        }
        free(es);
    }
    free(q); free(qd); omap_free(&seen);
    return found;
}

/* BFS shortest path with a best-effort fallback + byte budget (β-contract).
 *
 *  - target_reached: BFS arrived at `to`. out_path = from..to.
 *  - else best-effort: out_path = from..farthest (deepest BFS node), so the
 *    caller has a retry anchor. farthest = 0 if no edge was expanded.
 *  - budget_exhausted: stopped because per-node bytes crossed budget_bytes.
 *    The target check fires BEFORE the budget check, so a discovery that IS
 *    the target succeeds even at budget 0. Per-node cost is the real C BFS
 *    bookkeeping (~28B: omap entry + queue) plus the name/relType bytes we
 *    touch — the same mechanism as the old JS budget, C-native sizing.
 */
u32 graph_find_path_ex(graph_t *g, u64 from, u64 to, u32 max_depth, u32 direction,
                       u64 budget_bytes, u64 *out_path, u32 max_path,
                       int *target_reached, int *budget_exhausted, u64 *farthest) {
    *target_reached = 0; *budget_exhausted = 0; *farthest = 0;
    if (from == to) { if (max_path >= 1) out_path[0] = from; *target_reached = 1; return 1; }

    omap parent; omap_init(&parent, 256);
    omap_put(&parent, from, from);   /* root sentinel */
    u32 qcap = 256, head = 0, tail = 0;
    u64 *q = malloc(qcap * 8); u32 *qd = malloc(qcap * 4);
    q[tail] = from; qd[tail] = 0; tail++;

    u16 fl; (void)graph_entity_name(g, from, &fl);
    u64 bytes_used = (u64)fl + 28;
    int found = 0, exhausted = 0;

    while (head < tail && !found && !exhausted) {
        u64 f = q[head]; u32 d = qd[head]; head++;
        if (d >= max_depth) continue;
        u32 ec = graph_edge_count(g, f);
        if (!ec) continue;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, f, es, ec);
        for (u32 k = 0; k < ec; k++) {
            if (!dir_match(direction, es[k].direction)) continue;
            u64 t = es[k].target_offset;
            if (omap_has(&parent, t)) continue;
            omap_put(&parent, t, f);
            *farthest = t;
            if (t == to) { found = 1; break; }              /* target check first */
            u16 nl; (void)graph_entity_name(g, t, &nl);
            u16 rl; (void)st_get(g->st, es[k].rel_type_id, &rl);
            bytes_used += (u64)nl + (u64)rl + 28;
            if (bytes_used >= budget_bytes) { exhausted = 1; break; } /* then budget */
            if (tail == qcap) { qcap *= 2; q = realloc(q, qcap * 8); qd = realloc(qd, qcap * 4); }
            q[tail] = t; qd[tail] = d + 1; tail++;
        }
        free(es);
    }

    u32 n = 0;
    u64 endp = found ? to : *farthest;
    if (endp) {
        u64 *rev = malloc((size_t)max_path * 8);
        for (u64 cur = endp; cur != from && n < max_path; cur = omap_get(&parent, cur)) rev[n++] = cur;
        if (n < max_path) rev[n++] = from;
        for (u32 i = 0; i < n; i++) out_path[i] = rev[n - 1 - i];
        free(rev);
    }
    free(q); free(qd); omap_free(&parent);
    *target_reached = found; *budget_exhausted = exhausted;
    return n;
}

u32 graph_find_path(graph_t *g, u64 from, u64 to, u32 max_depth, u32 direction,
                    u64 *out_path, u32 max_path) {
    int tr, be; u64 fa;
    return graph_find_path_ex(g, from, to, max_depth, direction, (u64)-1,
                              out_path, max_path, &tr, &be, &fa);
}

/* ======================================================================
 * Ranking: visit counting (pagerank/llmrank), MERW psi, random walk
 * ====================================================================== */

static u64 g_rng = 0x9e3779b97f4a7c15ull;
void graph_seed_rng(u64 seed) { g_rng = seed ? seed : 0x9e3779b97f4a7c15ull; }
static inline u64 rng_u64(u64 *s) { u64 x = *s; x ^= x << 13; x ^= x >> 7; x ^= x << 17; return *s = x; }
static inline double rng_d(u64 *s) { return (double)(rng_u64(s) >> 11) * (1.0 / 9007199254740992.0); }  /* [0,1) */

void graph_inc_structural_visit(graph_t *g, u64 off) {
    wru64(g->mf, off + E_SVIS, rdu64(g->mf, off + E_SVIS) + 1);
    u64 hp = g->header_offset + GH_STRUCTURAL_TOTAL;
    wru64(g->mf, hp, rdu64(g->mf, hp) + 1);
}
void graph_inc_walker_visit(graph_t *g, u64 off) {
    wru64(g->mf, off + E_WVIS, rdu64(g->mf, off + E_WVIS) + 1);
    u64 hp = g->header_offset + GH_WALKER_TOTAL;
    wru64(g->mf, hp, rdu64(g->mf, hp) + 1);
}
u64 graph_structural_total(graph_t *g) { return rdu64(g->mf, g->header_offset + GH_STRUCTURAL_TOTAL); }
u64 graph_walker_total(graph_t *g)     { return rdu64(g->mf, g->header_offset + GH_WALKER_TOTAL); }
double graph_structural_rank(graph_t *g, u64 off) {
    u64 t = graph_structural_total(g); return t ? (double)rdu64(g->mf, off + E_SVIS) / (double)t : 0.0;
}
double graph_walker_rank(graph_t *g, u64 off) {
    u64 t = graph_walker_total(g); return t ? (double)rdu64(g->mf, off + E_WVIS) / (double)t : 0.0;
}
double graph_get_psi(graph_t *g, u64 off) { return rdf64(g->mf, off + E_PSI); }

/* migration: restore an entity's preserved fields exactly (logical rebuild). */
void graph_set_entity_fields(graph_t *g, u64 off, u64 mtime, u64 obs_mtime,
                             u64 structural_visits, u64 walker_visits, double psi) {
    wru64(g->mf, off + E_MTIME, mtime);
    wru64(g->mf, off + E_OBSM, obs_mtime);
    wru64(g->mf, off + E_SVIS, structural_visits);
    wru64(g->mf, off + E_WVIS, walker_visits);
    wrf64(g->mf, off + E_PSI, psi);
}
/* migration: restore the global visit totals. */
void graph_set_totals(graph_t *g, u64 structural_total, u64 walker_total) {
    wru64(g->mf, g->header_offset + GH_STRUCTURAL_TOTAL, structural_total);
    wru64(g->mf, g->header_offset + GH_WALKER_TOTAL, walker_total);
}

/* one MC complete-path walk (Avrachenkov Alg. 4): forward edges, damping, stop at dangling */
static u32 structural_walk(graph_t *g, u64 start, double damping) {
    u64 cur = start; u32 visits = 0;
    for (;;) {
        graph_inc_structural_visit(g, cur); visits++;
        u32 ec = graph_edge_count(g, cur);
        if (!ec) break;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, cur, es, ec);
        u32 fwd = 0;
        for (u32 k = 0; k < ec; k++) if (es[k].direction == DIR_FORWARD) fwd++;
        if (fwd == 0 || rng_d(&g_rng) >= damping) { free(es); break; }
        u32 pick = (u32)(rng_d(&g_rng) * fwd); if (pick >= fwd) pick = fwd - 1;
        u32 seen = 0; u64 next = cur;
        for (u32 k = 0; k < ec; k++) if (es[k].direction == DIR_FORWARD) { if (seen == pick) { next = es[k].target_offset; break; } seen++; }
        free(es);
        cur = next;
    }
    return visits;
}

u32 graph_structural_sample(graph_t *g, u32 iterations, double damping) {
    u32 n = graph_entity_count(g);
    if (n == 0) return 0;
    u64 *offs = malloc((size_t)n * 8);
    graph_list_entities(g, offs, n);
    u32 total = 0;
    for (u32 it = 0; it < iterations; it++)
        for (u32 i = 0; i < n; i++) total += structural_walk(g, offs[i], damping);
    free(offs);
    return total;
}

u32 graph_compute_merw_psi(graph_t *g, double alpha, u32 max_iter, double tol) {
    u32 n = graph_entity_count(g);
    if (n == 0) return 0;
    u64 *offs = malloc((size_t)n * 8);
    graph_list_entities(g, offs, n);

    omap idx; omap_init(&idx, n * 2 < 256 ? 256 : n * 2);
    for (u32 i = 0; i < n; i++) omap_put(&idx, offs[i], i + 1);   /* index+1; 0 = absent */

    /* CSR forward adjacency */
    u32 *rowoff = malloc((size_t)(n + 1) * 4);
    rowoff[0] = 0;
    for (u32 i = 0; i < n; i++) {
        u32 ec = graph_edge_count(g, offs[i]), d = 0;
        if (ec) {
            adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
            graph_read_edges(g, offs[i], es, ec);
            for (u32 k = 0; k < ec; k++) if (es[k].direction == DIR_FORWARD && omap_get(&idx, es[k].target_offset)) d++;
            free(es);
        }
        rowoff[i + 1] = rowoff[i] + d;
    }
    u32 nnz = rowoff[n];
    u32 *col = malloc((size_t)(nnz ? nnz : 1) * 4);
    for (u32 i = 0; i < n; i++) {
        u32 ec = graph_edge_count(g, offs[i]);
        if (!ec) continue;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, offs[i], es, ec);
        u32 w = rowoff[i];
        for (u32 k = 0; k < ec; k++) if (es[k].direction == DIR_FORWARD) {
            u64 j = omap_get(&idx, es[k].target_offset);
            if (j) col[w++] = (u32)(j - 1);
        }
        free(es);
    }

    double *psi = malloc((size_t)n * 8), *nx = malloc((size_t)n * 8);
    double warm_sum = 0; u32 warm_cnt = 0;
    for (u32 i = 0; i < n; i++) { double v = rdf64(g->mf, offs[i] + E_PSI); psi[i] = v; if (v > 0) { warm_sum += v; warm_cnt++; } }
    if (warm_cnt) { double m = warm_sum / warm_cnt; for (u32 i = 0; i < n; i++) if (psi[i] <= 0) psi[i] = m; }
    else { double u = 1.0 / __builtin_sqrt((double)n); for (u32 i = 0; i < n; i++) psi[i] = u; }
    double nrm = 0; for (u32 i = 0; i < n; i++) nrm += psi[i] * psi[i]; nrm = __builtin_sqrt(nrm);
    if (nrm > 0) for (u32 i = 0; i < n; i++) psi[i] /= nrm;

    double teleport = (1.0 - alpha) / (double)n;
    u32 iter = 0;
    for (iter = 0; iter < max_iter; iter++) {
        for (u32 i = 0; i < n; i++) nx[i] = 0;
        double psi_sum = 0; for (u32 i = 0; i < n; i++) psi_sum += psi[i];
        double tc = teleport * psi_sum;
        for (u32 i = 0; i < n; i++) { double val = alpha * psi[i]; for (u32 p = rowoff[i]; p < rowoff[i + 1]; p++) nx[col[p]] += val; }
        for (u32 i = 0; i < n; i++) nx[i] += tc;
        double norm = 0; for (u32 i = 0; i < n; i++) norm += nx[i] * nx[i]; norm = __builtin_sqrt(norm);
        if (norm > 0) for (u32 i = 0; i < n; i++) nx[i] /= norm;
        double diff = 0; for (u32 i = 0; i < n; i++) { double d = nx[i] - psi[i]; diff += d * d; } diff = __builtin_sqrt(diff);
        double *t = psi; psi = nx; nx = t;
        if (diff < tol) { iter++; break; }
    }
    for (u32 i = 0; i < n; i++) { if (psi[i] < 0) psi[i] = 0; wrf64(g->mf, offs[i] + E_PSI, psi[i]); }

    free(offs); free(rowoff); free(col); free(psi); free(nx); omap_free(&idx);
    return iter;
}

u32 graph_random_walk(graph_t *g, u64 start, u32 depth, u32 direction, int merw_mode,
                      u64 seed, u64 *out_path, u32 max_path) {
    u64 st = seed ? seed : g_rng;
    u32 plen = 0;
    if (max_path >= 1) out_path[plen] = start;
    plen = 1;
    u64 cur = start;
    for (u32 i = 0; i < depth; i++) {
        u32 ec = graph_edge_count(g, cur);
        if (!ec) break;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, cur, es, ec);
        u64 *cand = malloc((size_t)ec * 8); double *cpsi = malloc((size_t)ec * 8); u32 nc = 0;
        for (u32 k = 0; k < ec; k++) {
            if (!dir_match(direction, es[k].direction)) continue;
            u64 t = es[k].target_offset; if (t == cur) continue;
            double p = rdf64(g->mf, t + E_PSI);
            int found = 0;
            for (u32 j = 0; j < nc; j++) if (cand[j] == t) { if (p > cpsi[j]) cpsi[j] = p; found = 1; break; }
            if (!found) { cand[nc] = t; cpsi[nc] = p; nc++; }
        }
        free(es);
        if (nc == 0) { free(cand); free(cpsi); break; }
        double total_psi = 0; for (u32 j = 0; j < nc; j++) total_psi += cpsi[j];
        u64 chosen;
        if (merw_mode && total_psi > 0) {
            double r = rng_d(&st) * total_psi, cum = 0; chosen = cand[nc - 1];
            for (u32 j = 0; j < nc; j++) { cum += cpsi[j]; if (r <= cum) { chosen = cand[j]; break; } }
        } else {
            u32 ix = (u32)(rng_d(&st) * nc); if (ix >= nc) ix = nc - 1; chosen = cand[ix];
        }
        free(cand); free(cpsi);
        cur = chosen;
        if (plen < max_path) out_path[plen] = cur;
        plen++;
    }
    if (!seed) g_rng = st;
    return plen;
}

/* ======================================================================
 * validate_graph: integrity audit (observation limits + dangling edges)
 * ====================================================================== */

u32 graph_validate_obs(graph_t *g, u64 *off, u8 *count, u8 *oversize, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 n = rdu32(mf, log + 0), found = 0;
    for (u32 i = 0; i < n; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        u8 oc = rdu8(mf, e + E_OBSCNT);
        u32 o0 = rdu32(mf, e + E_OBS0), o1 = rdu32(mf, e + E_OBS1);
        u8 ov = 0; u16 l;
        if (o0) { (void)st_get(g->st, o0, &l); if (l > 140) ov |= 1; }
        if (o1) { (void)st_get(g->st, o1, &l); if (l > 140) ov |= 2; }
        if (oc > 2 || ov) {
            if (found < max) { off[found] = e; count[found] = oc; oversize[found] = ov; }
            found++;
        }
    }
    return found;
}

u32 graph_validate_dangling(graph_t *g, u64 *src, u64 *tgt, u32 max) {
    memfile_t *mf = g->mf;
    u64 log = node_log_off(g);
    u32 n = rdu32(mf, log + 0);
    omap live; omap_init(&live, n * 2 < 256 ? 256 : n * 2);
    for (u32 i = 0; i < n; i++)
        omap_put(&live, rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8), 1);
    u32 found = 0;
    for (u32 i = 0; i < n; i++) {
        u64 e = rdu64(mf, log + NODE_LOG_HEADER_SIZE + (u64)i * 8);
        u32 ec = graph_edge_count(g, e);
        if (!ec) continue;
        adj_entry_t *es = malloc((size_t)ec * sizeof(adj_entry_t));
        graph_read_edges(g, e, es, ec);
        for (u32 k = 0; k < ec; k++)
            if (!omap_has(&live, es[k].target_offset)) {
                if (found < max) { src[found] = e; tgt[found] = es[k].target_offset; }
                found++;
            }
        free(es);
    }
    omap_free(&live);
    return found;
}

/* ======================================================================
 * Lifecycle
 * ====================================================================== */

static u64 graph_init(graph_t *g) {
    memfile_t *mf = g->mf;
    u64 hdr = memfile_alloc(mf, GRAPH_HEADER_SIZE);
    u64 log = memfile_alloc(mf, NODE_LOG_HEADER_SIZE + (u64)INITIAL_LOG_CAPACITY * 8);
    u64 ni_size = 8 + (u64)NI_INITIAL_BUCKETS * NI_BUCKET_SIZE;
    u64 ni = memfile_alloc(mf, ni_size);
    if (!hdr || !log || !ni) return 0;

    wru32(mf, log + 0, 0);
    wru32(mf, log + 4, INITIAL_LOG_CAPACITY);

    memset(memfile_ptr(mf, ni), 0, ni_size);
    wru32(mf, ni + 0, NI_INITIAL_BUCKETS);
    wru32(mf, ni + 4, 0);

    memset(memfile_ptr(mf, hdr), 0, GRAPH_HEADER_SIZE);
    wru64(mf, hdr + GH_NODE_LOG_OFF, log);
    wru64(mf, hdr + GH_NAME_INDEX_OFF, ni);
    wru32(mf, hdr + GH_SCHEMA_VERSION, GRAPH_SCHEMA_VERSION);
    return hdr;
}

graph_t *graph_open(const char *graph_path, stringtable_t *st, size_t initial_size) {
    graph_t *g = calloc(1, sizeof(*g));
    if (!g) return NULL;
    g->st = st;
    g->mf = memfile_open(graph_path, initial_size ? initial_size : 65536);
    if (!g->mf) { free(g); return NULL; }

    memfile_lock_exclusive(g->mf);
    memfile_refresh(g->mf);
    if (g->mf->header->allocated <= sizeof(memfile_header_t)) {
        g->header_offset = graph_init(g);
        memfile_sync(g->mf);
    } else {
        g->header_offset = sizeof(memfile_header_t);   /* the file's first allocation */
    }
    memfile_unlock(g->mf);

    if (g->header_offset == 0) { graph_close(g); return NULL; }
    return g;
}

void graph_sync(graph_t *g) { memfile_sync(g->mf); }

void graph_close(graph_t *g) {
    if (!g) return;
    if (g->mf) { memfile_close(g->mf); free(g->mf); }
    free(g);   /* string table is shared; not closed here */
}
