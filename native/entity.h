// entity.h - TRUE single source of truth schema definition
#ifndef ENTITY_H
#define ENTITY_H

#include "schema.h"

//=============================================================================
// ENTITY_VERSIONS: The ONE AND ONLY definition of all versions and lenses
//
// BASE(version, struct_body) - first version, no lens
// V(version, prev_version, struct_body, lens_from_prev) - subsequent versions
//=============================================================================

#define ENTITY_BASE \
    BASE(1, { u32 name_id; u32 type_id; u64 adj_offset; u64 mtime; u64 obs_mtime; \
              u8 obs_count; u8 _pad0[3]; u32 obs0_id; u32 obs1_id; u32 _pad1; \
              u64 structural_visits; u64 walker_visits; double psi; })

/* Future schema changes append one line each, e.g.:
 *   V(2, 1, { <v1 fields>; f32 score; },
 *     { LENS_COPY(c->name_id, v->name_id); ...; c->score = 0; })  */
#define ENTITY_UPGRADES

#define ENTITY_CURRENT 1

//=============================================================================
// Expand: struct typedefs
//=============================================================================
#define BASE(ver, body) typedef struct body Entity_v##ver;
#define V(ver, prev, body, lens) typedef struct body Entity_v##ver;
ENTITY_BASE
ENTITY_UPGRADES
#undef BASE
#undef V

typedef Entity_v1 Entity;
#define ENTITY_VERSION ENTITY_CURRENT

//=============================================================================
// Expand: size table
//=============================================================================
#define BASE(ver, body) sizeof(Entity_v##ver),
#define V(ver, prev, body, lens) sizeof(Entity_v##ver),
static const size_t entity_sizes[] = { 0, ENTITY_BASE ENTITY_UPGRADES };
#undef BASE
#undef V

//=============================================================================
// Expand: lens functions - only from ENTITY_UPGRADES (base has no lens)
//=============================================================================
#define V(ver, prev, body, lens) \
    static inline void Entity_lens_##prev##_to_##ver(Entity_v##ver* c, const Entity_v##prev* v) lens
ENTITY_UPGRADES
#undef V

//=============================================================================
// Expand: upgrade dispatch
//=============================================================================
static inline void entity_upgrade(int from_ver, void* out, const void* in) {
    switch (from_ver) {
#define V(ver, prev, body, lens) \
    case prev: Entity_lens_##prev##_to_##ver((Entity_v##ver*)out, (const Entity_v##prev*)in); break;
ENTITY_UPGRADES
#undef V
    }
}

//=============================================================================
// Generic read: chains through lenses automatically
//=============================================================================
static inline int entity_read(const void* buf, Entity* out) {
    u32 ver = *(const u32*)buf;
    const void* data = (const char*)buf + sizeof(u32);
    
    if (ver == 0 || ver > ENTITY_VERSION) return -1;
    
    // Ping-pong buffers for chaining
    u8 tmp[2][sizeof(Entity)];
    int cur = 0;
    
    memcpy(tmp[cur], data, entity_sizes[ver]);
    
    while (ver < ENTITY_VERSION) {
        entity_upgrade(ver, tmp[1-cur], tmp[cur]);
        cur = 1 - cur;
        ver++;
    }
    
    memcpy(out, tmp[cur], sizeof(Entity));
    return 0;
}

static inline void entity_write(const Entity* c, void* buf) {
    *(u32*)buf = ENTITY_VERSION;
    memcpy((char*)buf + sizeof(u32), c, sizeof(Entity));
}

static inline size_t entity_bufsize(u32 version) {
    if (version == 0 || version > ENTITY_VERSION) return 0;
    return sizeof(u32) + entity_sizes[version];
}

#endif // ENTITY_H
