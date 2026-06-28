/*
 * StringTable - interned, refcounted string storage over a v3 MemoryFile.
 *
 * Entry layout (allocated via memfile_alloc): [u32 refcount][u32 hash][u16 len][u8 data[len]]
 *   String ID = the entry offset (v3 has no per-alloc header, so id == alloc offset directly).
 * Hash index: [u32 bucket_count][u32 _pad][u64 buckets[bucket_count]], linear probing.
 * Our header block (first allocation): [u64 hash_index_offset][u32 entry_count][u32 _pad].
 */
#ifndef STRINGTABLE_H
#define STRINGTABLE_H

#include "memoryfile.h"

typedef struct {
    memfile_t *mf;
    u64 header_offset;   /* our header block (the file's first allocation) */
} stringtable_t;

stringtable_t *st_open(const char *path, size_t initial_size);
void st_close(stringtable_t *st);
void st_sync(stringtable_t *st);

/* Intern: dedup + refcount++. Returns id (offset); 0 on failure. */
u64  st_intern(stringtable_t *st, const u8 *data, u16 len);
/* Look up without interning / bumping. Returns id, or 0 if absent. */
u64  st_find(stringtable_t *st, const u8 *data, u16 len);
/* refcount++ on an existing id. */
void st_addref(stringtable_t *st, u64 id);
/* refcount--; frees the entry + removes from index when it hits 0. */
void st_release(stringtable_t *st, u64 id);

/* Zero-copy read: returns a pointer into the mmap + length. Valid until the next
 * allocation/remap on this table. */
const u8 *st_get(stringtable_t *st, u64 id, u16 *len_out);
u32  st_refcount(stringtable_t *st, u64 id);
u32  st_count(stringtable_t *st);

/* Concurrency passthrough (strings file has its own fd/flock). */
int  st_lock_shared(stringtable_t *st);
int  st_lock_exclusive(stringtable_t *st);
int  st_unlock(stringtable_t *st);

#endif /* STRINGTABLE_H */
