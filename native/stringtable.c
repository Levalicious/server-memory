#include "stringtable.h"

#include <stdlib.h>
#include <string.h>

#define ENT_HEADER       10u   /* u32 refcount + u32 hash + u16 len */
#define OUR_HEADER_SIZE  16u   /* u64 hash_index_offset + u32 entry_count + u32 pad */
#define INITIAL_BUCKETS  4096u

/* ---- aliasing-safe field access via memfile offsets (never cache across alloc) ---- */
static inline u32 rdu32(memfile_t *mf, u64 o) { u32 v; memcpy(&v, memfile_ptr(mf, o), 4); return v; }
static inline u16 rdu16(memfile_t *mf, u64 o) { u16 v; memcpy(&v, memfile_ptr(mf, o), 2); return v; }
static inline u64 rdu64(memfile_t *mf, u64 o) { u64 v; memcpy(&v, memfile_ptr(mf, o), 8); return v; }
static inline void wru32(memfile_t *mf, u64 o, u32 v) { memcpy(memfile_ptr(mf, o), &v, 4); }
static inline void wru16(memfile_t *mf, u64 o, u16 v) { memcpy(memfile_ptr(mf, o), &v, 2); }
static inline void wru64(memfile_t *mf, u64 o, u64 v) { memcpy(memfile_ptr(mf, o), &v, 8); }

static u32 fnv1a(const u8 *d, u16 len) {
    u32 h = 0x811c9dc5u;
    for (u16 i = 0; i < len; i++) { h ^= d[i]; h *= 0x01000193u; }
    return h;
}

/* ---- header / index accessors ---- */
static inline u64 hash_index_off(stringtable_t *st) { return rdu64(st->mf, st->header_offset + 0); }
static inline u32 entry_count(stringtable_t *st)     { return rdu32(st->mf, st->header_offset + 8); }
static inline void set_entry_count(stringtable_t *st, u32 c) { wru32(st->mf, st->header_offset + 8, c); }
static inline u64 bucket_pos(u64 idx, u32 slot)      { return idx + 8 + (u64)slot * 8; }

static void st_rehash(stringtable_t *st, u32 new_bc);

/* ---- init ---- */
static u64 st_init(stringtable_t *st) {
    memfile_t *mf = st->mf;
    u64 hdr = memfile_alloc(mf, OUR_HEADER_SIZE);
    u64 idx_size = 8 + (u64)INITIAL_BUCKETS * 8;
    u64 idx = memfile_alloc(mf, idx_size);
    if (!hdr || !idx) return 0;
    memset(memfile_ptr(mf, idx), 0, idx_size);
    wru32(mf, idx + 0, INITIAL_BUCKETS);   /* bucket_count */
    wru64(mf, hdr + 0, idx);               /* hash_index_offset */
    wru32(mf, hdr + 8, 0);                 /* entry_count */
    return hdr;
}

stringtable_t *st_open(const char *path, size_t initial_size) {
    stringtable_t *st = calloc(1, sizeof(*st));
    if (!st) return NULL;
    st->mf = memfile_open(path, initial_size ? initial_size : 65536);
    if (!st->mf) { free(st); return NULL; }

    st_lock_exclusive(st);
    memfile_refresh(st->mf);
    if (st->mf->header->allocated <= sizeof(memfile_header_t)) {
        st->header_offset = st_init(st);
        memfile_sync(st->mf);
    } else {
        st->header_offset = sizeof(memfile_header_t);  /* the file's first allocation */
    }
    st_unlock(st);

    if (st->header_offset == 0) { st_close(st); return NULL; }
    return st;
}

/* ---- intern / find ---- */
u64 st_intern(stringtable_t *st, const u8 *data, u16 len) {
    memfile_t *mf = st->mf;
    u32 hash = fnv1a(data, len);
    u64 idx = hash_index_off(st);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash % bc;

    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 eoff = rdu64(mf, bucket_pos(idx, slot));

        if (eoff == 0) {                         /* empty -> new entry */
            u64 noff = memfile_alloc(mf, ENT_HEADER + len);
            if (!noff) return 0;
            wru32(mf, noff + 0, 1);              /* refcount */
            wru32(mf, noff + 4, hash);
            wru16(mf, noff + 8, len);
            if (len) memcpy(memfile_ptr(mf, noff + 10), data, len);

            idx = hash_index_off(st);            /* re-fetch (offset stable pre-rehash) */
            wru64(mf, bucket_pos(idx, slot), noff);
            u32 cnt = entry_count(st) + 1;
            set_entry_count(st, cnt);
            if ((u64)cnt * 10 > (u64)bc * 7) st_rehash(st, bc * 2);
            return noff;
        }

        if (rdu32(mf, eoff + 4) == hash) {       /* hash hit -> compare */
            u16 elen = rdu16(mf, eoff + 8);
            if (elen == len && (len == 0 || memcmp(memfile_ptr(mf, eoff + 10), data, len) == 0)) {
                wru32(mf, eoff + 0, rdu32(mf, eoff + 0) + 1);  /* refcount++ */
                return eoff;
            }
        }
    }
    return 0;  /* index full — should not happen with rehashing */
}

u64 st_find(stringtable_t *st, const u8 *data, u16 len) {
    memfile_t *mf = st->mf;
    u32 hash = fnv1a(data, len);
    u64 idx = hash_index_off(st);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash % bc;

    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 eoff = rdu64(mf, bucket_pos(idx, slot));
        if (eoff == 0) return 0;
        if (rdu32(mf, eoff + 4) == hash) {
            u16 elen = rdu16(mf, eoff + 8);
            if (elen == len && (len == 0 || memcmp(memfile_ptr(mf, eoff + 10), data, len) == 0))
                return eoff;
        }
    }
    return 0;
}

/* ---- refcount ---- */
void st_addref(stringtable_t *st, u64 id) {
    if (!id) return;
    wru32(st->mf, id + 0, rdu32(st->mf, id + 0) + 1);
}

/* circular-probe relocation test (Knuth backward-shift deletion) */
static int needs_reloc(u32 natural, u32 empty, u32 current) {
    if (natural <= current) return natural <= empty && empty < current;
    return natural <= empty || empty < current;
}
static void index_fixup(stringtable_t *st, u32 removed, u32 bc) {
    memfile_t *mf = st->mf;
    u64 idx = hash_index_off(st);
    u32 slot = (removed + 1) % bc;
    for (;;) {
        u64 e = rdu64(mf, bucket_pos(idx, slot));
        if (e == 0) break;
        u32 natural = rdu32(mf, e + 4) % bc;
        if (needs_reloc(natural, removed, slot)) {
            wru64(mf, bucket_pos(idx, removed), e);
            wru64(mf, bucket_pos(idx, slot), 0);
            removed = slot;
        }
        slot = (slot + 1) % bc;
    }
}
static void index_remove(stringtable_t *st, u64 off, u32 hash) {
    memfile_t *mf = st->mf;
    u64 idx = hash_index_off(st);
    u32 bc = rdu32(mf, idx + 0);
    u32 bucket = hash % bc;
    for (u32 i = 0; i < bc; i++) {
        u32 slot = (bucket + i) % bc;
        u64 e = rdu64(mf, bucket_pos(idx, slot));
        if (e == 0) return;
        if (e == off) { wru64(mf, bucket_pos(idx, slot), 0); index_fixup(st, slot, bc); return; }
    }
}

void st_release(stringtable_t *st, u64 id) {
    if (!id) return;
    memfile_t *mf = st->mf;
    u32 rc = rdu32(mf, id + 0);
    if (rc <= 1) {
        u32 hash = rdu32(mf, id + 4);
        u16 len = rdu16(mf, id + 8);
        index_remove(st, id, hash);
        memfile_free(mf, id, ENT_HEADER + len);   /* sized free */
        set_entry_count(st, entry_count(st) - 1);
    } else {
        wru32(mf, id + 0, rc - 1);
    }
}

/* ---- rehash ---- */
static void st_rehash(stringtable_t *st, u32 new_bc) {
    memfile_t *mf = st->mf;
    u64 old_idx = hash_index_off(st);
    u32 old_bc = rdu32(mf, old_idx + 0);
    u64 new_size = 8 + (u64)new_bc * 8;
    u64 new_idx = memfile_alloc(mf, new_size);
    if (!new_idx) return;                          /* leave old index in place; still correct */
    memset(memfile_ptr(mf, new_idx), 0, new_size);
    wru32(mf, new_idx + 0, new_bc);

    for (u32 i = 0; i < old_bc; i++) {
        u64 e = rdu64(mf, bucket_pos(old_idx, i));
        if (e == 0) continue;
        u32 b = rdu32(mf, e + 4) % new_bc;
        for (u32 j = 0; j < new_bc; j++) {
            u32 s = (b + j) % new_bc;
            if (rdu64(mf, bucket_pos(new_idx, s)) == 0) { wru64(mf, bucket_pos(new_idx, s), e); break; }
        }
    }
    wru64(mf, st->header_offset + 0, new_idx);     /* repoint header */
    memfile_free(mf, old_idx, 8 + (u64)old_bc * 8);
}

/* ---- read / stats ---- */
const u8 *st_get(stringtable_t *st, u64 id, u16 *len_out) {
    if (len_out) *len_out = rdu16(st->mf, id + 8);
    return (const u8 *)memfile_ptr(st->mf, id + 10);
}
u32 st_refcount(stringtable_t *st, u64 id) { return rdu32(st->mf, id + 0); }
u32 st_count(stringtable_t *st)            { return entry_count(st); }

/* ---- lifecycle / concurrency ---- */
void st_sync(stringtable_t *st)  { memfile_sync(st->mf); }
int  st_lock_shared(stringtable_t *st)    { return memfile_lock_shared(st->mf); }
int  st_lock_exclusive(stringtable_t *st) { return memfile_lock_exclusive(st->mf); }
int  st_unlock(stringtable_t *st)         { return memfile_unlock(st->mf); }

void st_close(stringtable_t *st) {
    if (!st) return;
    if (st->mf) { memfile_close(st->mf); free(st->mf); }
    free(st);
}
