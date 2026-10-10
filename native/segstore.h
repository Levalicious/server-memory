/*
 * libsegstore — paged COW segment store (graphstore v4, spec r2 §2).
 *
 * One segment file = a libmdbx-imitation paged store:
 *   - page-granularity COW: a write txn never modifies a live page; it builds
 *     new page images in heap buffers and publishes them with pwritev.
 *   - dual meta pages at pgno 0 and 1, alternating; commit toggles to the
 *     other slot; recovery picks the valid meta with the highest txid.
 *   - freelist as first-class COW data (retired pages keyed by retiring txid);
 *     reuse gated on snapshot pins.
 *   - NO writable mmap anywhere (Insight_ROMmapPwriteIsLibmdbxDefault):
 *     reads go through a PROT_READ MAP_SHARED mapping; writes through
 *     pwritev + fdatasync. msync does not exist in this codebase.
 *
 * Page format is GENERAL SLOTTED (Decision_SlottedFormatTypeAffinity):
 *   one page structure for every record kind; type separation is a placement
 *   *policy* (same-kind affinity), never a format constraint. COW touch always
 *   rewrites a page compacted, so intra-page fragmentation is txn-local.
 *
 * Scale split (Insight_PageVsExtentScale): page = COW/checksum unit (4K,
 *   matches OS page); extent = allocation/locality unit (contiguous page runs).
 *
 * Byte order: little-endian on disk (x86-only fleet; matches memfile v3
 * precedent of native packed structs).
 *
 * This header freezes candidates for the format-freeze review; nothing is
 * FROZEN until the spec says so.
 */
#ifndef SEGSTORE_H
#define SEGSTORE_H

#include <stdint.h>
#include <stddef.h>

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

/* ------------------------------------------------------------------ *
 * Constants (format-freeze candidates)
 * ------------------------------------------------------------------ */

#define SEG_MAGIC        0x53454731u   /* "SEG1" */
#define SEG_FORMAT_VER   1u
#ifndef SEG_PAGE_SIZE                  /* overridable (-DSEG_PAGE_SIZE=16384u) for
                                          the Q1 bench; default = the 4K prior */
#define SEG_PAGE_SIZE    4096u         /* COW unit; recorded in meta. KEPT at 4K:
                                          8K re-measured 2026-10-08 (~1.6x
                                          bytes/commit & amp @500K) and rejected.
                                          Names beyond SEG_PAGE_MAX_REC-4 stay
                                          unsupported (multi-page records = the
                                          future option; E2E case expected-fail). */
#endif
#define SEG_META_PAGES   2u            /* pgno 0 and 1 */

/* ------------------------------------------------------------------ *
 * Packed refs: (u16 seg | u32 pgno | u16 slot)
 *
 * 4K * 2^32 = 16 TB max per segment; 2^16 slots/page (a 4K page holds far
 * fewer; headroom is free). Field boundaries byte-clean on purpose.
 * pgno addresses a page within a segment; slot indexes its slot array.
 * ------------------------------------------------------------------ */

typedef u64 seg_ref_t;

#define SEG_REF_NULL ((seg_ref_t)0)   /* seg 0 pgno 0 is a meta page: never a record */

static inline seg_ref_t seg_ref_make(u16 seg, u32 pgno, u16 slot) {
    return ((u64)seg << 48) | ((u64)pgno << 16) | (u64)slot;
}
static inline u16 seg_ref_seg(seg_ref_t r)  { return (u16)(r >> 48); }
static inline u32 seg_ref_pgno(seg_ref_t r) { return (u32)(r >> 16); }
static inline u16 seg_ref_slot(seg_ref_t r) { return (u16)r; }

/* ------------------------------------------------------------------ *
 * Meta page (dual, at pgno 0 and 1)
 *
 * The ONLY checksummed structure: a torn meta write must be detectable so
 * recovery can fall back to the other slot. Data pages are not checksummed
 * (libmdbx/LMDB discipline): commit ordering (data fsync BEFORE meta write)
 * guarantees no committed-but-torn data page exists.
 *
 * checksum = crc32c over the struct with the checksum field itself as 0.
 * ------------------------------------------------------------------ */

typedef struct __attribute__((packed)) {
    u32 magic;                /* SEG_MAGIC */
    u32 format_version;       /* SEG_FORMAT_VER */
    u32 page_size;            /* SEG_PAGE_SIZE at creation; must match to open */
    u16 seg_id;               /* this segment's id in the store */
    u16 extent_pages_log2;    /* allocation cluster size = 2^n pages */
    u64 txid;                 /* transaction that wrote this meta */
    u64 watermark;            /* PHYSICAL page count: file pages [0, watermark) */
    u32 nameindex_root_pgno;  /* graph-layer roots (LOGICAL pgnos); 0 = none */
    u32 indirect_root_pgno;   /* node-id indirection table root (LOGICAL) */
    u32 freelist_root_pgno;   /* freelist snapshot page (PHYSICAL pgno) */
    u32 ptable_root_pgno;     /* shadow page-table root (PHYSICAL pgno)
                                 Decision_LogicalPgnoShadowTable: refs use
                                 stable LOGICAL pgnos; this maps them */
    u64 logical_pages;        /* logical pgno space: [0, logical_pages) */
    u64 _reserved1[3];
    u32 _reserved2;
    u32 checksum;             /* crc32c, LAST field (offsetof used in tests) */
} seg_meta_t;

/* One meta occupies the head of its 4K page; the rest of the page is zero. */

/* ------------------------------------------------------------------ *
 * Slotted page
 *
 * [seg_page_hdr_t][slot array ->...        ...<- record space]
 *
 * Slots grow down-from-header (ascending addresses), records grow up-from-end
 * (descending addresses). free space = [hdr+nslots*4, rec_floor).
 * A slot is (offset, size); offset is from page start; size in bytes.
 * offset == 0 marks a dead slot (page offset 0 is inside the header, so 0 is
 * never a valid record offset — same sentinel discipline as memfile).
 *
 * COW touch rewrites the page compacted: live records packed tight against
 * the end, dead slots dropped if nothing refs them (slot ids are stable only
 * within a page generation; cross-page refs use (pgno,slot), and compaction
 * preserves slot ids — only record OFFSETS move).
 * ------------------------------------------------------------------ */

typedef struct __attribute__((packed)) {
    u16 nslots;       /* slot array length (includes dead slots) */
    u16 rec_floor;    /* lowest byte offset used by record space */
    u16 kind_hint;    /* placement affinity tag (POLICY ONLY; not semantics) */
    u16 flags;
    u64 _reserved;
} seg_page_hdr_t;

typedef struct __attribute__((packed)) {
    u16 offset;       /* from page start; 0 = dead slot */
    u16 size;         /* record bytes */
} seg_slot_t;

#define SEG_PAGE_HDR_SIZE  ((u32)sizeof(seg_page_hdr_t))   /* 16 */
#define SEG_SLOT_SIZE      ((u32)sizeof(seg_slot_t))       /*  4 */
/* Max record bytes a single page can hold (one slot, one record). */
#define SEG_PAGE_MAX_REC   (SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE - SEG_SLOT_SIZE)

/* kind_hint values (placement affinity; graph layer owns the vocabulary) */
#define SEG_KIND_FREE      0u
#define SEG_KIND_ENTITY    1u
#define SEG_KIND_ADJ       2u
#define SEG_KIND_NAMEIDX   3u
#define SEG_KIND_INDIRECT  4u
#define SEG_KIND_STRING    5u
#define SEG_KIND_FREELIST  6u
#define SEG_KIND_EXTENT    7u   /* pages owned by an extent record (multi-page runs) */
/* seg_tree (docs/v4-index-design-note.md): node type lives in KIND so it
 * survives compact (kind and flags are both preserved). TREE_LEAF reuses the
 * numeric value of the dead NAMEIDX kind (pre-release; stores are fresh-only). */
#define SEG_KIND_TREE_LEAF   3u
#define SEG_KIND_TREE_BRANCH 8u

/* ------------------------------------------------------------------ *
 * Slotted page module (seg_page.c) — proof target #2.
 *
 * All functions operate on a SEG_PAGE_SIZE byte buffer (the owner's heap
 * working copy under COW, or a const view into the RO mapping). None do I/O.
 *
 * Free-space model: insert bumps rec_floor down; delete/shrink leave garbage
 * in record space (rec_floor is a low-watermark, not a live-bytes tracker).
 * seg_page_compact — used on every COW touch — rewrites tight, so garbage is
 * transaction-local by construction (Decision_SlottedFormatTypeAffinity).
 *
 * Slot ids are stable across compaction (only record OFFSETS move); interior
 * dead slots persist until they can be reused; trailing dead slots are
 * trimmed by compact.
 * ------------------------------------------------------------------ */

void seg_page_init(u8 *pg, u16 kind_hint);
/* header read accessors (node bookkeeping lives in the header; both survive
 * compact/touch — seg_page_compact re-emits kind AND flags). */
u16  seg_page_nslots(const u8 *pg);
u16  seg_page_kind(const u8 *pg);

/* Structural well-formedness audit: header bounds, every live slot inside
 * [rec_floor, SEG_PAGE_SIZE), no live slot overlaps the slot array. 1 = ok. */
int seg_page_validate(const u8 *pg);

/* Contiguous free bytes available to insert (accounts for the slot entry a
 * fresh insert may need). */
u32 seg_page_free_space(const u8 *pg);

/* Record pointer + size for a live slot; NULL if slot dead or out of range. */
const u8 *seg_page_read(const u8 *pg, u16 slot, u16 *size_out);

/* Insert a record (1 <= size <= SEG_PAGE_MAX_REC); reuses the lowest dead
 * slot else appends one. 1 = ok (*slot_out set), 0 = page full. */
int seg_page_insert(u8 *pg, const u8 *rec, u16 size, u16 *slot_out);

/* Bulk-refill a page with n records in order (the COW node-refresh path):
 * init + n ordered appends in one pass. 1 = ok, 0 = bad args / overflow. */
int seg_page_fill(u8 *pg, u16 kind_hint, const u8 *const *recs, const u16 *sizes, u32 n);

/* Replace a live slot's record. Same-size: in place. Shrink: in place, slot
 * size updated (tail bytes become garbage). Grow: needs contiguous free
 * space for the new copy. 1 = ok, 0 = no space / dead slot / bad size. */
int seg_page_update(u8 *pg, u16 slot, const u8 *rec, u16 size);

/* Mark a live slot dead. 1 = ok, 0 = already dead / out of range. */
int seg_page_delete(u8 *pg, u16 slot);

/* Copy-compact src into dst (both SEG_PAGE_SIZE buffers, non-overlapping):
 * live records packed tight against page end, slot ids preserved, trailing
 * dead slots trimmed, garbage dropped, free gap zeroed. THE COW-touch
 * primitive. 1 = ok; 0 = src fails validation or live records alias
 * (corruption — dst contents undefined, caller must not publish). */
int seg_page_compact(u8 *dst, const u8 *src);

/* ------------------------------------------------------------------ *
 * crc32c (Castagnoli, software table; dep-free per D_NoExternalDependence)
 * ------------------------------------------------------------------ */

u32 seg_crc32c(const u8 *data, size_t len);

/* ------------------------------------------------------------------ *
 * Meta module (seg_meta.c) — the recovery pivot, proof target #1.
 * Pure functions over structs; no I/O here.
 * ------------------------------------------------------------------ */

/* Fill *m and stamp its checksum. Roots/watermark set by caller afterwards
 * via seg_meta_seal (init gives a fresh-store meta: txid 0, empty roots,
 * watermark = SEG_META_PAGES). */
void seg_meta_init(seg_meta_t *m, u16 seg_id, u16 extent_pages_log2);

/* Recompute and store the checksum (call after any field edit). */
void seg_meta_seal(seg_meta_t *m);

/* 1 iff magic, version, page_size and checksum all check out. */
int seg_meta_valid(const seg_meta_t *m);

/* Recovery pivot: given the two meta slots (as read from disk, torn or not),
 * return the index (0 or 1) of the valid meta with the highest txid, or -1
 * if neither is valid. Tie (equal txids, both valid) returns 0: two valid
 * metas with equal txid are only possible on a freshly created store where
 * both slots hold the txid-0 meta, and the slots are then identical. */
int seg_meta_pick(const seg_meta_t *m0, const seg_meta_t *m1);

/* ------------------------------------------------------------------ *
 * seg_io — I/O vtable (seg_io.c).
 *
 * ALL bytes reach a segment file through this interface; there is no other
 * write path (and no writable mmap anywhere — Insight_ROMmapPwrite...).
 * Two implementations:
 *   - posix: fd + PROT_READ MAP_SHARED mapping; pwrite loop; fdatasync;
 *     ftruncate + remap on extend.
 *   - sim:   in-memory disk image + APPEND-ONLY EVENT LOG (every write /
 *     sync / extend). The crash harness replays event-log prefixes to
 *     materialize every legal crash state (Design_CrashHarness_PrefixReplay).
 *
 * read_base() returns a pointer to the CURRENT readable image (RO mapping /
 * sim buffer). It is invalidated by extend(); callers re-fetch after any
 * commit that grew the file.
 * ------------------------------------------------------------------ */

typedef struct seg_io seg_io_t;
struct seg_io {
    /* write len bytes at absolute offset; 1 = ok, 0 = I/O error */
    int (*write)(seg_io_t *io, const void *buf, u64 len, u64 off);
    /* durability barrier (fdatasync); 1 = ok */
    int (*sync)(seg_io_t *io);
    /* grow the file to new_size bytes (zero-filled); 1 = ok */
    int (*extend)(seg_io_t *io, u64 new_size);
    /* current readable image; *size_out = current file size */
    const u8 *(*read_base)(seg_io_t *io, u64 *size_out);
    void (*close)(seg_io_t *io);   /* frees io */
};

seg_io_t *seg_io_posix_open(const char *path, int create);

/* sim: fresh empty "file" with event recording */
seg_io_t *seg_io_sim_open(void);
/* number of recorded events so far */
u32 seg_io_sim_event_count(const seg_io_t *io);
/* materialize the disk image as of event prefix [0, k): a legal crash state
 * under an in-order device. Caller frees via free(). *size_out = file size
 * at that point. */
u8 *seg_io_sim_replay_prefix(const seg_io_t *io, u32 k, u64 *size_out);
/* index of the last SYNC event < k, or event count if k covers all events;
 * helper for harness bookkeeping */
u32 seg_io_sim_last_sync_before(const seg_io_t *io, u32 k);
/* global event clock across ALL sim ios in the process — lets a crash
 * harness cut one consistent point through several files' event logs */
u64 seg_io_sim_gseq_now(void);
u8 *seg_io_sim_replay_gseq(const seg_io_t *io, u64 gseq, u64 *size_out);
/* bench accounting: total bytes written / sync count so far */
u64 seg_io_sim_bytes_written(const seg_io_t *io);
u32 seg_io_sim_sync_count(const seg_io_t *io);

/* ------------------------------------------------------------------ *
 * segfile — one segment's lifecycle over a seg_io (seg_file.c).
 *
 * Commit protocol (spec r2 §3):
 *   1. extend file if watermark grew (extent-cluster-rounded)
 *   2. write every dirty page at pgno * SEG_PAGE_SIZE
 *   3. sync                                  <- data barrier
 *   4. seal + write new meta to the INACTIVE slot
 *   5. sync                                  <- commit point
 *   6. flip active slot in memory
 * Crash before (5) completes => the old meta wins recovery. Crash after all
 * of (4)'s bytes landed may legally recover the NEW txid (see harness).
 * ------------------------------------------------------------------ */

typedef struct {
    seg_io_t  *io;         /* owned: segfile_close closes it */
    seg_meta_t meta;       /* active (last committed) meta */
    int        active_slot;
} segfile_t;

/* Create a fresh segment: both meta slots hold the sealed txid-0 meta,
 * synced. Returns opened segfile (caller segfile_close's it). */
segfile_t *segfile_create(seg_io_t *io, u16 seg_id, u16 extent_pages_log2);

/* Open an existing segment: pick the valid max-txid meta whose watermark
 * fits the file; NULL if neither slot is usable. */
segfile_t *segfile_open(seg_io_t *io);
/* manifest-directed open: select the meta slot with EXACTLY txid (rolls a
 * ran-ahead segment back to the store pivot); NULL if no slot matches. */
segfile_t *segfile_open_at(seg_io_t *io, u64 txid);

/* RO pointer to page pgno of the committed image, NULL if pgno dead-zone
 * (meta pages) is requested via this path or out of watermark. */
const u8 *segfile_page(segfile_t *sf, u32 pgno);

/* Commit: dirty pages (pgno[i] -> page image bufs[i], each SEG_PAGE_SIZE
 * bytes), plus the successor meta (txid/watermark/roots set by caller;
 * seal is done here). Meta pgnos (0,1) are refused as dirty pages.
 * 1 = committed; 0 = I/O error or invalid args (state unchanged: the
 * in-memory meta only advances after the commit-point sync succeeds).
 *
 * COW DISCIPLINE (caller obligation, enforced by the txn layer above):
 * a dirty pgno must NOT be reachable from the last committed meta — a crash
 * between the data write and the commit point would otherwise tear the OLD
 * state. Fresh pages and freelist-reclaimed pages (retired, no live pin)
 * are the only legal targets. segfile itself cannot check reachability. */
int segfile_commit(segfile_t *sf, const u32 *pgnos, const u8 *const *bufs,
                   u32 ndirty, const seg_meta_t *next_meta);

void segfile_close(segfile_t *sf);

/* ------------------------------------------------------------------ *
 * segstore txn layer (seg_txn.c) — COW transactions over segfile.
 *
 * Identity model (Decision_LogicalPgnoShadowTable): callers address LOGICAL
 * pgnos, stable forever. A shadow page table (COW'd fixed-format pages:
 * root -> table pages -> u32 physical pgno entries, PT_NONE = unmapped)
 * maps them to physical file pages. Touch never relocates a logical page;
 * it re-points its table entry at a fresh physical page at commit.
 *
 * COW discipline is ENFORCED here: physical targets come only from the
 * freelist (retired, no blocking pin) or fresh watermark growth — never a
 * physical page reachable from the last committed meta.
 *
 * Freelist persistence: one snapshot (chain of fixed-format pages) per
 * commit holding the full {free[], pending[](txid,pgnos)} state; the meta
 * points at it; reopening rebuilds allocator state exactly. Restart clears
 * pins, so recovery promotes all pending groups with txid <= committed.
 *
 * Pins: seg_pin captures {txid, ptable copy}; physical pages retired at
 * txid T are reusable only when every active pin has pin->txid >= T.
 * Pinned reads therefore stay byte-stable regardless of later commits.
 * Single-writer: at most one open txn; no locks anywhere.
 * ------------------------------------------------------------------ */

#define SEG_PT_NONE 0xFFFFFFFFu   /* unmapped logical page */

typedef struct segstore segstore_t;
typedef struct segpin { u64 txid; u32 *ptable; u64 logical_pages;
                        struct segpin *next; } segpin_t;

segstore_t *segstore_create(seg_io_t *io, u16 seg_id, u16 extent_pages_log2);
segstore_t *segstore_open(seg_io_t *io);
segstore_t *segstore_open_at(seg_io_t *io, u64 txid);
void        segstore_close(segstore_t *st);
u64         segstore_txid(const segstore_t *st);
u64         segstore_logical_pages(const segstore_t *st);
u32         segstore_nameindex_root(const segstore_t *st);  /* staged if txn open */

/* committed read: RO pointer to logical page lpg, NULL if unmapped */
const u8 *segstore_read(segstore_t *st, u32 lpg);
/* read-through-txn: the open txn's working copy if lpg is dirty, else the
 * committed page. What a layer building ON txns (graph, strings) reads. */
const u8 *seg_txn_view(segstore_t *st, u32 lpg);

/* snapshot pins */
segpin_t *seg_pin(segstore_t *st);
void      seg_unpin(segstore_t *st, segpin_t *pin);
const u8 *seg_pin_read(segstore_t *st, const segpin_t *pin, u32 lpg);

/* single open txn; all bufs are SEG_PAGE_SIZE heap pages owned by the txn */
int  seg_txn_begin(segstore_t *st);
/* COW touch: compacted working copy of lpg (stable across calls in-txn) */
u8  *seg_txn_touch(segstore_t *st, u32 lpg);
/* fresh logical page (seg_page_init'd, kind_hint applied); *lpg_out set */
u8  *seg_txn_alloc(segstore_t *st, u16 kind_hint, u32 *lpg_out);
/* unmap a logical page; its physical page retires at commit. 1 = ok */
int  seg_txn_free(segstore_t *st, u32 lpg);
/* set graph-layer roots recorded in the next meta (logical pgnos) */
void seg_txn_set_roots(segstore_t *st, u32 nameindex_root, u32 indirect_root);

/* ------------------------------------------------------------------ *
 * seg_tree — the generic B+tree over slotted pages (docs/v4-index-design
 * -note.md; Design_IndexRedesign_2026_10_09). One implementation,
 * parameterized by codec; instantiated as catalog / name index / posting
 * trees. Entries are always stored in key order (bytewise compare, shorter
 * -prefix-first on ties); mutation rebuilds the touched node's image in
 * order; growth is incremental splits only; deletion is rewrite + empty
 * -unlink + single-pass sibling merge when the pair fits.
 * Node refs are lpg+1 (0 = none), matching the segment root slots.
 * ------------------------------------------------------------------ */

/* key: bytewise, either fixed-size or [u16 klen][bytes].
 * value: 0 = none (posting-style), 1 = u32, 2 = [u16 vlen][bytes]. */
typedef struct { u8 key_fix; u8 val_mode; } st_codec_t;

typedef struct {
    segstore_t *gs;
    st_codec_t  cd;
    u32 root;             /* root node lpg+1; 0 = empty tree */
    u64 count;            /* entries, maintained by this module */
} seg_tree_t;

void seg_tree_open(seg_tree_t *t, segstore_t *gs, st_codec_t cd, u32 root);
/* 1 = inserted new; 0 = key existed (replaced for valued trees / no-op for
 * postings); -1 = error. */
int  seg_tree_insert(seg_tree_t *t, const u8 *k, u16 klen, const u8 *v, u16 vlen);
/* 1 = deleted; 0 = absent; -1 = error. */
int  seg_tree_delete(seg_tree_t *t, const u8 *k, u16 klen);
/* 1 = found (value copied if the codec has one); 0 = absent; -1 = error. */
int  seg_tree_lookup(seg_tree_t *t, const u8 *k, u16 klen, u8 *v, u16 *vlen);
/* Ordered scan within [lo, hi]; NULL bound = open end. cb returns 0 to stop.
 * Returns 0 on completion, 1 if stopped by cb, -1 on error. */
int  seg_tree_scan(seg_tree_t *t,
                   const u8 *lo, u16 lol, int lo_incl,
                   const u8 *hi, u16 hil, int hi_incl,
                   int (*cb)(void *ctx, const u8 *k, u16 klen, const u8 *v, u16 vlen),
                   void *ctx);
u32  seg_tree_root(const seg_tree_t *t);
u64  seg_tree_count(const seg_tree_t *t);
/* publish: allocates physical pages (freelist/growth), writes data + ptable
 * + freelist snapshot + meta via segfile_commit. 1 = committed. */
int  seg_txn_commit(segstore_t *st);
/* commit with an explicit (store-assigned) txid; must be > current */
int  seg_txn_commit_as(segstore_t *st, u64 commit_txid);
void seg_txn_abort(segstore_t *st);

/* ------------------------------------------------------------------ *
 * st4 — refcounted interned strings over a segstore (seg_str4.c).
 *
 * Design_St4Strings_2026_08_24:
 *   sid (u32) = ((lpg << 12) | slot) + 1; 0 = NULL. Stable forever because
 *   slot ids survive compaction (FROZEN property of the slotted page).
 *   Caps the string segment at 2^20 logical pages (4 GB of strings).
 *   Record = [u32 refcount][bytes]; length = slot size - 4.
 *   Intern map (bytes-hash -> sid) is owner-private memory, rebuilt by a
 *   page scan at open; only records persist. decref to 0 deletes the
 *   record (slot reused by the page's lowest-dead-slot policy).
 *
 * All mutating calls require an open txn on the underlying segstore; reads
 * go through seg_txn_view so a txn sees its own interns. The graph and
 * string segments commit in ONE mstore txn = cross-segment atomicity.
 * ------------------------------------------------------------------ */

typedef struct st4 st4_t;

st4_t *st4_open(segstore_t *seg);              /* scans pages, builds map */
void   st4_close(st4_t *st);
u32    st4_count(const st4_t *st);             /* live string count */

/* intern: existing -> refcount+1; new -> record with refcount 1. 0 = fail.
 * len in [1, SEG_PAGE_MAX_REC-4]. */
u32 st4_intern(st4_t *st, const u8 *bytes, u16 len);
/* bytes of a live sid (txn view); NULL if dead/invalid. *len_out set. */
const u8 *st4_get(st4_t *st, u32 sid, u16 *len_out);
int st4_incref(st4_t *st, u32 sid);            /* 1 = ok */
int st4_decref(st4_t *st, u32 sid);            /* 1 = ok; 0 refs deletes */
u32 st4_refcount(st4_t *st, u32 sid);          /* 0 if dead */
/* lookup without interning: sid or 0 */
u32 st4_find(st4_t *st, const u8 *bytes, u16 len);

/* ------------------------------------------------------------------ *
 * graph4 — entities + persisted name index over segstores (seg_graph4.c).
 *
 * Design_Graph4Entities_2026_08_24. Two segstores: graph (entities, adj,
 * name index) + strings (st4). Both commit in ONE mstore txn.
 *
 * Entity id (eid, u32) = ((lpg << 12) | slot) + 1, 0 = NULL — same packing
 * discipline as sids, stable via frozen slot-id compaction stability.
 *
 * Entity record (68B, LE, in SEG_KIND_ENTITY slotted pages):
 *   [u32 version][u32 name_sid][u32 type_sid][u32 adj_ref]
 *   [u64 mtime][u64 obs_mtime][u32 obs0_sid][u32 obs1_sid]
 *   [u8 obs_count + 3 pad][u64 structural_visits][u64 walker_visits][f64 psi]
 *
 * Name index (PERSISTED, Decision_PersistNameIndex): open-addressing over
 *   name_sid. A NAMEIDX page is a slotted page whose slot 0 is one 4072B
 *   record = 509 buckets {u32 name_sid, u32 eid} (0,0 = empty) — slotted so
 *   COW-touch compaction remains universal; updates are same-size in-place.
 *   A directory page (slot 0 record: [u32 npages][u32 lpg…]) lists index
 *   pages; meta.nameindex_root_pgno = directory lpg + 1 (0 = none).
 *   Probe: h(name_sid) % (npages*509), linear, backward-shift delete.
 *   Rehash at load > 0.7 (fresh pages, old ones freed).
 *
 * String refcounts: entity owns one ref each on name_sid, type_sid and its
 * observation sids (v3 discipline, unchanged).
 * ------------------------------------------------------------------ */

typedef struct graph4 graph4_t;
typedef struct mstore mstore_t;   /* fwd (full decl below); C11 permits */

#define G4_SEG_GRAPH   0u
#define G4_SEG_STRINGS 1u

/* open over an mstore created with >= 2 segments (graph, strings).
 * mstore stays caller-owned; graph4_close does not close it. */
graph4_t *graph4_open(mstore_t *ms);
void      graph4_close(graph4_t *g);

/* all mutating ops require an open mstore txn */
u32  g4_create_entity(graph4_t *g, const u8 *name, u16 nlen,
                      const u8 *type, u16 tlen, u64 mtime);  /* existing -> its eid */
int  g4_delete_entity(graph4_t *g, u32 eid);                 /* 1 = deleted */
u32  g4_lookup(graph4_t *g, const u8 *name, u16 nlen);       /* eid or 0 */
/* node-id or 0; *gen_out = the name binding's generation (directory, step 3) */
u32  g4_lookup_ex(graph4_t *g, const u8 *name, u16 nlen, u32 *gen_out);

/* ---- anti-entropy symbol extraction (shard-seam note §8) ----
 * One G4_SYM_LEN-byte symbol per row, emitted in a consistent txn view —
 * call inside the caller's txn. Symbols are the RIBLT reconciliation unit;
 * the emitted layout:
 *   adjacency:    [peer u32][dir u8][rel_sid u32][mtime u64][pad]
 *   vertex-state: [node u32][binding gen u32][content-hash u64][pad]
 * (content hash covers type_sid, obs sids/count, mtime, obs_mtime, psi —
 * visits reconcile separately, the relaxed-counter class). Returns count. */
#define G4_SYM_LEN 32u
u32  g4_adj_symbols(graph4_t *g, void (*cb)(void *ctx, const u8 *sym), void *ctx);
u32  g4_vstate_symbols(graph4_t *g, void (*cb)(void *ctx, const u8 *sym), void *ctx);

typedef struct {
    u32 eid, name_sid, type_sid, adj_ref;
    u64 mtime, obs_mtime;
    u32 obs0_sid, obs1_sid;
    u8  obs_count;
    u64 structural_visits, walker_visits;
    double psi;
} g4_entity_t;

int  g4_read_entity(graph4_t *g, u32 eid, g4_entity_t *out); /* 1 = live */
u32  g4_entity_count(graph4_t *g);
/* enumerate live eids (bucket order); returns count written (<= max) */
u32  g4_list_entities(graph4_t *g, u32 *out, u32 max);

/* observations (KB constraint: max 2, each <= 140 bytes enforced above) */
int  g4_add_observation(graph4_t *g, u32 eid, const u8 *obs, u16 len, u64 mtime);
int  g4_remove_observation(graph4_t *g, u32 eid, const u8 *obs, u16 len, u64 mtime);

/* string accessor passthrough (for callers resolving sids) */
const u8 *g4_str(graph4_t *g, u32 sid, u16 *len_out);

/* ---- adjacency (Design_Graph4Adjacency_2026_08_24) ----
 * Bidirectional storage (BGS_BidirStorage): an edge A-[rt]->B stores a
 * FORWARD entry on A and a BACKWARD mirror on B; each entry owns one ref
 * on rel_sid. Adjacency lives in chained ADJ records ([u32 count]
 * [u32 next_ref][20B entries]); a record relocates freely (single
 * referrer: its entity or the previous chain link). */

#define G4_DIR_FORWARD  0u
#define G4_DIR_BACKWARD 1u
#define G4_DIR_ANY      255u   /* traversal filter: follow any direction */

typedef struct {
    u32 target_eid;
    u32 rel_sid;
    u64 mtime;
    u32 direction;         /* G4_DIR_* */
} g4_edge_t;

/* 1 = created; 0 = exists already / dead endpoint / error */
int g4_create_relation(graph4_t *g, u32 from, u32 to,
                       const u8 *rt, u16 rtlen, u64 mtime);
/* 1 = deleted (both mirrors); 0 = not found */
int g4_delete_relation(graph4_t *g, u32 from, u32 to, const u8 *rt, u16 rtlen);
/* read up to max edges of eid into out[]; returns TRUE total edge count */
u32 g4_edges(graph4_t *g, u32 eid, g4_edge_t *out, u32 max);
u32 g4_edge_count(graph4_t *g, u32 eid);

/* ---- traversal (C hop-count internal: depth 1 = immediate; the public
 * 0-indexed numbering translates at the boundary — Fix_DepthDefault) ----
 * g4_neighbors: eids within <= depth hops of start (start excluded),
 * direction-filtered (dir_match: ANY or exact). depth==1 fast path.
 * Returns TRUE reachable count (out gets min(count, max)).
 * g4_find_path: bidirectional level-sync BFS; reverse frontier follows the
 * INVERTED filter (mirror storage makes both sides local). Returns node
 * count including endpoints written to out_path (0 = no path). */
u32 g4_neighbors(graph4_t *g, u32 start, u32 depth, u32 direction,
                 u32 *out, u32 max);
u32 g4_find_path(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                 u32 *out_path, u32 max_path);
/* g4_find_path_ex: unidirectional BFS with the β-contract — best-effort path
 * to the last discovered node when `to` isn't reached, byte-budget tracking
 * (budget = (u64)-1 disables), and per-call flags. See seg_graph4.c. */
u32 g4_find_path_ex(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                    u64 budget_bytes, u32 *out_path, u32 max_path,
                    int *target_reached, int *budget_exhausted, u32 *farthest);
/* g4_find_path_ex2: the same engine plus continuation support —
 * replay_until = cumulative bytes where a prior run stopped (0 = fresh);
 * cut_out = the trip value (next replay_until). Exact replay holds under an
 * unchanged store txid (frozen discovery order). See seg_graph4.c. */
u32 g4_find_path_ex2(graph4_t *g, u32 from, u32 to, u32 max_depth, u32 direction,
                     u64 budget_bytes, u64 replay_until,
                     u32 *out_path, u32 max_path,
                     int *target_reached, int *budget_exhausted, u32 *farthest,
                     u64 *cut_out);

/* ---- indexes + search (minimal-core port: trigram prefilter decoupled
 * from writes via dirty-set, type index O(1)-maintained, both owner-private
 * in-memory, rebuilt lazily — sound under the single-writer owner) ----
 * g4_search: own regex engine over name|type|obs0|obs1 (v3 semantics),
 * trigram-prefiltered, DFA-verified when the candidate set is large.
 * Returns TRUE match count; out gets min(count, max) eids. */
u32 g4_search(graph4_t *g, const char *pattern, u32 *out, u32 max);
int g4_regex_valid(const char *pattern);
/* bring indexes current (deferred from writes); search calls it itself */
void g4_index_sync(graph4_t *g);
u32 g4_entities_by_type(graph4_t *g, const u8 *type, u16 tlen, u32 *out, u32 max);
u32 g4_entity_types(graph4_t *g, u32 *out_sids, u32 max);   /* distinct type sids */
u32 g4_relation_types(graph4_t *g, u32 *out_sids, u32 max); /* distinct rel sids (O(E) scan) */
u32 g4_orphaned(graph4_t *g, u32 *out, u32 max);            /* eids with no edges */

/* ---- rank + walks (verbatim v3 algorithms, eid-keyed; xorshift64 RNG,
 * same seed => same walks/samples when candidate order matches; totals are
 * recomputed from records at open and maintained in memory) ---- */
void   g4_seed_rng(u64 seed);
void   g4_inc_structural_visit(graph4_t *g, u32 eid);
void   g4_inc_walker_visit(graph4_t *g, u32 eid);
u64    g4_structural_total(graph4_t *g);
u64    g4_walker_total(graph4_t *g);
double g4_structural_rank(graph4_t *g, u32 eid);
double g4_walker_rank(graph4_t *g, u32 eid);
double g4_get_psi(graph4_t *g, u32 eid);
u32    g4_relation_count(graph4_t *g);                      /* O(E) fwd sweep */
/* MC pagerank: `iterations` damped forward walks from every entity */
u32    g4_structural_sample(graph4_t *g, u32 iterations, double damping);
/* MERW psi power iteration (warm-started from stored psi); returns iters */
u32    g4_compute_merw_psi(graph4_t *g, double alpha, u32 max_iter, double tol);
/* random walk; merw_mode weights by target psi; seed 0 = global rng.
 * avoid_cycles: 1 = self-avoiding (never revisits a node; stops early when
 * every neighbor is already on the path).
 * out_uniform_steps (optional): merw-requested steps that fell back to
 * uniform sampling because psi weighting was unavailable at that step. */
u32    g4_random_walk(graph4_t *g, u32 start, u32 depth, u32 direction,
                      int merw_mode, u64 seed, int avoid_cycles, u32 *out_path, u32 max_path,
                      u32 *out_uniform_steps);
/* migration support: restore preserved fields on an existing entity */
/* Restore global totals verbatim (v3->v4 import; source totals may exceed
 * the sum of live per-entity visits when entities were deleted upstream). */
void   g4_set_totals(graph4_t *g, u64 structural_total, u64 walker_total);
int    g4_set_entity_fields(graph4_t *g, u32 eid, u64 mtime, u64 obs_mtime,
                            u64 svis, u64 wvis, double psi);

/* ------------------------------------------------------------------ *
 * mstore — multi-segment store with a manifest pivot (seg_mstore.c).
 *
 * Spec r2 §3 cross-segment atomicity, Q5 resolved CONSERVATIVELY: a
 * manifest record is appended (and fsync'd) on EVERY store commit — the
 * record is the store-level commit point. Single-segment folding is a
 * later optimization, not a correctness feature.
 *
 * Why recovery always works (the dual-meta selection property): a store
 * commit advances each dirty segment's meta by exactly one toggle, so the
 * two meta slots of every segment always hold the last two states the
 * store could want. The manifest's last valid record names the exact seg
 * txid per segment; segstore_open_at selects the matching slot, rolling
 * back any segment that ran ahead (crash after its toggle, before the
 * manifest append). The orphaned newer slot is overwritten by the next
 * commit (it is the inactive slot after an _at open).
 *
 * Manifest record (LE):
 *   [u32 MST_MAGIC][u32 nsegs][u64 store_txid]
 *   [(u32 seg_id, u32 pad0, u64 seg_txid) x nsegs][u32 crc32c(record sans crc)]
 * Recovery scans forward; the last fully-valid record wins; a torn tail is
 * ignored. Records always list ALL segments (nsegs small by design).
 *
 * The embedder owns file naming/opening: mstore takes one seg_io for the
 * manifest and one per segment (index == seg_id). All ios are owned by the
 * mstore after a successful create/open (closed by mstore_close).
 * ------------------------------------------------------------------ */

#define MST_MAGIC 0x4D535431u   /* "MST1" */

mstore_t *mstore_create(seg_io_t *manifest_io, seg_io_t **seg_ios, u32 nsegs,
                        u16 extent_pages_log2);
mstore_t *mstore_open(seg_io_t *manifest_io, seg_io_t **seg_ios, u32 nsegs);
void      mstore_close(mstore_t *ms);
u64       mstore_txid(const mstore_t *ms);
u32       mstore_nsegs(const mstore_t *ms);
/* the per-segment store, for reads and txn ops */
segstore_t *mstore_seg(mstore_t *ms, u32 seg_id);

/* store-level txn: begin on all segments; ops go through seg_txn_* on the
 * individual segments; commit = per-segment commits (dirty segments only,
 * as store_txid+1) + manifest append (the pivot). */
int mstore_txn_begin(mstore_t *ms);
int mstore_txn_commit(mstore_t *ms);
void mstore_txn_abort(mstore_t *ms);

#endif /* SEGSTORE_H */
