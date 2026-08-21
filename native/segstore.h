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
#define SEG_PAGE_SIZE    4096u         /* COW unit; recorded in meta (Q1 prior) */
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
    u64 watermark;            /* page count: pages [0, watermark) are addressable */
    u32 nameindex_root_pgno;  /* graph-layer roots; 0 = none */
    u32 indirect_root_pgno;   /* node-id indirection table root */
    u32 freelist_root_pgno;   /* retired-page list root */
    u32 _reserved0;
    u64 _reserved1[4];
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

#endif /* SEGSTORE_H */
