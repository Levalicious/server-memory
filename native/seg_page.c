/*
 * seg_page.c — general slotted page (libsegstore proof target #2).
 *
 * Layout within one SEG_PAGE_SIZE buffer:
 *   [seg_page_hdr_t 16B][slot array, 4B each, ascending][free][records, descending]
 *
 * Invariants (checked by seg_page_validate, maintained by every mutator):
 *   I1  SEG_PAGE_HDR_SIZE + nslots*SEG_SLOT_SIZE <= rec_floor <= SEG_PAGE_SIZE
 *   I2  live slot s: rec_floor <= s.offset  &&  s.offset + s.size <= SEG_PAGE_SIZE
 *   I3  live slot s: s.size >= 1
 *   (records may overlap only via garbage — live slots written by this module
 *    never alias, guaranteed by bump-down allocation + compact; validated
 *    exhaustively by the fuzz-vs-model harness, not per-op.)
 *
 * WP discipline (Heuristic_WP_RealMmapCode): byte-buffer access goes through
 * oracle accessors (hdr/slot load-store) with reads/assigns contracts — the
 * trust boundary. Offset arithmetic and invariant preservation above them
 * prove deductively. Functional record-byte equality is the fuzz harness's
 * job (byte-quantified loop invariants drown WP; known lesson).
 */
#include "segstore.h"

/* ------------------------------------------------------------------ *
 * Oracle accessors (trust boundary: unaligned-safe, cast-free byte access)
 * ------------------------------------------------------------------ */

/*@ requires \valid_read(p) && \valid_read(p+1);
    assigns \nothing; */
static u16 ld16(const u8 *p) { return (u16)(((u32)p[1] << 8) | (u32)p[0]); }

/*@ requires \valid(p) && \valid(p+1);
    assigns p[0..1]; */
static void st16(u8 *p, u16 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }

/* header field offsets (packed struct, LE) */
#define OFF_NSLOTS    0u
#define OFF_RECFLOOR  2u
#define OFF_KIND      4u
#define OFF_FLAGS     6u
/* slot s lives at SEG_PAGE_HDR_SIZE + s*4: [u16 offset][u16 size] */

/* max slots a page could ever hold: (8192-16)/4 = 2044 */
#define SEG_PAGE_MAX_SLOTS ((SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / SEG_SLOT_SIZE)

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1)); assigns \nothing; */
static u16 pg_nslots(const u8 *pg)    { return ld16(pg + OFF_NSLOTS); }
/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1)); assigns \nothing; */
static u16 pg_recfloor(const u8 *pg)  { return ld16(pg + OFF_RECFLOOR); }

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    requires s < SEG_PAGE_MAX_SLOTS;
    assigns \nothing; */
static u16 slot_off(const u8 *pg, u16 s)  { return ld16(pg + SEG_PAGE_HDR_SIZE + (u32)s * SEG_SLOT_SIZE); }
/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    requires s < SEG_PAGE_MAX_SLOTS;
    assigns \nothing; */
static u16 slot_size(const u8 *pg, u16 s) { return ld16(pg + SEG_PAGE_HDR_SIZE + (u32)s * SEG_SLOT_SIZE + 2); }

/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    requires s < SEG_PAGE_MAX_SLOTS;
    assigns pg[SEG_PAGE_HDR_SIZE + s*SEG_SLOT_SIZE .. SEG_PAGE_HDR_SIZE + s*SEG_SLOT_SIZE + 3]; */
static void slot_set(u8 *pg, u16 s, u16 off, u16 size) {
    st16(pg + SEG_PAGE_HDR_SIZE + (u32)s * SEG_SLOT_SIZE, off);
    st16(pg + SEG_PAGE_HDR_SIZE + (u32)s * SEG_SLOT_SIZE + 2, size);
}

/* (SEG_PAGE_MAX_SLOTS is defined above, next to its first use.) */

/*@ requires \valid(dst + (0 .. n-1)) && \valid_read(src + (0 .. n-1));
    requires \separated(dst + (0 .. n-1), src + (0 .. n-1));
    assigns dst[0 .. n-1]; */
static void rec_copy(u8 *dst, const u8 *src, u16 n) {
    /*@ loop invariant 0 <= i <= n;
        loop assigns i, dst[0 .. n-1];
        loop variant n - i; */
    for (u16 i = 0; i < n; i++) dst[i] = src[i];
}

/* ------------------------------------------------------------------ *
 * API
 * ------------------------------------------------------------------ */

/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns pg[0 .. SEG_PAGE_SIZE-1]; */
void seg_page_init(u8 *pg, u16 kind_hint) {
    /*@ loop invariant 0 <= i <= SEG_PAGE_HDR_SIZE;
        loop assigns i, pg[0 .. SEG_PAGE_HDR_SIZE-1];
        loop variant SEG_PAGE_HDR_SIZE - i; */
    for (u32 i = 0; i < SEG_PAGE_HDR_SIZE; i++) pg[i] = 0;
    st16(pg + OFF_NSLOTS,   0);
    st16(pg + OFF_RECFLOOR, (u16)SEG_PAGE_SIZE);
    st16(pg + OFF_KIND,     kind_hint);
}

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns \nothing; */
u16 seg_page_nslots(const u8 *pg) { return pg_nslots(pg); }

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns \nothing; */
u16 seg_page_kind(const u8 *pg) { return ld16(pg + OFF_KIND); }

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns \nothing;
    ensures \result == 0 || \result == 1; */
int seg_page_validate(const u8 *pg) {
    u32 nslots = pg_nslots(pg);
    u32 floor  = pg_recfloor(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;
    u32 slots_end = SEG_PAGE_HDR_SIZE + nslots * SEG_SLOT_SIZE;
    if (slots_end > floor || floor > SEG_PAGE_SIZE) return 0;        /* I1 */
    /*@ loop invariant 0 <= s <= nslots;
        loop assigns s;
        loop variant nslots - s; */
    for (u32 s = 0; s < nslots; s++) {
        u32 off = slot_off(pg, (u16)s);
        if (off == 0) continue;                                       /* dead */
        u32 size = slot_size(pg, (u16)s);
        if (size < 1) return 0;                                       /* I3 */
        if (off < floor || off + size > SEG_PAGE_SIZE) return 0;      /* I2 */
    }
    return 1;
}

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns \nothing; */
u32 seg_page_free_space(const u8 *pg) {
    u32 nslots    = pg_nslots(pg);
    u32 floor     = pg_recfloor(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;                /* corrupt header */
    u32 slots_end = SEG_PAGE_HDR_SIZE + nslots * SEG_SLOT_SIZE;
    if (slots_end > floor || floor > SEG_PAGE_SIZE) return 0; /* corrupt header */
    u32 gap = floor - slots_end;
    /* a fresh insert may need a new slot entry unless a dead one exists;
     * report conservatively (new-slot case) */
    return gap > SEG_SLOT_SIZE ? gap - SEG_SLOT_SIZE : 0;
}

/*@ requires \valid_read(pg + (0 .. SEG_PAGE_SIZE-1));
    requires size_out == \null || \valid(size_out);
    assigns *size_out; */
const u8 *seg_page_read(const u8 *pg, u16 slot, u16 *size_out) {
    u32 nslots = pg_nslots(pg);
    u32 floor  = pg_recfloor(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS || floor > SEG_PAGE_SIZE) return 0;
    if (slot >= nslots) return 0;
    u32 off = slot_off(pg, slot);
    if (off == 0) return 0;
    u32 size = slot_size(pg, slot);
    if (size < 1 || off < floor || off + size > SEG_PAGE_SIZE) return 0;  /* I2/I3 */
    if (size_out) *size_out = (u16)size;
    return pg + off;
}

/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    requires size == 0 || \valid_read(rec + (0 .. size-1));
    requires \separated(pg + (0 .. SEG_PAGE_SIZE-1), rec + (0 .. size-1));
    requires slot_out == \null || \valid(slot_out);
    assigns pg[0 .. SEG_PAGE_SIZE-1], *slot_out; */
int seg_page_insert(u8 *pg, const u8 *rec, u16 size, u16 *slot_out) {
    if (size < 1 || size > SEG_PAGE_MAX_REC) return 0;
    u32 nslots    = pg_nslots(pg);
    u32 floor     = pg_recfloor(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;                /* corrupt header */
    u32 slots_end = SEG_PAGE_HDR_SIZE + nslots * SEG_SLOT_SIZE;
    if (slots_end > floor || floor > SEG_PAGE_SIZE) return 0; /* corrupt header */

    /* find lowest dead slot (reuse), else plan an append */
    u32 target = nslots;
    /*@ loop invariant 0 <= s <= nslots;
        loop invariant target <= nslots;
        loop assigns s, target;
        loop variant nslots - s; */
    for (u32 s = 0; s < nslots; s++)
        if (slot_off(pg, (u16)s) == 0) { target = s; break; }

    u32 need = size;
    if (target == nslots) {                       /* appending a slot entry */
        if (nslots >= SEG_PAGE_MAX_SLOTS) return 0;
        need += SEG_SLOT_SIZE;
        slots_end += SEG_SLOT_SIZE;
    }
    if (floor - (slots_end - (target == nslots ? SEG_SLOT_SIZE : 0)) < need)
        return 0;                                 /* page full */

    u16 off = (u16)(floor - size);
    rec_copy(pg + off, rec, size);
    slot_set(pg, (u16)target, off, size);
    st16(pg + OFF_RECFLOOR, off);
    if (target == nslots) st16(pg + OFF_NSLOTS, (u16)(nslots + 1));
    if (slot_out) *slot_out = (u16)target;
    return 1;
}

/* Bulk-refill a page with n records in order (the COW node-refresh path:
 * a B+tree node is re-emitted whole on every mutation).  Equivalent to
 * seg_page_init + n ordered appends, in ONE pass — records packed top-down
 * in the order given, slots 0..n-1 pointing at them.  1 = ok, 0 = bad args
 * or overflow. */
/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    requires n <= SEG_PAGE_MAX_SLOTS;
    requires \valid_read(recs + (0 .. n-1)) && \valid_read(sizes + (0 .. n-1));
    assigns pg[0 .. SEG_PAGE_SIZE-1]; */
int seg_page_fill(u8 *pg, u16 kind_hint, const u8 *const *recs, const u16 *sizes, u32 n) {
    if (n > SEG_PAGE_MAX_SLOTS) return 0;
    u32 total = SEG_PAGE_HDR_SIZE + n * SEG_SLOT_SIZE;
    for (u32 i = 0; i < n; i++) {
        if (sizes[i] < 1 || sizes[i] > SEG_PAGE_MAX_REC) return 0;
        total += sizes[i];
    }
    if (total > SEG_PAGE_SIZE) return 0;
    seg_page_init(pg, kind_hint);
    u32 floor = SEG_PAGE_SIZE;
    for (u32 i = 0; i < n; i++) {
        floor -= sizes[i];
        rec_copy(pg + floor, recs[i], sizes[i]);
        slot_set(pg, (u16)i, (u16)floor, sizes[i]);
    }
    if (n) st16(pg + OFF_RECFLOOR, (u16)floor);
    st16(pg + OFF_NSLOTS, (u16)n);
    return 1;
}

/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    requires size == 0 || \valid_read(rec + (0 .. size-1));
    requires \separated(pg + (0 .. SEG_PAGE_SIZE-1), rec + (0 .. size-1));
    assigns pg[0 .. SEG_PAGE_SIZE-1]; */
int seg_page_update(u8 *pg, u16 slot, const u8 *rec, u16 size) {
    if (size < 1 || size > SEG_PAGE_MAX_REC) return 0;
    u32 nslots = pg_nslots(pg);
    u32 floor  = pg_recfloor(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;                /* corrupt header */
    u32 slots_end0 = SEG_PAGE_HDR_SIZE + nslots * SEG_SLOT_SIZE;
    if (slots_end0 > floor || floor > SEG_PAGE_SIZE) return 0;/* corrupt header */
    if (slot >= nslots) return 0;
    u32 old_off  = slot_off(pg, slot);
    if (old_off == 0) return 0;                   /* dead */
    u32 old_size = slot_size(pg, slot);
    if (old_size < 1 || old_off < floor || old_off + old_size > SEG_PAGE_SIZE)
        return 0;                                 /* corrupt slot (I2/I3) */

    if (size <= old_size) {                       /* in place (shrink leaves tail garbage) */
        rec_copy(pg + old_off, rec, size);
        slot_set(pg, slot, old_off, size);
        return 1;
    }
    /* grow: new copy in free space; old bytes become garbage */
    if (floor - slots_end0 < size) return 0;
    u16 off = (u16)(floor - size);
    rec_copy(pg + off, rec, size);
    slot_set(pg, slot, off, size);
    st16(pg + OFF_RECFLOOR, off);
    return 1;
}

/*@ requires \valid(pg + (0 .. SEG_PAGE_SIZE-1));
    assigns pg[0 .. SEG_PAGE_SIZE-1]; */
int seg_page_delete(u8 *pg, u16 slot) {
    u32 nslots = pg_nslots(pg);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;    /* corrupt header */
    if (slot >= nslots) return 0;
    if (slot_off(pg, slot) == 0) return 0;
    slot_set(pg, slot, 0, 0);
    return 1;
}

/*@ requires \valid(dst + (0 .. SEG_PAGE_SIZE-1));
    requires \valid_read(src + (0 .. SEG_PAGE_SIZE-1));
    requires \separated(dst + (0 .. SEG_PAGE_SIZE-1), src + (0 .. SEG_PAGE_SIZE-1));
    assigns dst[0 .. SEG_PAGE_SIZE-1]; */
int seg_page_compact(u8 *dst, const u8 *src) {
    if (!seg_page_validate(src)) return 0;
    u32 nslots = pg_nslots(src);
    if (nslots > SEG_PAGE_MAX_SLOTS) return 0;    /* re-establish bound for WP */

    /* trailing dead slots trim */
    u32 keep = nslots;
    /*@ loop invariant 0 <= keep <= nslots;
        loop assigns keep;
        loop variant keep; */
    while (keep > 0 && slot_off(src, (u16)(keep - 1)) == 0) keep--;

    seg_page_init(dst, ld16(src + OFF_KIND));
    st16(dst + OFF_FLAGS, ld16(src + OFF_FLAGS));
    st16(dst + OFF_NSLOTS, (u16)keep);

    u32 slots_end = SEG_PAGE_HDR_SIZE + keep * SEG_SLOT_SIZE;
    u32 floor = SEG_PAGE_SIZE;
    /*@ loop invariant 0 <= s <= keep;
        loop invariant slots_end <= floor <= SEG_PAGE_SIZE;
        loop assigns s, floor, dst[0 .. SEG_PAGE_SIZE-1];
        loop variant keep - s; */
    for (u32 s = 0; s < keep; s++) {
        u16 off = slot_off(src, (u16)s);
        if (off == 0) { slot_set(dst, (u16)s, 0, 0); continue; }
        u16 size = slot_size(src, (u16)s);
        /* re-check I2/I3 (validate guarantees them, but WP cannot see through
         * validate's semantics; the checks are free and make this total) */
        if (size < 1 || (u32)off + size > SEG_PAGE_SIZE) return 0;
        /* per-slot bounds hold (validated), but the SUM of live sizes can
         * exceed capacity only if live records alias — corruption this
         * module never produces. Refuse rather than wrap. */
        if (size > floor - slots_end) return 0;
        floor -= size;
        rec_copy(dst + floor, src + off, size);
        slot_set(dst, (u16)s, (u16)floor, size);
    }
    st16(dst + OFF_RECFLOOR, (u16)floor);
    /* zero the free gap: stale heap bytes must never reach disk */
    /*@ loop invariant slots_end <= i <= floor;
        loop assigns i, dst[0 .. SEG_PAGE_SIZE-1];
        loop variant floor - i; */
    for (u32 i = slots_end; i < floor; i++) dst[i] = 0;
    return 1;
}
