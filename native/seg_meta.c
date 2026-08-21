/*
 * seg_meta.c — dual-meta recovery pivot for libsegstore. Pure, no I/O.
 *
 * Proof target #1 (Design_Libsegstore): meta-pick correctness. ACSL contracts
 * on every function; WP + RTE discharged via `make wp-segmeta`.
 *
 * Trust boundary (technique per Heuristic_WP_RealMmapCode — assume the byte
 * oracle, prove above it): the crc32c computation is abstracted as the ACSL
 * logic function crc_of(m); meta_body_crc's ensures binds the C computation
 * to it and is the ONE assumed (unproved-by-construction) obligation. All
 * validity/pick logic above that line proves deductively.
 */
#include "segstore.h"
/* no string.h: no memset, no byte-blasting of structs */

/*@ axiomatic MetaCrc {
      logic integer crc_of{L}(seg_meta_t *m) reads *m;
    }
*/

/*@ predicate meta_valid_p{L}(seg_meta_t *m) =
      m->magic == SEG_MAGIC
   && m->format_version == SEG_FORMAT_VER
   && m->page_size == SEG_PAGE_SIZE
   && m->checksum == crc_of(m);
*/

/* ------------------------------------------------------------------ *
 * crc32c — Castagnoli polynomial (reflected 0x82F63B78), byte-at-a-time
 * table. Table generation is deterministic and idempotent; libsegstore is
 * single-writer by design, so no init race exists.
 * ------------------------------------------------------------------ */

static u32 crc_table[256];
static int crc_table_ready = 0;

/*@ assigns crc_table[0..255], crc_table_ready;
    ensures crc_table_ready == 1;
*/
static void crc_init(void) {
    /*@ loop invariant 0 <= i <= 256;
        loop assigns i, c, j, crc_table[0..255];
        loop variant 256 - i; */
    for (u32 i = 0, c = 0, j = 0; i < 256; i++) {
        c = i;
        /*@ loop invariant 0 <= j <= 8;
            loop assigns j, c;
            loop variant 8 - j; */
        for (j = 0; j < 8; j++)
            c = (c & 1u) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        crc_table[i] = c;
    }
    crc_table_ready = 1;
}

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns crc_table[0..255], crc_table_ready;
*/
u32 seg_crc32c(const u8 *data, size_t len) {
    if (!crc_table_ready) crc_init();
    u32 crc = 0xFFFFFFFFu;
    /*@ loop invariant 0 <= i <= len;
        loop assigns i, crc;
        loop variant len - i; */
    for (size_t i = 0; i < len; i++)
        crc = crc_table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

/* ------------------------------------------------------------------ *
 * Checksum over the meta with its checksum field excluded: the checksum is
 * the LAST field, so this is crc over [0, offsetof(checksum)).
 *
 * TRUST BOUNDARY: the ensures below binds the C bytes-crc to the abstract
 * crc_of. It is assumed, not proved (crc arithmetic is outside the model).
 * ------------------------------------------------------------------ */

/*@ requires \valid_read(m);
    assigns crc_table[0..255], crc_table_ready;
    ensures \result == crc_of(m);
*/
static u32 meta_body_crc(const seg_meta_t *m) {
    return seg_crc32c((const u8 *)m, offsetof(seg_meta_t, checksum));
}

/*@ requires \valid(m);
    assigns m->checksum, crc_table[0..255], crc_table_ready;
*/
void seg_meta_seal(seg_meta_t *m) {
    m->checksum = meta_body_crc(m);
}

/*@ requires \valid(m);
    assigns *m, crc_table[0..255], crc_table_ready;
    ensures m->magic == SEG_MAGIC;
    ensures m->format_version == SEG_FORMAT_VER;
    ensures m->page_size == SEG_PAGE_SIZE;
    ensures m->seg_id == seg_id;
    ensures m->extent_pages_log2 == extent_pages_log2;
    ensures m->txid == 0;
    ensures m->watermark == SEG_META_PAGES;
*/
void seg_meta_init(seg_meta_t *m, u16 seg_id, u16 extent_pages_log2) {
    *m = (seg_meta_t){0};   /* struct assignment: WP-provable, unlike memset */
    m->magic             = SEG_MAGIC;
    m->format_version    = SEG_FORMAT_VER;
    m->page_size         = SEG_PAGE_SIZE;
    m->seg_id            = seg_id;
    m->extent_pages_log2 = extent_pages_log2;
    m->txid              = 0;
    m->watermark         = SEG_META_PAGES;   /* pages 0,1 are the metas */
    seg_meta_seal(m);
}

/*@ requires \valid_read(m);
    assigns crc_table[0..255], crc_table_ready;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==> meta_valid_p(m);
*/
int seg_meta_valid(const seg_meta_t *m) {
    if (m->magic != SEG_MAGIC)               return 0;
    if (m->format_version != SEG_FORMAT_VER) return 0;
    if (m->page_size != SEG_PAGE_SIZE)       return 0;
    return m->checksum == meta_body_crc(m) ? 1 : 0;
}

/*@ requires \valid_read(m0) && \valid_read(m1);
    assigns crc_table[0..255], crc_table_ready;
    ensures \result == -1 || \result == 0 || \result == 1;
    behavior neither:
      assumes !meta_valid_p(m0) && !meta_valid_p(m1);
      ensures \result == -1;
    behavior only0:
      assumes  meta_valid_p(m0) && !meta_valid_p(m1);
      ensures \result == 0;
    behavior only1:
      assumes !meta_valid_p(m0) &&  meta_valid_p(m1);
      ensures \result == 1;
    behavior both:
      assumes  meta_valid_p(m0) &&  meta_valid_p(m1);
      ensures (m0->txid >= m1->txid ==> \result == 0)
           && (m0->txid <  m1->txid ==> \result == 1);
    complete behaviors;
    disjoint behaviors;
*/
int seg_meta_pick(const seg_meta_t *m0, const seg_meta_t *m1) {
    int v0 = seg_meta_valid(m0);
    int v1 = seg_meta_valid(m1);
    if (!v0 && !v1) return -1;
    if (v0 && !v1)  return 0;
    if (!v0 && v1)  return 1;
    /* both valid: highest txid wins; tie -> 0 (fresh store: identical metas) */
    return (m1->txid > m0->txid) ? 1 : 0;
}
