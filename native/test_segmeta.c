/*
 * test_segmeta.c — unit table for the dual-meta recovery pivot.
 *
 * Covers: init/seal/valid roundtrip, every corruption class recovery must
 * survive (torn write at every byte boundary, bit flips, stale-vs-new pick,
 * wrong magic/version/page_size), pick's full 2x2 validity matrix, ref
 * packing roundtrip, and struct-layout freeze guards.
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

int main(void) {
    printf("test_segmeta:\n");

    /* ---- layout freeze guards (format-freeze candidates) ---- */
    TEST(layout_sizes);
    assert(sizeof(seg_meta_t) == 4+4+4+2+2+8+8+4+4+4+4+32+4+4);
    assert(offsetof(seg_meta_t, checksum) == sizeof(seg_meta_t) - 4);
    assert(sizeof(seg_page_hdr_t) == 16);
    assert(sizeof(seg_slot_t) == 4);
    assert(SEG_PAGE_MAX_REC == 4096 - 16 - 4);
    assert(sizeof(seg_meta_t) <= SEG_PAGE_SIZE);
    PASS();

    /* ---- ref packing roundtrip ---- */
    TEST(ref_pack_roundtrip);
    {
        seg_ref_t r = seg_ref_make(0xBEEF, 0xDEADBEEFu, 0xCAFE);
        assert(seg_ref_seg(r)  == 0xBEEF);
        assert(seg_ref_pgno(r) == 0xDEADBEEFu);
        assert(seg_ref_slot(r) == 0xCAFE);
        assert(seg_ref_make(0, 0, 0) == SEG_REF_NULL);
    }
    PASS();

    /* ---- crc32c known-answer tests (RFC 3720 / Intel vectors) ---- */
    TEST(crc32c_known_answers);
    {
        /* "123456789" -> 0xE3069283 (canonical crc32c check value) */
        assert(seg_crc32c((const u8 *)"123456789", 9) == 0xE3069283u);
        /* 32 zero bytes -> 0x8A9136AA (iSCSI test vector) */
        u8 z[32] = {0};
        assert(seg_crc32c(z, 32) == 0x8A9136AAu);
        /* 32 0xFF bytes -> 0x62A8AB43 */
        u8 f[32]; memset(f, 0xFF, 32);
        assert(seg_crc32c(f, 32) == 0x62A8AB43u);
        assert(seg_crc32c((const u8 *)"", 0) == 0x00000000u);
    }
    PASS();

    /* ---- init/seal/valid roundtrip ---- */
    seg_meta_t a, b;
    TEST(init_is_valid);
    seg_meta_init(&a, 7, 4);
    assert(seg_meta_valid(&a) == 1);
    assert(a.seg_id == 7 && a.extent_pages_log2 == 4);
    assert(a.txid == 0 && a.watermark == SEG_META_PAGES);
    PASS();

    TEST(edit_without_seal_invalid_edit_with_seal_valid);
    a.txid = 42; a.watermark = 100; a.freelist_root_pgno = 9;
    assert(seg_meta_valid(&a) == 0);        /* stale checksum detected */
    seg_meta_seal(&a);
    assert(seg_meta_valid(&a) == 1);
    PASS();

    /* ---- corruption classes ---- */
    TEST(wrong_magic_version_pagesize_rejected);
    b = a; b.magic ^= 1;            seg_meta_seal(&b); b.magic = a.magic ^ 1;
    assert(seg_meta_valid(&b) == 0);
    b = a; b.format_version += 1;   /* checksum now stale too, but even */
    seg_meta_seal(&b);              /* resealed: version gate alone must reject */
    assert(seg_meta_valid(&b) == 0);
    b = a; b.page_size = 16384;     seg_meta_seal(&b);
    assert(seg_meta_valid(&b) == 0);
    PASS();

    TEST(every_single_bitflip_detected);
    {
        int flips = 0;
        for (size_t byte = 0; byte < sizeof(seg_meta_t); byte++) {
            for (int bit = 0; bit < 8; bit++) {
                b = a;
                ((u8 *)&b)[byte] ^= (u8)(1u << bit);
                if (seg_meta_valid(&b)) { flips++; }
            }
        }
        assert(flips == 0);   /* no single-bit corruption survives */
    }
    PASS();

    TEST(torn_write_every_prefix_detected);
    {
        /* simulate a torn meta write: first k bytes of the NEW meta land,
         * the tail still holds the OLD meta's bytes. k = 0 must yield the
         * old (valid) meta; k = sizeof must yield the new (valid) meta;
         * every strictly-partial k must be rejected OR equal one of the
         * two legal images (when the prefixes happen to coincide). */
        seg_meta_t oldm, newm;
        seg_meta_init(&oldm, 3, 4);
        newm = oldm; newm.txid = 99; newm.watermark = 500;
        newm.nameindex_root_pgno = 11; seg_meta_seal(&newm);
        for (size_t k = 0; k <= sizeof(seg_meta_t); k++) {
            seg_meta_t torn = oldm;
            memcpy(&torn, &newm, k);
            int v = seg_meta_valid(&torn);
            if (memcmp(&torn, &oldm, sizeof torn) == 0) assert(v == 1);
            else if (memcmp(&torn, &newm, sizeof torn) == 0) assert(v == 1);
            else assert(v == 0);
        }
    }
    PASS();

    /* ---- pick: full validity matrix ---- */
    TEST(pick_matrix);
    {
        seg_meta_t v_lo, v_hi, bad;
        seg_meta_init(&v_lo, 1, 4);
        v_lo.txid = 10; seg_meta_seal(&v_lo);
        v_hi = v_lo; v_hi.txid = 11; seg_meta_seal(&v_hi);
        bad = v_hi; bad.checksum ^= 0xFFFFu;

        assert(seg_meta_pick(&bad,  &bad)  == -1);  /* neither */
        assert(seg_meta_pick(&v_lo, &bad)  == 0);   /* only m0 */
        assert(seg_meta_pick(&bad,  &v_lo) == 1);   /* only m1 */
        assert(seg_meta_pick(&v_lo, &v_hi) == 1);   /* both: max txid */
        assert(seg_meta_pick(&v_hi, &v_lo) == 0);
        assert(seg_meta_pick(&v_lo, &v_lo) == 0);   /* tie -> 0 */
    }
    PASS();

    TEST(pick_survives_torn_partner);
    {
        /* recovery scenario: commit tore while writing meta slot 1; slot 0
         * holds the previous commit. For EVERY torn prefix, pick must land
         * on a valid meta and never on the torn one (unless the tear
         * happens to complete the image). */
        seg_meta_t prev, next;
        seg_meta_init(&prev, 2, 4);
        prev.txid = 7; seg_meta_seal(&prev);
        next = prev; next.txid = 8; next.watermark = 64; seg_meta_seal(&next);
        for (size_t k = 0; k <= sizeof(seg_meta_t); k++) {
            seg_meta_t slot1 = prev;               /* old bytes underneath */
            memcpy(&slot1, &next, k);              /* torn new write */
            int p = seg_meta_pick(&prev, &slot1);
            assert(p == 0 || p == 1);              /* never -1: prev is valid */
            const seg_meta_t *chosen = p ? &slot1 : &prev;
            assert(seg_meta_valid(chosen) == 1);
            assert(chosen->txid == 7 || chosen->txid == 8);
        }
    }
    PASS();

    printf("test_segmeta: %d tests passed\n", tests_run);
    return 0;
}
