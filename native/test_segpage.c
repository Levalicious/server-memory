/*
 * test_segpage.c — slotted page unit table + fuzz-vs-model.
 *
 * The fuzz half is the functional-correctness authority (byte equality,
 * slot-id stability across compact, free-space accounting): a shadow model
 * (plain arrays) runs every op in parallel with the page; after every op the
 * page must validate and read-back must match the model byte-for-byte.
 * WP owns bounds/termination; this owns semantics. (Design_Libsegstore)
 */
#include "segstore.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

/* deterministic PRNG (splitmix64) — no libc rand */
static u64 rng_state;
static u64 rng(void) {
    u64 z = (rng_state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

/* ---- shadow model ---- */
#define MODEL_MAX ((SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / SEG_SLOT_SIZE)
typedef struct {
    int live[MODEL_MAX];
    u16 size[MODEL_MAX];
    u8  bytes[MODEL_MAX][512];
    u32 nslots;
} model_t;

static void model_check(const model_t *md, const u8 *pg) {
    assert(seg_page_validate(pg) == 1);
    assert(pg == pg); /* silence */
    for (u32 s = 0; s < md->nslots; s++) {
        u16 sz = 0;
        const u8 *rec = seg_page_read(pg, (u16)s, &sz);
        if (!md->live[s]) { assert(rec == 0); continue; }
        assert(rec != 0);
        assert(sz == md->size[s]);
        assert(memcmp(rec, md->bytes[s], sz) == 0);
    }
    /* reads past model nslots must fail (page may have fewer after compact trim) */
    assert(seg_page_read(pg, (u16)(MODEL_MAX + 1), 0) == 0);
}

int main(void) {
    printf("test_segpage:\n");
    static u8 pg[SEG_PAGE_SIZE], cp[SEG_PAGE_SIZE];

    TEST(init_validate_freespace);
    seg_page_init(pg, SEG_KIND_ENTITY);
    assert(seg_page_validate(pg) == 1);
    assert(seg_page_free_space(pg) == SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE - SEG_SLOT_SIZE);
    assert(seg_page_read(pg, 0, 0) == 0);
    PASS();

    TEST(insert_read_roundtrip);
    {
        u8 rec[76]; for (int i = 0; i < 76; i++) rec[i] = (u8)i;
        u16 slot = 999;
        assert(seg_page_insert(pg, rec, 76, &slot) == 1);
        assert(slot == 0);
        u16 sz = 0;
        const u8 *r = seg_page_read(pg, slot, &sz);
        assert(r && sz == 76 && memcmp(r, rec, 76) == 0);
        assert(seg_page_validate(pg) == 1);
    }
    PASS();

    TEST(fill_page_to_capacity_then_reject);
    {
        seg_page_init(pg, SEG_KIND_ENTITY);
        u8 rec[80]; memset(rec, 0xAB, sizeof rec);
        u32 n = 0; u16 slot;
        while (seg_page_insert(pg, rec, 80, &slot)) { assert(slot == n); n++; }
        /* capacity: n*(80+4) <= 4080 -> n = 48 */
        assert(n == (SEG_PAGE_SIZE - SEG_PAGE_HDR_SIZE) / (80 + SEG_SLOT_SIZE));
        assert(seg_page_insert(pg, rec, 80, &slot) == 0);   /* still full */
        assert(seg_page_validate(pg) == 1);
    }
    PASS();

    TEST(delete_then_reuse_lowest_dead_slot);
    {
        u16 slot;
        assert(seg_page_delete(pg, 7) == 1);
        assert(seg_page_delete(pg, 7) == 0);          /* double delete refused */
        assert(seg_page_read(pg, 7, 0) == 0);
        assert(seg_page_delete(pg, 3) == 1);
        u8 rec[16]; memset(rec, 0xCD, sizeof rec);
        assert(seg_page_insert(pg, rec, 16, &slot) == 1);
        assert(slot == 3);                            /* lowest dead reused */
        assert(seg_page_validate(pg) == 1);
    }
    PASS();

    TEST(update_same_shrink_grow);
    {
        seg_page_init(pg, SEG_KIND_STRING);
        u8 a[100], b[100], c[200];
        memset(a, 1, sizeof a); memset(b, 2, sizeof b); memset(c, 3, sizeof c);
        u16 slot, sz;
        assert(seg_page_insert(pg, a, 100, &slot) == 1);
        assert(seg_page_update(pg, slot, b, 100) == 1);        /* same size */
        assert(memcmp(seg_page_read(pg, slot, &sz), b, 100) == 0 && sz == 100);
        assert(seg_page_update(pg, slot, b, 40) == 1);         /* shrink */
        assert(seg_page_read(pg, slot, &sz) && sz == 40);
        assert(seg_page_update(pg, slot, c, 200) == 1);        /* grow */
        assert(memcmp(seg_page_read(pg, slot, &sz), c, 200) == 0 && sz == 200);
        assert(seg_page_update(pg, 9, c, 10) == 0);            /* no such slot */
        assert(seg_page_validate(pg) == 1);
    }
    PASS();

    TEST(compact_preserves_slots_reclaims_garbage);
    {
        seg_page_init(pg, SEG_KIND_ADJ);
        u8 r[64]; u16 s0, s1, s2;
        memset(r, 7, 64); assert(seg_page_insert(pg, r, 64, &s0));
        memset(r, 8, 64); assert(seg_page_insert(pg, r, 64, &s1));
        memset(r, 9, 64); assert(seg_page_insert(pg, r, 64, &s2));
        assert(seg_page_delete(pg, s1) == 1);                 /* interior dead */
        u8 big[100]; memset(big, 4, sizeof big);
        assert(seg_page_update(pg, s0, big, 100) == 1);       /* garbage from grow */
        u32 before = seg_page_free_space(pg);
        assert(seg_page_compact(cp, pg) == 1);
        assert(seg_page_validate(cp) == 1);
        u32 after = seg_page_free_space(cp);
        assert(after > before);                               /* garbage reclaimed */
        u16 sz;
        assert(memcmp(seg_page_read(cp, s0, &sz), big, 100) == 0 && sz == 100);
        assert(seg_page_read(cp, s1, 0) == 0);                /* interior dead persists */
        const u8 *r2 = seg_page_read(cp, s2, &sz);
        assert(r2 && sz == 64 && r2[0] == 9);                 /* slot id stable */
        PASS();
    }

    TEST(compact_trims_trailing_dead_and_zeros_gap);
    {
        seg_page_init(pg, SEG_KIND_ADJ);
        u8 r[32]; memset(r, 5, 32);
        u16 s0, s1;
        assert(seg_page_insert(pg, r, 32, &s0));
        assert(seg_page_insert(pg, r, 32, &s1));
        assert(seg_page_delete(pg, s1) == 1);                 /* trailing dead */
        memset(cp, 0xEE, sizeof cp);                          /* stale heap bytes */
        assert(seg_page_compact(cp, pg) == 1);
        u16 sz;
        assert(seg_page_read(cp, s0, &sz) && sz == 32);
        assert(seg_page_read(cp, s1, 0) == 0);                /* trimmed: dead */
        /* whole free gap must be zero (no stale 0xEE reaches disk) */
        u32 slots_end = SEG_PAGE_HDR_SIZE + 1 * SEG_SLOT_SIZE;
        u32 floor = SEG_PAGE_SIZE - 32;
        for (u32 i = slots_end; i < floor; i++) assert(cp[i] == 0);
        PASS();
    }

    TEST(compact_refuses_corrupt_src);
    {
        seg_page_init(pg, SEG_KIND_ENTITY);
        u8 r[100]; memset(r, 1, sizeof r);
        u16 slot; assert(seg_page_insert(pg, r, 100, &slot));
        pg[2] = 0xFF; pg[3] = 0xFF;                           /* rec_floor smashed */
        assert(seg_page_validate(pg) == 0);
        assert(seg_page_compact(cp, pg) == 0);
        /* aliasing live records: two slots, same offset, sizes summing past capacity */
        seg_page_init(pg, SEG_KIND_ENTITY);
        u8 big[SEG_PAGE_MAX_REC]; memset(big, 2, sizeof big);
        assert(seg_page_insert(pg, big, (u16)(SEG_PAGE_MAX_REC - SEG_SLOT_SIZE), &slot));
        /* forge a second live slot aliasing the first record's bytes */
        u16 forged_off = (u16)(SEG_PAGE_SIZE - (SEG_PAGE_MAX_REC - SEG_SLOT_SIZE));
        pg[0] = 2; pg[1] = 0;                                 /* nslots = 2 */
        pg[SEG_PAGE_HDR_SIZE + 4] = (u8)forged_off;
        pg[SEG_PAGE_HDR_SIZE + 5] = (u8)(forged_off >> 8);
        pg[SEG_PAGE_HDR_SIZE + 6] = (u8)(SEG_PAGE_MAX_REC - SEG_SLOT_SIZE);
        pg[SEG_PAGE_HDR_SIZE + 7] = (u8)((SEG_PAGE_MAX_REC - SEG_SLOT_SIZE) >> 8);
        if (seg_page_validate(pg))                            /* per-slot bounds pass */
            assert(seg_page_compact(cp, pg) == 0);            /* sum check refuses */
        PASS();
    }

    TEST(fuzz_vs_model_20k_ops);
    {
        static model_t md;
        memset(&md, 0, sizeof md);
        seg_page_init(pg, SEG_KIND_ENTITY);
        rng_state = 0x5E657374ull;                             /* fixed seed */
        u32 inserts = 0, deletes = 0, updates = 0, compacts = 0, fulls = 0;
        for (int op = 0; op < 20000; op++) {
            u32 kind = (u32)(rng() % 100);
            if (kind < 45) {                                   /* insert */
                u16 size = (u16)(1 + rng() % 300);
                u8 rec[512];
                for (u16 i = 0; i < size; i++) rec[i] = (u8)rng();
                u16 slot;
                if (seg_page_insert(pg, rec, size, &slot)) {
                    assert(slot < MODEL_MAX && !md.live[slot]);
                    md.live[slot] = 1; md.size[slot] = size;
                    memcpy(md.bytes[slot], rec, size);
                    if (slot >= md.nslots) md.nslots = slot + 1u;
                    inserts++;
                } else fulls++;
            } else if (kind < 65) {                            /* delete random slot */
                u16 slot = (u16)(rng() % (md.nslots ? md.nslots : 1));
                int ok = seg_page_delete(pg, slot);
                assert(ok == (md.nslots && md.live[slot] ? 1 : 0));
                if (ok) { md.live[slot] = 0; deletes++; }
            } else if (kind < 85) {                            /* update random slot */
                u16 slot = (u16)(rng() % (md.nslots ? md.nslots : 1));
                u16 size = (u16)(1 + rng() % 300);
                u8 rec[512];
                for (u16 i = 0; i < size; i++) rec[i] = (u8)rng();
                int ok = seg_page_update(pg, slot, rec, size);
                if (!(md.nslots && md.live[slot])) assert(ok == 0);
                else if (ok) { md.size[slot] = size; memcpy(md.bytes[slot], rec, size); updates++; }
                /* ok==0 with live slot = legitimately out of space (grow path) */
            } else {                                           /* compact (the COW touch) */
                assert(seg_page_compact(cp, pg) == 1);
                memcpy(pg, cp, SEG_PAGE_SIZE);
                /* model: page may have trimmed trailing dead slots */
                while (md.nslots > 0 && !md.live[md.nslots - 1]) md.nslots--;
                compacts++;
            }
            model_check(&md, pg);
        }
        printf("(ins %u del %u upd %u cmp %u full %u) ", inserts, deletes, updates, compacts, fulls);
        assert(inserts > 1000 && deletes > 1000 && updates > 500 && compacts > 1000);
    }
    PASS();

    printf("test_segpage: %d tests passed\n", tests_run);
    return 0;
}
