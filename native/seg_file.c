/*
 * seg_file.c — one segment's lifecycle over a seg_io.
 *
 * The commit protocol lives here and ONLY here (spec r2 §3):
 *   extend -> write dirty pages -> SYNC -> write meta to inactive slot ->
 *   SYNC -> flip. The second sync is the commit point; in-memory state
 *   advances only after it succeeds, so an I/O error leaves the segfile
 *   still serving the previous commit.
 *
 * Recovery = seg_meta_pick over the two slots, then a watermark-fits-file
 * sanity gate (belt over the meta checksum's suspenders: a valid-looking
 * meta pointing past EOF loses to a valid meta that fits).
 */
#include "segstore.h"

#include <stdlib.h>
#include <string.h>

static u64 cluster_round_pages(const seg_meta_t *m, u64 pages) {
    u64 c = 1ull << m->extent_pages_log2;
    return (pages + c - 1) / c * c;
}

/* meta slot i lives at the head of page i */
static int write_meta_slot(seg_io_t *io, int slot, const seg_meta_t *m) {
    u8 page[SEG_PAGE_SIZE];
    memset(page, 0, sizeof page);
    memcpy(page, m, sizeof *m);
    return io->write(io, page, SEG_PAGE_SIZE, (u64)slot * SEG_PAGE_SIZE);
}

segfile_t *segfile_create(seg_io_t *io, u16 seg_id, u16 extent_pages_log2) {
    seg_meta_t m;
    seg_meta_init(&m, seg_id, extent_pages_log2);
    u64 bytes = cluster_round_pages(&m, SEG_META_PAGES) * SEG_PAGE_SIZE;
    if (!io->extend(io, bytes)) return NULL;
    if (!write_meta_slot(io, 0, &m)) return NULL;
    if (!write_meta_slot(io, 1, &m)) return NULL;
    if (!io->sync(io)) return NULL;

    segfile_t *sf = (segfile_t *)calloc(1, sizeof *sf);
    if (!sf) return NULL;
    sf->io = io; sf->meta = m; sf->active_slot = 0;
    return sf;
}

segfile_t *segfile_open(seg_io_t *io) {
    u64 size = 0;
    const u8 *base = io->read_base(io, &size);
    if (!base || size < SEG_META_PAGES * SEG_PAGE_SIZE) return NULL;
    u64 file_pages = size / SEG_PAGE_SIZE;

    seg_meta_t m0, m1;
    memcpy(&m0, base, sizeof m0);
    memcpy(&m1, base + SEG_PAGE_SIZE, sizeof m1);

    /* pick with the watermark-fits gate: disqualify a meta whose watermark
     * exceeds the file before picking, by treating it as invalid. */
    int v0 = seg_meta_valid(&m0) && m0.watermark <= file_pages;
    int v1 = seg_meta_valid(&m1) && m1.watermark <= file_pages;
    int slot;
    if (!v0 && !v1) return NULL;
    else if (v0 && !v1) slot = 0;
    else if (!v0 && v1) slot = 1;
    else slot = (m1.txid > m0.txid) ? 1 : 0;

    segfile_t *sf = (segfile_t *)calloc(1, sizeof *sf);
    if (!sf) return NULL;
    sf->io = io;
    sf->meta = slot ? m1 : m0;
    sf->active_slot = slot;
    return sf;
}

const u8 *segfile_page(segfile_t *sf, u32 pgno) {
    if (pgno < SEG_META_PAGES || pgno >= sf->meta.watermark) return NULL;
    u64 size = 0;
    const u8 *base = sf->io->read_base(sf->io, &size);
    if (!base || ((u64)pgno + 1) * SEG_PAGE_SIZE > size) return NULL;
    return base + (u64)pgno * SEG_PAGE_SIZE;
}

int segfile_commit(segfile_t *sf, const u32 *pgnos, const u8 *const *bufs,
                   u32 ndirty, const seg_meta_t *next_meta) {
    if (!next_meta) return 0;
    if (next_meta->txid != sf->meta.txid + 1) return 0;       /* txids are dense */
    if (next_meta->watermark < sf->meta.watermark) return 0;  /* never shrinks (v1) */
    for (u32 i = 0; i < ndirty; i++) {
        if (pgnos[i] < SEG_META_PAGES) return 0;              /* meta pages sacred */
        if (pgnos[i] >= next_meta->watermark) return 0;       /* page must be addressable */
    }

    /* 1. grow file if needed (extent-cluster-rounded) */
    u64 need = cluster_round_pages(next_meta, next_meta->watermark) * SEG_PAGE_SIZE;
    if (!sf->io->extend(sf->io, need)) return 0;

    /* 2. dirty pages */
    for (u32 i = 0; i < ndirty; i++)
        if (!sf->io->write(sf->io, bufs[i], SEG_PAGE_SIZE,
                           (u64)pgnos[i] * SEG_PAGE_SIZE)) return 0;

    /* 3. data barrier */
    if (!sf->io->sync(sf->io)) return 0;

    /* 4. sealed meta into the INACTIVE slot */
    seg_meta_t m = *next_meta;
    seg_meta_seal(&m);
    int inactive = 1 - sf->active_slot;
    if (!write_meta_slot(sf->io, inactive, &m)) return 0;

    /* 5. commit point */
    if (!sf->io->sync(sf->io)) return 0;

    /* 6. flip (only now does the segfile serve the new txid) */
    sf->meta = m;
    sf->active_slot = inactive;
    return 1;
}

void segfile_close(segfile_t *sf) {
    if (!sf) return;
    if (sf->io) sf->io->close(sf->io);
    free(sf);
}
