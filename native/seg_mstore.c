/*
 * seg_mstore.c — multi-segment store: manifest pivot over N segstores.
 *
 * The manifest append is the ONLY store-level commit point. Per-segment
 * commits (which are themselves atomic) become store-visible exactly when
 * the record naming them is durably appended. See segstore.h block comment
 * for the dual-meta selection argument.
 */
#include "segstore.h"

#include <stdlib.h>
#include <string.h>

struct mstore {
    seg_io_t    *mio;         /* manifest io (owned) */
    segstore_t **segs;        /* nsegs segstores (owned) */
    u64         *seg_txids;   /* per-seg txid as of last store commit */
    u32          nsegs;
    u64          store_txid;
    u64          m_end;       /* manifest append offset (logical end) */
    int          txn_open;
};

static u32 rec_bytes(u32 nsegs) { return 4 + 4 + 8 + nsegs * 16u + 4; }

static void wr32(u8 *p, u32 v) { p[0]=(u8)v; p[1]=(u8)(v>>8); p[2]=(u8)(v>>16); p[3]=(u8)(v>>24); }
static void wr64(u8 *p, u64 v) { wr32(p, (u32)v); wr32(p + 4, (u32)(v >> 32)); }
static u32  rd32(const u8 *p) { return (u32)p[0]|((u32)p[1]<<8)|((u32)p[2]<<16)|((u32)p[3]<<24); }
static u64  rd64(const u8 *p) { return (u64)rd32(p) | ((u64)rd32(p + 4) << 32); }

static u8 *build_record(const mstore_t *ms, u64 store_txid, const u64 *txids, u32 *len_out) {
    u32 len = rec_bytes(ms->nsegs);
    u8 *rec = (u8 *)malloc(len);
    if (!rec) return NULL;
    wr32(rec, MST_MAGIC);
    wr32(rec + 4, ms->nsegs);
    wr64(rec + 8, store_txid);
    for (u32 s = 0; s < ms->nsegs; s++) {
        wr32(rec + 16 + 16u * s, s);
        wr32(rec + 16 + 16u * s + 4, 0);
        wr64(rec + 16 + 16u * s + 8, txids[s]);
    }
    wr32(rec + len - 4, seg_crc32c(rec, len - 4));
    *len_out = len;
    return rec;
}

/* scan the manifest; fill *store_txid / txids[] from the LAST valid record.
 * returns the byte offset just past that record (the append point), or
 * (u64)-1 if no valid record exists. */
static u64 scan_manifest(seg_io_t *mio, u32 nsegs, u64 *store_txid, u64 *txids) {
    u64 size = 0;
    const u8 *base = mio->read_base(mio, &size);
    u32 want = rec_bytes(nsegs);
    u64 off = 0, good_end = (u64)-1;
    while (base && off + want <= size) {
        const u8 *r = base + off;
        if (rd32(r) != MST_MAGIC) break;
        if (rd32(r + 4) != nsegs) break;
        if (rd32(r + want - 4) != seg_crc32c(r, want - 4)) break;   /* torn tail */
        *store_txid = rd64(r + 8);
        for (u32 s = 0; s < nsegs; s++) {
            if (rd32(r + 16 + 16u * s) != s) return (u64)-1;        /* corrupt */
            txids[s] = rd64(r + 16 + 16u * s + 8);
        }
        off += want;
        good_end = off;
    }
    return good_end;
}

static int append_record(mstore_t *ms, u64 store_txid, const u64 *txids) {
    u32 len = 0;
    u8 *rec = build_record(ms, store_txid, txids, &len);
    if (!rec) return 0;
    if (!ms->mio->extend(ms->mio, ms->m_end + len)) { free(rec); return 0; }
    if (!ms->mio->write(ms->mio, rec, len, ms->m_end)) { free(rec); return 0; }
    free(rec);
    if (!ms->mio->sync(ms->mio)) return 0;      /* THE store commit point */
    ms->m_end += len;
    return 1;
}

static mstore_t *ms_new(seg_io_t *mio, u32 nsegs) {
    mstore_t *ms = (mstore_t *)calloc(1, sizeof *ms);
    if (!ms) return NULL;
    ms->mio = mio;
    ms->nsegs = nsegs;
    ms->segs = (segstore_t **)calloc(nsegs, sizeof *ms->segs);
    ms->seg_txids = (u64 *)calloc(nsegs, sizeof *ms->seg_txids);
    if (!ms->segs || !ms->seg_txids) { free(ms->segs); free(ms->seg_txids); free(ms); return NULL; }
    return ms;
}

mstore_t *mstore_create(seg_io_t *manifest_io, seg_io_t **seg_ios, u32 nsegs,
                        u16 extent_pages_log2) {
    if (nsegs == 0 || nsegs > 0xFFFF) return NULL;
    mstore_t *ms = ms_new(manifest_io, nsegs);
    if (!ms) return NULL;
    for (u32 s = 0; s < nsegs; s++) {
        ms->segs[s] = segstore_create(seg_ios[s], (u16)s, extent_pages_log2);
        if (!ms->segs[s]) {
            seg_ios[s]->close(seg_ios[s]);                 /* create failed: io not consumed */
            for (u32 k = s + 1; k < nsegs; k++) seg_ios[k]->close(seg_ios[k]);
            mstore_close(ms);
            return NULL;
        }
        ms->seg_txids[s] = 0;
    }
    ms->store_txid = 0;
    ms->m_end = 0;
    if (!append_record(ms, 0, ms->seg_txids)) { mstore_close(ms); return NULL; }
    return ms;
}

mstore_t *mstore_open(seg_io_t *manifest_io, seg_io_t **seg_ios, u32 nsegs) {
    if (nsegs == 0) return NULL;
    mstore_t *ms = ms_new(manifest_io, nsegs);
    if (!ms) return NULL;
    u64 end = scan_manifest(manifest_io, nsegs, &ms->store_txid, ms->seg_txids);
    if (end == (u64)-1) {
        for (u32 s = 0; s < nsegs; s++) seg_ios[s]->close(seg_ios[s]);
        mstore_close(ms);
        return NULL;
    }
    ms->m_end = end;
    for (u32 s = 0; s < nsegs; s++) {
        ms->segs[s] = segstore_open_at(seg_ios[s], ms->seg_txids[s]);
        if (!ms->segs[s]) {           /* open_at consumed seg_ios[s] */
            for (u32 k = s + 1; k < nsegs; k++) seg_ios[k]->close(seg_ios[k]);
            mstore_close(ms);
            return NULL;
        }
    }
    return ms;
}

void mstore_close(mstore_t *ms) {
    if (!ms) return;
    for (u32 s = 0; s < ms->nsegs; s++)
        if (ms->segs[s]) segstore_close(ms->segs[s]);   /* closes seg io */
    free(ms->segs); free(ms->seg_txids);
    if (ms->mio) ms->mio->close(ms->mio);
    free(ms);
}

u64 mstore_txid(const mstore_t *ms)  { return ms->store_txid; }
u32 mstore_nsegs(const mstore_t *ms) { return ms->nsegs; }
segstore_t *mstore_seg(mstore_t *ms, u32 seg_id) {
    return seg_id < ms->nsegs ? ms->segs[seg_id] : NULL;
}

int mstore_txn_begin(mstore_t *ms) {
    if (ms->txn_open) return 0;
    for (u32 s = 0; s < ms->nsegs; s++)
        if (!seg_txn_begin(ms->segs[s])) {
            for (u32 k = 0; k < s; k++) seg_txn_abort(ms->segs[k]);
            return 0;
        }
    ms->txn_open = 1;
    return 1;
}

void mstore_txn_abort(mstore_t *ms) {
    if (!ms->txn_open) return;
    for (u32 s = 0; s < ms->nsegs; s++) seg_txn_abort(ms->segs[s]);
    ms->txn_open = 0;
}

int mstore_txn_commit(mstore_t *ms) {
    if (!ms->txn_open) return 0;
    u64 next = ms->store_txid + 1;
    u64 *txids = (u64 *)malloc((size_t)ms->nsegs * 8);
    if (!txids) { mstore_txn_abort(ms); return 0; }
    memcpy(txids, ms->seg_txids, (size_t)ms->nsegs * 8);

    int any = 0;
    for (u32 s = 0; s < ms->nsegs; s++) {
        /* seg_txn_commit_as is a no-op-commit (txid unchanged) when the
         * segment has nothing staged; only dirty segments advance. */
        u64 before = segstore_txid(ms->segs[s]);
        if (!seg_txn_commit_as(ms->segs[s], next)) {
            /* segment commit failed: abort the rest; segments already
             * committed at `next` are orphaned and will be rolled back by
             * recovery (manifest still names the old txids). */
            for (u32 k = s + 1; k < ms->nsegs; k++) seg_txn_abort(ms->segs[k]);
            ms->txn_open = 0;
            free(txids);
            return 0;
        }
        if (segstore_txid(ms->segs[s]) != before) { txids[s] = next; any = 1; }
    }
    ms->txn_open = 0;
    if (!any) { free(txids); return 1; }          /* store-wide no-op */

    if (!append_record(ms, next, txids)) {
        /* pivot failed: on-disk manifest still names the old state; the
         * in-memory mstore is now stale — the embedder must reopen. */
        free(txids);
        return 0;
    }
    memcpy(ms->seg_txids, txids, (size_t)ms->nsegs * 8);
    ms->store_txid = next;
    free(txids);
    return 1;
}
