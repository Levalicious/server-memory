# Graph Store v4 — networked single-writer, segmented, page-COW (libmdbx-imitation)

Status: r3 — **FORMAT FREEZE**. Sections stamped FROZEN are format commitments:
changing them henceforth means a migration, not an edit. Unstamped sections
remain design-current but revisable.

r3 (2026-08-23) supersedes r2. Deltas, with KB anchors:

- **Formats FROZEN** (§2, §5): every on-disk structure is implemented, crash-
  harness-verified, and where marked, machine-proved. Evidence inline.
- **Q1 CLOSED**: 4K pages, by measurement on the agreed 500/5K/50K/500K
  entity ladder (`Finding_SegstoreScaleLadder_2026_08_23`) — 2.8–3.4× less
  write volume than 16K, no losing axis.
- **Identity model added** (§2.3): logical pgnos + COW'd shadow page table
  (`Decision_LogicalPgnoShadowTable_2026_08_23`) — the one deliberate
  libmdbx deviation, forced by graph in-refs (r2 Q3 closed and extended).
- **Q4 CLOSED**: RO-mmap reads + pwrite/fdatasync writes — libmdbx's own
  default mode (`Insight_ROMmapPwriteIsLibmdbxDefault_2026_08_20`); no
  writable mmap and no msync exist in the codebase.
- **Q5 CLOSED**: manifest record fsync'd on every store commit; recovery by
  dual-meta *selection* (`Insight_DualMetaSelectionProperty_2026_08_23`).
- **Protocol gains the traversal algebra** (§6.2): TRAVERSE/RESUME with
  declared combine-semantics; partial traversals are first-class
  (`Design_PartialTraversalProtocol_2026_08_23`, proposed by Lev).
- **Depth semantics canonicalized** (§6.3): 0-indexed everywhere public
  (`Bug_DepthDefaultMismatch_2026_08_23` → `Fix_DepthDefault_2026_08_23`).
- Phase 2 (libsegstore) is **DONE** (§10).

## 0. Why (epistemic chain — unchanged from r2, abridged)

Production hang: msync-under-flock on a single-owner memfile bolted into
N-process sharing (`Diag_MemfileSingleOwner_2026_06_29`,
`Insight_MsyncUnderLockAntipattern_2026_07_22`). Plus 8+ months of
multi-machine stdio use with hand-carried KB files. One owner daemon on one
host dissolves both. Requirements: ACID, batched ingest >1M rec/s, graph-
native read path, sharding designed-for before networking is built.

## 1. Architecture

One **owner daemon** per store, on the store's host. Clients are thin
stdio-MCP shims speaking a binary protocol over TCP+token (LAN). Nobody but
the owner opens a store file. One concurrency mechanism: the owner's queue.

    ┌─ machine A ─┐   ┌─ machine B ─┐
    │ claude⇄shim │   │ claude⇄shim │      (stdio MCP, unchanged surface)
    └──────┬──────┘   └──────┬──────┘
           └───── TCP+token ──┴────► owner daemon ──► store files (one host)

    store/
      MANIFEST            # store-level commit pivot (append-only, fsync'd)
      seg-NNNN.kb         # segment: self-contained paged store (§2)
      strings-NN.kb       # string segments (same mechanics)

**Segment** ≠ shard: one machine, one writer, one tx domain. Segments are
the physical layout, COW granularity, *and future shard seam* (§7).

## 2. Segment format — FROZEN

Implemented in `native/segstore.h` / `seg_meta.c` / `seg_page.c` /
`seg_file.c` / `seg_txn.c`. The header is the normative reference; this
section fixes the commitments.

### 2.1 Constants and refs — FROZEN

- `SEG_PAGE_SIZE = 4096`. Closed by `bench_segstore` on the 500/5K/50K/500K
  ladder: bytes/commit 2.8–3.4× better than 16K, touch 3× cheaper, recover
  2.4× faster @500K; 16K's sole theoretical edge (co-location) measures
  negligible (E[distinct pages | K=16] 15.9 vs 15.5). 8K was re-evaluated
  2026-10-08 for longer records and REJECTED: ~1.6× bytes/commit & amp
  @500K for +2× record capacity. Record-cap note: st4 caps records at
  `SEG_PAGE_MAX_REC-4` (4072 B); entity names beyond that are a recorded
  contract delta vs v3 (E2E long-name case = expected-fail under the daemon
  backend until multi-page records land). Page = COW unit; **extent**
  (`extent_pages_log2`, per-segment) = allocation/locality unit.
- Packed ref: `(u16 seg | u32 pgno | u16 slot)` in a u64. pgno is LOGICAL
  (§2.3). 16TB/segment address space; byte-clean fields.
- Byte order: little-endian.

### 2.2 Dual meta — FROZEN

`seg_meta_t` at pgno 0 and 1, alternating; crc32c over all-but-checksum
(the only checksummed structure — data pages rely on commit ordering, the
libmdbx/LMDB discipline). Commit toggles slots; recovery picks valid-max-
txid (or manifest-directed exact txid, §5). Fields include graph roots
(logical), `ptable_root_pgno` + `freelist_root_pgno` (physical),
`watermark` (physical pages), `logical_pages`.

Evidence: WP+RTE 119/121 (2 = the declared crc trust boundary); exhaustive
single-bitflip (0 survivors) and torn-write-every-prefix tests.

### 2.3 Identity model: logical pgnos + shadow page table — FROZEN

`Decision_LogicalPgnoShadowTable_2026_08_23` — the deliberate libmdbx
deviation. Relocating COW (libmdbx pgno = physical) cascades through
arbitrary graph in-refs; a B-tree has one parent, a graph node has any
number of referrers. Therefore refs carry **stable logical pgnos** and a
COW'd page table maps them:

    meta.ptable_root_pgno → root page [ntpages][tpage_phys…]
                          → table pages [1024 × u32 phys entries, PT_NONE]

Touch never relocates a logical page; commit re-points its table entry at a
fresh physical page. Table pages and root are COW'd like data. Cost: ~15
table pages at 500K entities; +1–10 pages per commit of table dirt.

### 2.4 Slotted page — FROZEN

General slotted format for every record kind
(`Decision_SlottedFormatTypeAffinity_2026_08_20`): 16B header (nslots,
rec_floor, kind_hint, flags), 4B slots (offset,size; offset 0 = dead)
growing up, records growing down. `kind_hint` is placement POLICY, never
semantics. COW-touch always copy-compacts: slot ids stable, free gap
zeroed (no stale heap bytes reach disk), refuses corrupt input. Records
>page use extent-descriptor kind (multi-page runs).

Evidence: WP+RTE **367/367, zero assumed obligations**; 20K-op
fuzz-vs-shadow-model with byte equality after every op; parse-don't-trust
header guards (a corrupt disk page cannot drive OOB — found by WP, invisible
to fuzz).

### 2.5 Freelist — FROZEN

Retired physical pages persist as a snapshot chain (fixed-format pages:
next, nwords, word stream = free[], pending[(txid, pgnos)]), rewritten each
commit, meta-rooted. Reuse gated: a page retired at txid T is allocatable
iff every live pin has txid ≥ T; restart clears pins. **The freelist feeds
itself** (chain pages allocate from free[] pop-then-serialize — the
watermark-only variant leaked +1 pg/commit under churn;
`Finding_FreelistMustFeedItself_2026_08_23`). Structurally acyclic:
append-only records in COW pages, never links through freed space.

## 3. Transactions and durability

- Write path: `seg_txn_begin / touch (COW+compact into heap) / alloc /
  free / commit`. COW discipline is **enforced here**: physical targets
  come only from gated freelist or watermark growth — never a page
  reachable from the last committed meta.
- Segment commit protocol — FROZEN ordering:
  1. extend (cluster-rounded)  2. write dirty data+ptable+freelist pages
  3. **sync** (data barrier)   4. sealed meta → inactive slot
  5. **sync** (commit point)   6. flip in memory (only now)
- Durability modes: `DURABLE` (as above) and `SAFE_NOSYNC` (fsync on
  cadence; loses the tail as a unit, never tears — the default; the KB's
  unit-of-loss is a melt batch).
- Relaxed class: walker/structural counters and ψ accumulate in owner
  memory, piggyback on real commits, never the sole cause of a COW.
- Snapshot pins: `{txid, ptable copy}`; pinned reads are byte-stable
  (verified through 20-commit churn on the pinned page).

Evidence: crash harness (prefix replay over recorded write/sync/extend
events = every legal in-order crash state): segfile 67 prefix + 126
torn-write states; txn layer 132 states over real COW txns with recycling.
Property: recovered txid ∈ {last_acked, last_acked+1}
(`Insight_RecoveredTxidPlusOneLegal_2026_08_23` — a fully-landed unacked
commit may legally win) with byte-exact state for whichever recovered.

## 4. Reads

All reads through the owner (remote machines make local-mmap clients moot).
RO `MAP_SHARED` mapping + pwrite coherence via the page cache; the only
durability barrier is fdatasync on the commit path, held under nothing.
Leases (owner-internal, bounded) pin snapshots for cursors; expired cursors
re-resolve and say so. (r3.1, implemented: continuation leases — a bounded
TTL/cap table of opaque tokens with O(1) replay-descriptor payloads; RESUME
validates the store txid and reports TOKEN_STALE/TOKEN_EXPIRED explicitly,
so the client re-anchors from the farthest it already holds. Snapshot pins
(`seg_pin`) remain the phase-B step for daemon-side cursors.)

Measured (rdtsc p50, ladder N=500K, 4K): read 88 cyc, pin_read 85,
touch 4.4K (memcpy-bound), alloc 265, pin+unpin 3.7K (ptable memcpy →
lease-per-cursor, not per-op), commit(K=16 melt) 69K ≈ 27µs, recover 30K ≈
11.5µs. Commit cost is scale-flat 5K→500K.

## 5. mstore: manifest + cross-segment atomicity — FROZEN

Record: `[MST_MAGIC][nsegs][store_txid][(seg_id, pad, seg_txid)×nsegs][crc]`,
append-only, fsync'd on **every** store commit (r2-Q5 closed conservatively;
folding is a later optimization). Scan-forward recovery, torn tail ignored.

**Dual-meta selection property** (`Insight_DualMetaSelectionProperty`): a
store commit advances each dirty segment by exactly one meta toggle, so
both states the store could want are always on disk; the manifest *names*
which one each segment presents (`segfile_open_at` exact-txid selection).
Rollback of a ran-ahead segment is a selection, not a repair; its orphaned
slot is overwritten by the next commit. Seg txids are store txids (sparse
per segment); segfile's txid gate is strictly-forward.

Evidence: 184 global crash points (one event clock across manifest + 3
segment files) — recovery always lands on ONE consistent store txid ∈
{acked, acked+1}, zero mixed states, byte-exact per segment.

Free consequence: a **consistent cross-segment (later: cross-shard)
snapshot is just a manifest record** — pin the store txid, each segment
pins its named seg txid.

## 6. Network protocol

### 6.1 Framing and auth

Length-prefixed binary frames, request-id multiplexed, version byte first;
payload encoding may evolve, the frame header is frozen at daemon v1.
Static bearer token in the handshake (0600 secret file); LAN boundary; TLS
out of scope until the deployment leaves the LAN. Write API is batch-shaped
end-to-end: one client batch = one store txn = one group commit.

### 6.2 Traversal algebra: TRAVERSE / RESUME
(`Design_PartialTraversalProtocol_2026_08_23`)

Partial traversals are first-class protocol objects, not an optimization:

    TRAVERSE { seeds: [(ref, alg_state)], spec: {dir, filter, budget},
               semantics }
    → { partial_results, continuations: [(ref, alg_state, residual_budget)],
        exhausted }
    RESUME { token }        // token = { store_txid, continuations[] }

    Law: resume(continuations) ⊕ partial_results ≡ full traversal,
         ⊕ = the declared combine algebra.

Semantics classes (normative):

| class     | algebra                | ops                          | distribution behavior |
|-----------|------------------------|------------------------------|-----------------------|
| SET       | union (comm., idem.)   | neighbors(d), reachability, orphans | shard-local schedule freedom (BFS/DFS/async); idempotent retry |
| MIN_DIST  | min-combine (monotone) | BFS distances, shortest path | async over-exploration legal (delta-stepping); label correction |
| ANY_PATH  | first-witness          | find_path                    | bidir seeds; first meet wins |
| SERIAL    | none (chain)           | random_walk, DFS *ordering*  | continuation migrates to owning shard; exactly-once token |

- Continuation tokens carry the **store txid**: a resumed traversal — across
  requests, shards, minutes — runs against one consistent snapshot (§5).
- `alg_state` cost: SET none, MIN_DIST u32, RPQ = NFA state mask (≤64-state
  masks per the regex determinizer) — RPQ distributes with zero extra
  protocol machinery (`BGS_RPQ` closed over).
- Today's MCP ops are thin presets over TRAVERSE; pagination cursors are
  continuations that never crossed a shard. `graph_find_path_ex`'s
  {budget_exhausted, farthest} was this shape before it had a name.
- v4.0 single-host: continuations surface only on budget exhaustion.
  Sharding changes *who resumes*, never what a continuation is.
- (r3.1: implemented — OP_FIND_PATH returns a u64 continuation lease id
  exactly on budget exhaustion; OP_RESUME (0x2e) continues it.)

### 6.3 Depth semantics — canonical

Public numbering is **0-indexed**: depth 0 = immediate neighbors (op_bench
"d0", MCP schema). C-layer hop-count (1 = immediate) is internal; exactly
one +1 translation lives at the boundary, pinned by an omitted-depth test
(`Fix_DepthDefault_2026_08_23`; the handler-default divergence returned
2-hop sets against a schema promising immediate — three numberings may
never again meet an untested default).

### 6.4 The shard seam
(`Design_ShardSeamFirst_2026_08_20`, `Design_ShardOpClasses_2026_08_23`)

- Refs segment-qualified on the wire; node identity = logical node id via
  the per-segment indirection table (rebalance mechanism + stable external
  id + future cross-shard forwarding point, in one structure).
- (r3.2, 2026-10-09: the logical node-id indirection table and the wire
  shard-id space are **EXPLICITLY DEFERRED**
  (Decision_Lev_ReconciliationRulings_2026_10_09). The retained seam arms:
  segments as the transaction/placement domain, segment-qualified refs, and
  the reserved SEG_KIND_INDIRECT page kind as the named future hook.)
- Op classes under sharding: **split-and-route** (search, by-type, scans,
  point reads — per-shard indexes, scatter-gather, ≥linear) vs
  **sequentially-bound** (traversals — placement hostage; cross-host edge
  ≈ 3 orders over the 175-cyc local d0 op). Community placement (§8) keeps
  crossings rare; TRAVERSE continuations grouped per shard per round keep
  them batched; bidir BFS halves the rounds.
- Writes: community-clustered melts → mostly single-shard txns (zero
  coordination); cross-shard txns ride §5 unchanged (the selection argument
  is host-count-agnostic; only the manifest's home host is new engineering).
- Open under sharding: ψ/MERW (per-shard iteration + boundary exchange is
  an argument, not yet a measurement).

## 7. Rank maintenance (amortized, background — unchanged from r2)

MC structural rank in idle slices (error ∝ 1/√π: important nodes converge
first); ψ warm-start power iteration on edit watermarks; all rank writes via
the relaxed counter class. Never on an op path (`Design_RankAmortized`).

## 8. Locality (mitigation, not solution — unchanged)

Community streaming placement (Fennel/LDG), bounded KL/FM swaps riding
existing commit dirt, hubs accepted as cross-segment, full re-cut last
resort (`Design_CommunityPageLocality_2026_07_01`).

## 9. What this deletes (all verified deleted in the v4 stack)

flocks + lock ordering; refresh/remap; migrate.lock; msync (does not exist);
per-client processes on one live file; manual KB file transfer; the WAL
(never written); segment-generation file churn (COW is intra-file).

## 10. Delivery

1. ~~Spec freeze~~ — **this document (r3)**.
2. ~~libsegstore~~ — **DONE**: seg_meta / seg_page / seg_io / seg_file /
   seg_txn / seg_mstore + crash harnesses + WP + ladder bench.
   `feat/libsegstore` @ `c6e4704`. Residual: WP pass over seg_txn/mstore
   arithmetic helpers (quality, not blocking).
3. **Owner daemon** ← next: TCP+token framing, TRAVERSE/RESUME +
   batch-write verbs, group commit over mstore, leases, counter relaxation,
   background rank; graph layer (graph.c + minimal-core indexes: trigram,
   type, bidir BFS) ported onto seg_txn refs.
4. **Client cutover**: server.ts → thin shim; one-shot v3 import; all
   machines point at the daemon. Ends KB file transfers.
5. **Locality + rank cadence** from measurement.

## 11. Non-goals (unchanged)

Multi-writer; sharding implementation (the seam is in, the second host is
not); shared reader tables; LSM; in-place mutation of published pages;
TLS/WAN.
