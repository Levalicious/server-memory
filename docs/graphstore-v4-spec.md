# Graph Store v4 — networked single-writer, segmented, page-COW (libmdbx-imitation)

Status: DRAFT r2 for iteration. Nothing here is frozen until marked FROZEN.

r2 (2026-08-20) supersedes r1 (2026-07-01). Deltas, with KB anchors:

- **Externalized.** The owner is a network daemon (TCP + token on LAN), not a
  local-shm peer. Multi-machine access is now a *goal*, not a non-goal
  (`Goal_KBExternalization_2026_08_20`, `Decision_ExternalizeViaCDaemon_2026_08_20`).
- **COW unit is the page, not the segment.** Each segment is internally a
  libmdbx-imitation paged store (`Decision_LibmdbxTargetArch_2026_08_20`,
  `Decision_PagedStorePerSegment_2026_08_20`). r1 §1.1's "memfile unchanged"
  premise is retired — segment-granularity copy was write amplification of
  exactly the kind this spec exists to kill.
- **No WAL, ever.** The scale-up roadmap's stage-2 WAL is deleted
  (`Design_DBScaleUp_Roadmap_2026_07_20` is SETTLED by the libmdbx target):
  commit = COW'd pages + meta toggle; throughput comes from sync-mode knobs,
  not log machinery.
- **Minimal-core is the baseline store**, not v3-as-it-shipped: schema v3
  node-log elimination, decoupled in-memory indexes with O(delta) catch-up,
  own regex engine + trigram prefilter + DFA, bidirectional BFS
  (`Finding_NodeLogEliminated_2026_07_13`, `Finding_AllOpsFinal_2026_07_20`,
  branch `bench/regex-minimalcore`).
- **Rank is a background concern**, permanently off the op paths
  (`Plan_V3MinimalCoreBench_2026_07_13`); re-added amortized in the owner (§5).
- **Shard seam designed now, implemented never (yet)**
  (`Design_ShardSeamFirst_2026_08_20`).

## 0. Why (epistemic chain)

Production hang: `create_entities` times out at 300s, self-recovers, silent
write loss. Diagnosis (KB: `Diag_MemfileSingleOwner_2026_06_29`):

- memfile is a **single-owner** construct; commit `13937aa` bolted N-process
  shared-mmap + flock onto it ("multi-instance safety") — off-design.
- The hang is a lock holder stalled in blocking `msync(MS_SYNC)` under I/O
  saturation, holding the flock; peers time out
  (`Insight_MsyncUnderLockAntipattern_2026_07_22`: *any* lock held across a
  durability barrier serializes all writers — the flavor of lock is
  irrelevant).
- Root problem: four half concurrency systems (2 flocks + migrate.lock +
  refresh/remap + msync-under-lock), none providing crash-atomicity.

New forcing function (2026-08-20): 8+ months of multi-machine work over
stdio MCP, with the KB file manually shepherded between hosts. The store
must live on exactly one host and be reachable from all of them. This
dissolves the multi-instance problem rather than solving it: one daemon
*is* the single writer.

Requirements (Lev): do it properly. ACID semantics. Batched ingest at
millions of nodes/sec (already demonstrated on minimal-core: all write ops
>1M TPS, `Finding_AllOpsFinal_2026_07_20`). Graph-native read path (traversal
is the hot path — no LSM merge tax, no KV hop tax). Design for sharding
ahead of implementing networking — "just an upgrade" must never mean
"rewrite the codebase".

## 1. Architecture

One **owner daemon** per store, on the store's host. Clients are thin
stdio-MCP shims (server.ts reduced to framing + pagination + formatting),
one per machine, speaking a binary protocol over TCP. Nobody but the owner
ever opens a store file. There is exactly one concurrency mechanism in the
system: the owner's request queue.

    ┌─ machine A ─┐   ┌─ machine B ─┐
    │ claude⇄shim │   │ claude⇄shim │      (stdio MCP, unchanged surface)
    └──────┬──────┘   └──────┬──────┘
           └───── TCP+token ──┴────► owner daemon ──► store files (one host)

Storage is **N segment files + 1 manifest**:

    store/
      MANIFEST            # cross-segment commit pivot (tiny, append-only, fsync'd)
      seg-0003.kb         # segment 3: self-contained paged store (§2)
      seg-0007.kb
      strings-00.kb       # string segments (same mechanics, §7.Q2)

Terminology: **segment**, deliberately not "shard" — one machine, one writer,
one transaction domain. Segments are a physical layout + COW granularity +
*future placement* choice, not (yet) a distribution boundary. Cross-segment
edges are cheap (a deref into another mapping), never a cross-domain
operation.

## 2. Segment = libmdbx-imitation paged store

Imitation, not linkage (`D_NoExternalDependence`; the mandate was never
"avoid the architecture", it was "own the code"). Each segment file:

- **Page-granularity COW.** Fixed page size (§7.Q1). A write tx never
  modifies a live page: it copies the page, mutates the copy, and the new
  page becomes reachable only at commit.
- **Dual meta pages** at fixed offsets 0 and 1, alternating. A meta page
  holds: txid, root refs (name-index root, adjacency heap root, freelist
  root), page-count watermark, checksum. Commit toggles to the *other* meta
  slot; recovery picks the valid meta with the highest txid. A torn meta
  write loses nothing — the other slot is the previous commit.
- **Freelist as first-class data.** Pages retired by COW (the old versions)
  are recorded in a freelist tree, itself COW'd, keyed by the txid that
  retired them. A retired page is reusable once no live snapshot (§4) can
  reference it. This is the libmdbx GC discipline: space is reclaimed by
  *reuse*, not compaction; the file grows only when the reclaimable set is
  empty.
- **No WAL.** Crash-atomicity is the meta toggle. Durability is the fsync
  policy (§3). Recovery is O(1): read two metas, pick one. No replay.

### 2.1 What lives inside the pages

The minimal-core v3 *content* layout carries over as the intra-page record
format — this is layout knowledge, not code reuse:

- EntityRecord 76B (`[u32 version][72B body]`, biscuit-style versioned
  records — the version word is how records migrate schema in place).
- AdjEntry 24B, bidirectional storage, `target<<2|dir` packing.
- Name index (open-addressing, name_id → ref) as the **sole entity
  registry** (schema v3; node log stays dead —
  `Decision_EliminateNodeLog_2026_07_13`). Enumeration is bucket order.
- Refcount discipline against the string store unchanged: adj entry owns
  one ref on relType_id; entity owns refs on name_id, type_id, obs ids.

What does NOT carry over: the memfile cartesian-tree arena and all in-place
mutation of published bytes. Allocation becomes page-local (records packed
into pages; a page's free space is its own concern). The v3 sin — in-place
mutation of shared state — is structurally impossible, not policed.

### 2.2 In-memory index layer (owner-private, per-segment)

Proven on `bench/regex-minimalcore`; all owner-private, none persisted,
all rebuilt lazily and maintained incrementally:

- **Trigram prefilter** over name+type+obs, decoupled from the write path:
  writes do an O(1) dirty-set mark (offset → op, last-wins); `index_sync`
  applies O(delta) catch-up at search time
  (`Finding_WriteDecoupled_2026_07_20`, `Finding_IncrementalSync_2026_07_20`:
  K=1 catch-up 1.2µs vs 267ms rebuild @200K). Search = trigram prefilter →
  own-engine DFA/NFA verify; 132–157× glibc ERE at 2K, 4403× selective at
  500K (`Finding_BenchCorrectBaseline_2026_07_20`).
- **Type index** (type_id → offset postings), lazy, O(1) maintained,
  entities_by_type O(result): 340× at 500K (`Finding_TypeIndex_2026_07_20`).
- **Traversal fast paths**: depth-1 immediate-neighbor path (4.84×,
  `Finding_ImmediateNeighborFastPath_2026_07_20`); bidirectional level-sync
  BFS for find_path (22.7×, `Finding_BidirectionalBFS_2026_07_20`).

The single-writer owner is what makes these *sound*: exactly one process
mutates, so in-memory indexes cannot go stale under it
(`Insight_TrigramIsSingleWriterFeature_2026_07_14` — now ungated).

## 3. Transactions, durability, ACID

- **Write path**: client batches arrive at the owner; the owner coalesces
  them into a tx (group commit is *the default shape*, not an optimization:
  one batch = one tx = one meta toggle per dirty segment + at most one
  manifest record).
- **Commit protocol**:
  1. write COW'd data pages + freelist pages of every dirty segment
  2. per dirty segment: fsync (policy-dependent, below), toggle meta
  3. if >1 segment dirty: append manifest record
     `{txid, [(seg → meta_txid)...], checksum}`, fsync — the record is the
     cross-segment pivot. Single-segment txs skip the manifest entirely;
     the segment meta *is* the commit point (§7.Q5 folds manifest state).
  4. retired pages enter per-segment freelists, tagged with txid
- **Durability modes** (libmdbx-imitation, per-store config):
  - `DURABLE`: fsync data + meta every commit. The pre-crash txid is the
    recovered txid.
  - `SAFE_NOSYNC`: fsync on a cadence (ops/bytes/ms watermark), meta toggle
    ordered after data writes (`fdatasync` barrier only at checkpoint). A
    crash loses the tail *as a unit* — recovers to the last checkpointed
    txid, never a torn state. This is the default: the KB's durability
    unit-of-loss is a melt batch, and the walker already tolerates replay.
- **ACID**: A = meta toggle / manifest record, all-or-nothing. C = single
  writer validates before publish. I = snapshots (§4); single writer ⇒
  serializable. D = mode above, *chosen*, not accidental.
- **Relaxed-durability class (unchanged from r1)**: walker/structural visit
  counters and ψ accumulate in owner memory, flush piggybacked on real
  commits. Counter loss = rank statistics loss, explicitly not graph truth.
  Under page-COW this matters *more*: a counter bump must never be the sole
  reason a page is COW'd.

## 4. Reads and snapshots

- All reads go through the owner (v4.0; there is no local-mmap client
  anymore — machines are remote). A read request executes against **the
  live root under the owner's tx mutex-free read view**: single writer +
  COW means a reader that captured root refs at txid T sees frozen pages
  for as long as those refs are pinned.
- **Snapshot pin = owner-internal lease** on {segment metas, manifest txid}.
  Leases pin freelist reclamation (a retired page outlives every lease that
  can reach it). Leases are bounded (timeout) so a stuck client cannot
  wedge reclamation — libmdbx's long-reader problem, solved by fiat: the
  owner *owns* the leases and expires them.
- Paginated MCP ops (cursors) hold a lease across the cursor's lifetime,
  bounded; an expired cursor re-resolves against the newest snapshot
  (documented, observable via txid in the cursor token).
- v4.1 (option, unchanged): same-host direct read-only mmap under a lease.
  Only if owner-mediated read throughput measurably bottlenecks — at 14.9M
  TPS immediate-neighbor reads (`Finding_AllOpsFinal_2026_07_20`), the
  network is the bottleneck long before the owner is.

## 5. Rank maintenance (amortized, background)

r1 had rank in the op paths (resample-on-write, ψ power iteration inline).
Minimal-core ripped it out and the ops got their >1M TPS. It comes back as
an **owner background job**, never on an op path:

- **Visit counters**: already relaxed (§3). `pagerank`/`llmrank` sort keys
  read whatever the counters say now — they are statistics, not invariants.
- **MC structural rank**: `graph_structural_sample` (exists in C) run in
  idle slices. Monte-Carlo error ∝ 1/√π (`PR_ErrorInverselyProportionalToRank`)
  — important nodes converge first, so partial work is immediately useful,
  which is exactly the amortization property we want.
- **MERW ψ**: incremental recompute. Power iteration restarted from the
  *previous* ψ after a batch of graph edits converges in few iterations
  (warm start; eigengap does the work). Trigger: dirty-edge count watermark
  or idle timer, not per-op. Sweeps enumerate via the name index — the node
  log stays dead; if sweep locality ever measurably hurts, that is a §7
  question, not a resurrection.
- Scheduling: strictly idle/background-priority in the owner; a rank job
  yields to any incoming op. Rank writes go through the counter relaxation
  path (in-memory, piggyback flush), so background rank NEVER causes page
  COW on its own.

## 6. Network protocol and the shard seam

- **Framing**: length-prefixed binary, request-id multiplexed (concurrent
  in-flight requests per connection), version byte first. CBOR-ish
  self-describing payloads are acceptable for v4.0 (op rate is human-scale);
  the frame header is what's frozen, payload encoding can evolve.
- **Auth**: static bearer token in the connection handshake (per-store
  secret file, `0600`, same token on all clients). LAN deployment. TLS
  explicitly out of scope for v4.0 (LAN trust boundary — revisit only if
  the deployment leaves the LAN).
- **Batching**: the write API is batch-shaped end-to-end (MCP tools already
  are: create_entities[], create_relations[]). One client batch = one tx
  request = one group commit. No autocommit-per-record anywhere.
- **The seam** (`Design_ShardSeamFirst_2026_08_20`): every ref on the wire
  and in adjacency is segment-qualified: packed u64 `(u16 seg | u48 off)`.
  Node identity for external callers is the **logical node id** (r1 Q3
  resolved: option (b)), mapped via a per-segment indirection table
  (node_id → packed ref). The indirection table is simultaneously:
  1. the rebalance mechanism (move node = update one table entry),
  2. the stable external id for the MCP layer and walker counters,
  3. the future cross-shard forwarding point (a table entry that says
     "segment 12, which lives on host X" is routing, and nothing about
     the record format changes).
  Sharding-the-implementation = "some segments are served by another owner
  behind the same protocol". The protocol, refs, and indirection are built
  so that sentence is *only* about the owner, never about the store format.
- Cost accepted: one indirection per hop entry into a node. Adjacency
  stores packed refs (fast path); the indirection is consulted at the MCP
  boundary and on rebalance-forwarding, not per traversal step (refs are
  repaired lazily when a traversal crosses a moved node — forwarding entry
  retained until the last referrer segment has been rewritten by some
  later tx).

## 7. Open questions (to resolve before freeze)

- **Q1 page size.** 4K logical pages (matches device/mmap granularity,
  minimal COW amplification for scattered melts) vs 16K (fewer freelist
  entries, longer adjacency runs per page). Strawman: 4K pages, allocation
  *clusters* of 4 pages for adjacency-heavy nodes. Decide by benchmark on
  the real 90K-entity store's dirty-page distribution per melt batch.
- **Q2 strings.** r1 strawman stands: global string segment group (max
  dedup — the KB dedups heavily), own refcounts, same paged mechanics.
  Per-segment tables would simplify future sharding (no cross-shard string
  refs) at real dedup cost — measure dedup ratio before freeze; the seam
  requires only that string refs also be `(seg|off)`-shaped.
- **Q3 segment size / split.** Target 4–16MB, split on overflow. With
  page-COW this is placement granularity only (no copy cost cliff), so the
  pressure that produced r1-Q1 is gone; choose for locality.
- **Q4 dirty-page write strategy.** Write COW pages via `pwrite` into the
  (preallocated, `fallocate`d) file vs mmap-dirty + `msync(frozen range)`.
  Lean `pwrite` + `fdatasync`: no writable mmap of live files at all, which
  makes §2's "structurally impossible" literal.
- **Q5 manifest folding.** Single-segment txs commit via segment meta only;
  the manifest then lags. Recovery rule: store txid = max(manifest txid,
  per-segment meta txids) with manifest consulted only for multi-segment
  atomicity groups. Verify this composes with lease pinning; else manifest
  every tx (it's one small fsync'd append — acceptable fallback).
- **Q6 owner lifecycle.** systemd user unit on the KB host (it's a
  long-lived network daemon now — first-client-spawns no longer makes
  sense). Health endpoint for the shims; shim behavior on owner restart =
  reconnect + re-resolve cursors (§4).
- **Q7 rank cadence.** Watermarks for ψ warm-start recompute and MC top-up
  (edit-count? wall-clock? both?). Needs measurement of rank drift vs melt
  rate on the live store.
- **Q8 migration.** One-shot import: v3 file → owner ingest as one batched
  tx stream (segment placement = §8 streaming assignment from a cold
  start). Verify counts + spot-check refs + full validate_graph before
  cutover; keep the v3 file frozen as fallback for one release.

## 8. Locality (mitigation, not solution — unchanged from r1)

KB: `Design_CommunityPageLocality_2026_07_01`. Community-based streaming
placement (Fennel/LDG-style), bounded KL/FM refinement piggybacked only on
commits already dirtying both segments, hubs accepted as cross-segment.
Under page-COW the refinement budget is cheaper (pages, not segments), but
the discipline stands: swaps ride existing dirt, never generate their own.

## 9. What this deletes

- both flocks, lock ordering, `withReadLock`/`withWriteLock`
- `refresh()`/remap protocol, `migrate.lock`
- blocking whole-arena `msync` under a lock (there is no shared mmap, no
  arena, and no lock to hold it under)
- per-client server processes mapping one live file; N stdio servers
- manual KB file transfer between machines (the entire failure mode)
- the WAL that was never written (roadmap stage 2)
- segment-generation file churn from r1 (page COW happens *inside* the
  segment file; generations exist only as meta txids)

## 10. Delivery phases

1. **Spec freeze** — iterate this document; freeze: page header, meta page,
   freelist record, manifest record, packed ref, frame header, handshake.
2. **libsegstore (C)** — paged segment + dual meta + freelist + manifest +
   commit/recovery, single process, no network. Crash-injection harness
   (kill -9 at every write/fsync boundary; property: recovered state ==
   last committed txid, no torn page reachable). ACSL/WP over commit/
   recovery paths (`Heuristic_WP_RealMmapCode_2026_06_28` technique; the
   memfile-era proofs do NOT carry over — new core, new proofs).
3. **Owner daemon** — TCP transport + token, request multiplexing, group
   commit, snapshot leases, counter relaxation, background rank (§5).
   Minimal-core index layer (trigram/type/BFS) ported onto paged reads.
4. **Client cutover** — server.ts → thin shim (MCP surface unchanged);
   migrate v3 store (§7.Q8); delete the flock layer; all machines point at
   the daemon. **This is the phase that ends KB file transfers.**
5. **Locality + rank tuning** — streaming assignment, then swaps/compaction
   and rank cadence from measurement, not before.

## 11. Explicit non-goals

- Multi-writer. One owner. Forever, until a measured workload says otherwise.
- Sharding *implementation* (multi-host segment serving). The seam (§6) is
  in; the second host is not.
- General MVCC with a shared reader table (owner-internal leases only).
- LSM levels / read-path merging (rejected: traversal is the hot path).
- In-place mutation of published pages (the v3 sin, in any disguise).
- TLS / WAN exposure (LAN + token; revisit on deployment change).
