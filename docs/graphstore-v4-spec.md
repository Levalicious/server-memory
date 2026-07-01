# Graph Store v4 — single-writer, segmented, freeze-on-publish

Status: DRAFT for iteration. Nothing here is frozen until marked FROZEN.

## 0. Why (epistemic chain)

Production hang: `create_entities` times out at 300s, self-recovers, silent
write loss. Diagnosis (KB: `Diag_MemfileSingleOwner_2026_06_29`):

- memfile is a **single-owner** construct; commit `13937aa` bolted N-process
  shared-mmap + flock onto it ("multi-instance safety") — off-design.
- The hang is a lock holder stalled in blocking `msync(MS_SYNC)` (32MB, whole
  arena) under I/O saturation, holding the flock; peers time out. Scale and
  op cost are ruled out (all ops <100ms on the real 67k store).
- Root problem: four half concurrency systems (2 flocks + migrate.lock +
  refresh/remap + msync-under-lock), none providing crash-atomicity.

Requirements (Lev): do it properly. ACID semantics. Batched ingest at
millions of nodes/sec. Graph-native read path (traversal is the hot path —
no LSM merge tax, no KV hop tax). Locality as mitigation, not solution.

## 1. Architecture

One **owner** process per store. Clients talk to the owner over SPSC
shared-memory rings (design reused from biscuit v2 IPC; KB: `v2_I_IPC`).
Nobody but the owner ever maps a writable segment. There is exactly one
concurrency mechanism in the system: the ring.

Storage is **N segment files + 1 manifest**:

    store/
      MANIFEST            # commit pivot (tiny, append-only, fsync'd)
      seg-0003.g42        # segment 3, generation 42 (immutable once published)
      seg-0007.g18
      strings-00.g9       # string segments (same mechanics)
      ...

Terminology: **segment**, deliberately not "shard" — one machine, one writer,
one transaction domain. Segments are a physical layout + COW granularity
choice, not a distribution boundary. Cross-segment edges are cheap (a deref
into another mapping), never a cross-domain operation.

### 1.1 Segment = memfile, unchanged

Each segment IS a memfile (v3 construction: header, cartesian-tree
allocator, in-place mutation). The memfile code does not change and is not
shared: it remains single-owner and in-place — **but only ever as the
owner's private working copy**.

### 1.2 Freeze-on-publish (the COW rule)

- Published segment files are **immutable**. Nothing in-place-mutates a
  published file, ever.
- To mutate segment S at generation g: owner works on a private copy
  (CoW of `seg-S.g`), mutates in place freely (memfile semantics intact),
  then publishes it as `seg-S.(g+1)` at commit.
- Commit protocol:
  1. write + fsync every dirty segment's new generation file
  2. append a manifest record: `{txid, [(seg → gen)...], checksum}`; fsync
  3. the manifest record IS the commit point (single atomic pivot)
  4. old generations are reclaimed once no reader lease pins them
- Recovery: read MANIFEST to the last valid record; map exactly those
  generations; delete orphan generation files. No WAL replay, no torn
  state — a crash before (2) leaves the old commit intact by construction.

This is shadow paging at segment granularity, with memfile as the page.

### 1.3 ACID

- **A**: the manifest append is the only commit point; all-or-nothing.
- **C**: single writer, validation runs in the owner before publish.
- **I**: readers pin a manifest txid (lease); published files are immutable,
  so a snapshot is a set of frozen files — trivially stable. Single writer
  ⇒ serializable.
- **D**: fsync order (segments, then manifest). Group commit: one batch =
  one commit = one manifest fsync — this is where millions-of-nodes/sec
  batched ingest comes from (sequential writes into fresh segment files +
  a single pivot).

**Relaxed-durability class (required):** walker/structural visit counters
mutate on every *read* op. Under freeze-on-publish, counter-per-read would
copy a segment per read. Counters therefore accumulate in owner memory and
flush on a periodic/piggybacked commit. A crash loses recent counters —
rank statistics, explicitly not graph truth. Everything else is fully
durable at its commit.

### 1.4 Addressing

Node/edge references become `(segment_id, offset)` packed into u64
(u16 seg | u48 offset). Segment growth or regeneration never invalidates
refs (offsets are stable within a segment lineage; generations change file
names, not offsets). Rebalancing (§1.6) DOES move nodes across segments and
must rewrite in-refs — see open question Q3.

### 1.5 Read path

- v4.0: all reads through the owner (SPSC request/response). Single-version,
  no merge, traversal identical to v3's — sequential adjacency within a
  mapped segment.
- v4.1 (option, later): direct read-only clients. Because published
  segments are immutable, a reader may mmap them PROT_READ under a manifest
  lease with zero races by construction. This is MVCC-lite where
  immutability does all the work: no reader table in shared memory, just
  lease bookkeeping in the owner. Only adopt if owner-mediated read
  throughput actually becomes the bottleneck.

### 1.6 Locality (mitigation, not solution)

KB: `Design_CommunityPageLocality_2026_07_01`.

- Node→segment assignment is **community-based** (min-cut-ish), not
  centrality-based. Streaming placement on insert (Fennel/LDG-style:
  place with plurality of neighbors, subject to segment fill).
- Refinement: bounded KL/FM-style swaps, lazy, and only piggybacked on
  commits already dirtying both segments involved (a swap is a COW write;
  the budget caps write amplification).
- Hubs: the KB is power-law; hub edges WILL cross segments. Accepted cost
  (cross-segment deref ≈ cache miss, nothing more). Do not ask the
  partitioner to localize hubs.
- Full re-cut: offline compaction pass, last resort, never required for
  correctness.

### 1.7 What this deletes

- both flocks, lock ordering, `withReadLock`/`withWriteLock`
- `refresh()`/remap protocol, `migrate.lock`
- blocking whole-arena `msync` under a lock (msync of a frozen file happens
  before publish, outside any critical section — durability, not integrity)
- per-client server processes all mapping one live file

## 2. Delivery phases

1. **Spec freeze** — iterate this document; freeze formats (manifest record,
   segment header delta, packed ref).
2. **libsegstore (C)** — segments + manifest + commit/recovery, single
   process, no IPC. Crash-injection harness (kill -9 at every fsync
   boundary; property: recovered state == last committed txid). ACSL/WP
   over the real commit/recovery paths (technique per
   `Heuristic_WP_RealMmapCode_2026_06_28`; memfile-level proofs carry over
   because memfile is unchanged).
3. **Owner daemon + SPSC** — ring transport (biscuit v2 design), op
   protocol, batching, counter relaxation.
4. **Client cutover** — server.ts becomes a thin SPSC client (MCP surface
   unchanged); delete the flock layer; migrate v3 store (one import batch).
5. **Locality** — streaming assignment first; swaps + compaction after
   measurement, not before.

## 3. Open questions (to resolve before freeze)

- **Q1 segment size.** COW copy cost per dirty segment vs file-count/locality
  granularity. Strawman: target 4–16 MB, split on overflow.
- **Q2 strings.** Global string segments (max dedup, KB dedups heavily) vs
  per-segment tables (locality, simpler moves). Strawman: global string
  segment group with its own refcounts, same freeze-on-publish mechanics.
- **Q3 in-ref rewriting on rebalance.** Moving node n (seg A → seg B)
  invalidates (A,off_n) refs held in other segments' adjacency. Options:
  (a) forwarding stubs in A until next compaction touches the referrer;
  (b) per-node indirection table (node-id → packed ref) making refs logical
  everywhere. (b) costs one indirection per hop; (a) complicates traversal.
  Leaning (b) — the indirection table is also what makes node-ids stable
  for external callers (MCP layer, walker counters).
- **Q4 dirty-copy strategy.** Copy whole segment on first dirty (simple,
  bounded by Q1) vs reflink/`copy_file_range` where FS supports it.
- **Q5 manifest compaction.** Append-only manifest grows; periodic rewrite
  with a generation of its own (same freeze-on-publish trick, applied to
  the manifest).
- **Q6 owner lifecycle.** Who starts/supervises the owner daemon (systemd
  user unit vs first-client-spawns+lockfile); owner crash = clients
  reconnect, state = last commit.

## 4. Explicit non-goals

- Distributed sharding, multi-machine anything.
- Multi-writer. One owner. Forever, until a measured workload says otherwise.
- General MVCC with a shared reader table (v4.1's immutable-snapshot leases
  are as far as we go).
- LSM levels / read-path merging (rejected: traversal is the hot path).
- In-place mutation of published state (the v3 sin, in any disguise).
