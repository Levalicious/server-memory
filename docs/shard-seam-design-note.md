# Shard-Seam Activation — Design Note

Status: **approved direction** (Lev rulings 2026-10-09; tombstone sketch +
RIBLT reuse approved 2026-10-09/10). Spec §6.4 r3.3 points here.
(`Design_ShardSeamFirst_2026_08_20`, `Design_ShardOpClasses_2026_08_23`,
`Design_CommunityPageLocality_2026_07_01`, `BGS_Sharding` + its open
questions, `Decision_Lev_ReconciliationRulings_2026_10_09`.)

The seam goes in NOW, at N=1, because every arm below is format-breaking if
retrofitted: wire ids are frozen on first release, node identity must be
logical before anything moves, and the replication channel is a protocol
addition. Sharding *deployment* still scales up only when turned on.

## 1. Model: vertex-cut with duplicated vertices

One owner daemon per shard; one linearizable writer per domain.

- **Authority**: each entity's record and its own adjacency chain live on
  its primary shard (community-assigned, §3). All writes to them are
  shard-local single-domain commits — Class A, zero coordination.
- **Duplicated vertices (mirrors)**: each shard keeps replicated copies of
  its boundary vertices — fields, flags, ψ — so traversals and result
  rendering read locally instead of hopping. This is PowerGraph's
  vertex-cut result applied here: on power-law graphs, vertex-cuts produce
  cuts orders of magnitude smaller than edge-cuts; hubs are *replicated,
  not shattered*. The KB is power-law; this is the family that scales for
  it.
- **Adjacency halves are endpoint-local** (ruling i): an edge A–B is two
  halves, A's side at primary(A), B's side at primary(B). No edge is ever
  split; each half is complete and local. A relation create is **two
  independent idempotent local commits**, synchronous (both acked before
  the client's op returns — ruling ii) — *not* a distributed transaction.
  A crash window leaves a half-edge; anti-entropy repairs it (§8).
- **No primary coordinator** (D2): placement is a pure function —
  `name → directory shard` (hash) and `node-id → primary shard`
  (community + indirection). Every daemon resolves it locally from cached
  directory/assignment state; ops forward at most one hop. There is no
  coordinating role; cross-shard effects ride replication, not a
  transaction protocol.

## 2. Convergence theory spine

Every mechanism below is classified by the standard monotonicity/commutativity
lens; coordination appears **only** where it is mathematically irreducible.

| class | ops | algebra | coordination needed |
|---|---|---|---|
| SET | neighbors(d), reachability, orphans, search, by-type, scans | union — commutative, idempotent, monotone | none (CALM): any order, any retry, any duplication |
| MIN_DIST | distances, shortest path | min-combine — monotone | none: async over-exploration legal, label correction |
| ANY_PATH | find_path | first-witness over idempotent search | none: first meet wins |
| LWW-registers | attribute + ψ updates, deletions | total order by (stamp, shard-id) | none: deterministic merge |
| ANTITONIC/bookkeeping | GC watermarks, tombstone horizons | monotone knowledge ("all peers have seen ≥ e") | none: lattice of knowledge states |
| SERIAL | random_walk order, DFS ordering, cursors | non-commutative chain | **the one survivor**: exactly-once token handoff — the existing v1.5 lease; consensus scoped to one token transfer, never global |
| UNIQUENESS | entity name binding | compare-and-bind | the directory shard serializes per name (hash-routed) — a single-domain owner decision, not a quorum |

ψ specifically is an **asynchronous contraction iteration**: the MERW
update is a contraction (spectral radius < 1); asynchronous fixed-point
iteration over shards converges under exactly that condition — which is
*why* no boundary-epoch protocol is needed (§7).

## 3. Identity & placement

- **Node ids are logical, `u32`** — same width as the physical refs they
  replaced, so every table layout (name values, posting keys, adjacency
  entries, wire payloads) stayed byte-identical; only the meaning changed.
  Dense from 1, minted on create, never reused.
- **Indirection table** activates the reserved `SEG_KIND_INDIRECT`: a
  per-store `node-id → (shard, seg, lpg/slot)` map. At N=1 it is the
  identity mapping; it is the mechanism that lets entities *move* —
  community migration (Fennel/LDG streaming assignment, lazy, piggybacked
  on COW writes) and resharding both reduce to: copy → flip indirection →
  tombstone old.
  **Landed (step 2):** dir page holds one record `[u32 npages][u32 lpg ×
  npages]`; data page `p` holds one record of 1019 `u32` eids for nodes
  `[p·1019, (p+1)·1019)`. Dir capacity 1018 pages ≈ 1.03M nodes — the
  named level-1 cliff (a second-level dir or a tree replaces it if
  crossed; create fails loudly, never silently). `next_node` persists in
  META v2 (pre-release: v2-only, strict — v1 stores refuse to open rather
  than mint colliding ids). A killed id resolves dead: stale refs fail
  cleanly instead of aliasing a recycled physical slot (the v3
  reuse-aliasing class, closed by construction).
- **Name directory**: `hash(name) → {node-id, generation}` bindings,
  one directory (tables per shard). The directory shard is the only global
  serialization point, hit on create/delete only (rare vs edits). Names
  immutable per generation; re-creation binds a new node-id generation, so
  stale caches self-invalidate on generation conflict.
  **Landed (step 3):** rows are 8-byte `{node u32, gen u32}` values in the
  name table; every transition (bind or unbind) advances `gen`; unbound
  names keep a tombstone row (`node = 0`) so a later re-creation advances
  the generation — enumeration skips tombstones; generations persist and
  continue across reopen. Routing hash (normative once N>1):
  `fnv1a32(name) % N` — a pure function of the name bytes, so no per-store
  state is read to decide where a binding lives; implemented when the
  directory becomes multi-shard addressable.
- **N is growable** (D5); deployment starts at 1.

## 4. Replicated vertex record (+ mirrors)

Mirror payload per vertex: the entity fields as published + ψ + flags.
Merge = LWW per field group by `(stamp, shard-id)` with a deterministic
tiebreak; `deleted` is just another stamped field (§9). Stamps are the
per-shard monotone counters (the write-gen built for the index gate
generalizes). ψ is stored as a **fixed-point int** (fixed 1e-9 units) —
int encoding makes merges exact and reproducible; "ranks stable across
clients" (ruling 4-i) survives sharding by construction.

Mirror scope ruling: **vertex-data mirrors only** (fields + ψ). Full
adjacency mirroring for hubs is deferred pending measurement (§12).

## 5. Adjacency

- Creation: two idempotent local half-writes, sync-both-acked.
- Deletion: remove at each half; **no per-edge tombstones** — each chain
  carries one u64 *remove watermark* (max deleted mtime); a re-applied add
  with an older mtime is dropped. O(1) per chain; covers the retry
  resurrection path entirely.
- Bidirectional halves remain the traversal substrate (the 08-23 note's
  "bidir halves rounds").

## 6. Traversal & the wire

- Frontier batching per `Design_ShardOpClasses_2026_08_23`: level-synchronous
  BFS with **one batched RPC per shard per level**; bidir halves the rounds.
- Continuation tokens/leases gain the **shard-id space** (v1.6): a token
  names (shard, node-id, txid). Cross-shard snapshot = pin the manifest
  record (§5 of the spec, unchanged). SERIAL ops migrate to the owning
  shard with the exactly-once lease (v1.5 machinery already shipped).

## 7. Rank / ψ under sharding (closes the 08-23 "Open: psi")

Per-shard slices run the amortized MERW iteration over the local subgraph
*including mirror values* (mirrors are local data); the result is published
as the vertex's int-LWW ψ. Global convergence = asynchronous contraction
iteration (§2) over the replication channel; staleness = one anti-entropy
round of lag, nothing more. No boundary-vector epochs, no rank epochs in
the manifest.

## 8. Anti-entropy: RIBLT, reused

Reuse `sunder/src/sunder/sync/riblt.{c,h}` (+ tests): rateless IBLT
(Yang/Gilad/Alizadeh SIGCOMM 2024), universal encoder (one coded stream
serves every peer), already carrying the cell-0-reliable / rest-rateless
wire contract (Rule_RIBLT_Cell0Reliable_RestRateless) and the peel
invariant from `RIBLT_Bug_PeelMustTrackFutureCells_2026_05_23`
(re-verify on port). Self-authored, no external dependence; vendored
Blake2s.

32-byte symbol codecs (landed, step 4; emitted inside a txn view):

- **adjacency**: `[peer u32][dir u8][rel_sid u32][mtime u64][pad]` — one per
  chain row; halves reconcile as plain symbol sets.
- **vertex-state**: `[node u32][binding gen u32][content-hash u64][pad]` —
  the hash covers type_sid, obs sids/count, mtime, obs_mtime, psi; visits
  reconcile separately (the relaxed-counter class). The hash is a local
  64-bit mix for diff *detection* only — the RIBLT's own checksum stays
  blake2s-keyed, so peeling correctness never depends on it.
- Verified against ground truth in `test_repl`: clone a committed store,
  mutate (creates, deletes, observations, relation add/remove, a
  delete+recreate name), extract both sides, reconcile — the decoded
  symmetric difference equals the exact set difference, both families.

Rounds run pairwise between shards over the daemon channel (REPLICA /
ANTI_ENTROPY op classes, same binary protocol, versioned). Cost is
O(symmetric difference); community placement keeps healthy-shard diffs
small by construction. Per-peer watermarks fall out of the rounds (§9).

## 9. Tombstones & GC contract — bounded by construction

- Vertex deletion = the `deleted` LWW field; merges like everything else.
  No tombstone set.
- **Physical drop** of a tombstoned mirror record once
  `min over current peers(watermark) ≥ its stamp`, plus grace.
- A peer offline past the horizon **loses its vote**: on return it does a
  full-fetch resync of its mirrors (drop + rebuild) instead of delta
  repair. History is never stockpiled for absentees — the bounded-memory
  contract.
- Directory tombstones live for the in-flight op window only
  (generation-guarded).

Net GC surface: deleted-vertex mirror records (watermark+grace bounded),
O(N) watermark tables, one u64 per adjacency chain. **Nothing grows
unboundedly.**

## 10. Wire additions

- v1.6: shard-id in refs, continuations, leases. Id 0 today; no behavior
  change at N=1.
- Daemon channel op classes for replication (REPLICA, ANTI_ENTROPY);
  trailing-field growth per spec §6 policy.

## 11. Build order (all at N=1)

1. Wire shard-id space (v1.6). — protocol only, testable against v1.5.
   **DONE (`3d5a1ee`).**
2. Node-id indirection (identity-mapped; `SEG_KIND_INDIRECT` live). —
   refs become logical; the existing battery + parity gate prove no
   behavior change. **DONE (this commit).**
3. Name directory store (hash-routed bindings + generations).
   **DONE (this commit).**
4. Replication channel + RIBLT port + symbol codecs + watermarks.
   RIBLT port + codecs + reconcile-on-real-data **DONE (`13d5c1a` +
   this commit)**; the daemon channel op classes and peer watermarks land
   with the sharded fuzz (step 5), where peers exist to exercise them.
5. Sharded fuzz: extend the existing 30-agent harness to N daemons over N
   shards, with kill-mid-half-write, offline-past-horizon resync, and
   convergence assertions. This is the gate for every step above.
6. Rank slices over mirrors (closes §7).

## 12. Open items (measure, don't guess)

- Mirror scope: vertex-data-only vs hub full-adjacency — decide by
  measurement on the sharded fuzz.
- Grace/horizon defaults — set from the fuzz's kill-window matrix.
- mtime skew clamp for new adds with regressed clocks (bump locally).
- Directory sizing re-check at ~1M names (~16 B/name today).

## 13. Non-goals

No consensus protocol, no quorum, no synchronous cross-shard transaction
anywhere on the data path; the only "agreement" artifacts are the directory
shard's per-name serialization and the SERIAL-op token handoff. No
per-edge tombstones. No history stockpiling for absent shards.
