# V4 Concurrency — Design Note

Status: **DRAFT for Lev's review.** Nothing in code changes on the strength of
this note until Lev has read it. Rulings it builds on:
`Decision_Lev_V4ConcurrencyRulings` (2026-11-04): format freeze lifted;
OR-set adjacency tentatively approved (size condition, §6.4); per-client
processes on one live file approved; separate DB instances that need manual
resolution must become impossible.

KB anchors: `Design_V4C_TwoLayers` (entry point), `Req_ConcurrentClientsDriveSharding`,
`Insight_ConflictTopologyFollowsIncidence`, `Req_GateNoAckedWriteLost`,
`Req_OneDurableLogicalDB`.

---

## 0. Why this note exists

The requirement Lev set from the start: **many clients work on the KB at the
same time, and that scales.** Sharding exists *because of* that requirement;
locality and low shard-to-shard traffic are means to it, not the goal.

The r3 spec did not meet it, and said so in its own text:

- Title: "networked **single-writer**".
- §1: "One concurrency mechanism: the owner's queue." §4: "All reads through
  the owner."
- §9 lists "per-client processes on one live file" as *deleted*.
- §11 non-goals: "**Multi-writer**".
- §0's diagnosis of the v3 hang (msync under flock with N processes) was
  answered with "one owner daemon … dissolves both" — the N clients were
  removed rather than served. `Design_DBScaleUp_Roadmap_2026_07_20` had
  recorded "Multi-writer required"; the spec dropped it without saying so
  (`Pattern_SilentScopeNarrowing`).
- The shard-seam note keeps the premise per shard: "one linearizable writer
  per domain".
- The 2026-10-09 reconciliation graded ownership/concurrency **ALIGNED** — it
  graded the code against the spec, not against the requirement.

Removing the lock file did not remove serialization. Walking the frozen
formats turns up these structural single points (each has a KB node):

| # | Where | Why it serializes |
|---|---|---|
| S1 | one meta root per segment, toggled per commit; MANIFEST fsync'd on **every** commit | every commit passes one root and one log append |
| S2 | `next_node` "dense from 1, minted on create" (META v2) | every create increments one counter; consecutive ids also hit one tail page |
| S3 | LWW stamps = per-shard monotone counter | every write increments one cell |
| S4 | freelist rewritten every commit, meta-rooted | every committer read-modify-writes it |
| S5 | name index whole-index doubling rehash | one write touching every bucket (resolved by the B+tree ruling, `cb249c8`) |
| S6 | string refcounts in one record per string | every `PART_OF` edge writes the same record |
| S7 | visit counters bumped by reads | every read writes the entities it returned; hubs hottest |
| S8 | page-granular COW (4K) | writers on *different* records of one page collide |
| S9 | `seg_txn_begin(segstore_t*)` has no txn handle | one live txn per store object, by signature |

And three places where a second writer would **lose acknowledged writes**
(R4 violations), safe today only because there is one writer:

| # | Where | Failure |
|---|---|---|
| L1 | observations merged as LWW registers | two clients each add one obs to a one-obs entity: one write vanishes; merging both breaks the 2-cap |
| L2 | per-chain remove watermark (`adj_wm`) | A adds X@t1, B deletes unrelated Y@t2>t1 on the same node; a late/retried X is dropped, and anti-entropy answers it with a *delete* everywhere |
| L3 | vertex delete = LWW `deleted` field | `create_relations` to X concurrent with deleting X: acked, then gone |

On the read side, leases Phase A (`RESUME` requires `store_txid ==
lease.store_txid`) makes every open cursor stale on *any* commit; under steady
writes paging can never finish.

## 1. Requirements

- **C1** Any number of writer processes and threads, on one host, against one
  live store. No flock, no lock file, no owner queue, no daemon that owns
  state. (Lev 10-10, 11-04.)
- **C2** Serializable: every committed write batch equals its request applied
  to the state left by the commit before it.
- **C3** No acknowledged write is ever lost; anything not applied is rejected
  visibly (R1/R4). Ledgers (R3) describe what actually happened.
- **C4** Readers are never blocked by writers; cursors stay valid under
  concurrent commits.
- **C5** The only coordination is what an invariant makes irreducible, and it
  happens at the object that owns the invariant (one entity, one name) —
  never a shard or the store.
- **C6** Across shards: converge automatically with bounded memory; no
  per-edge tombstones; no n×m structures.
- **C7** One durable logical DB. Divergent instances that need manual
  resolution must not be possible (§8).

## 2. Conflict model — where the graph gives the sharing topology

Invariant confluence (Bailis et al., *Coordination Avoidance in Database
Systems*, PVLDB 8(3), 2014): an operation needs coordination iff merging two
independently valid states can violate an invariant. Applied to the KB's
invariants:

| Invariant | Owner | Merge-safe? |
|---|---|---|
| entity name unique | the name's binding | **no** — compare-and-bind |
| ≤ 2 observations per entity | the entity record | **no** |
| a relation's endpoints exist | each endpoint | **no** under concurrent delete |
| relation set membership (`from,type,to`) | that adjacency entry | yes — set add/remove of distinct entries commute |
| counters (visits, refcounts) | — | yes — pure increments |
| id uniqueness | — | yes, given disjoint id blocks |

**Consequence (`Insight_ConflictTopologyFollowsIncidence`):** two write
batches can conflict only through a shared vertex or name, and only when at
least one of them does a non-merge-safe operation on it. The contention
structure *is* the KB's incidence graph, filtered to non-commuting ops. Edge
adds to a hub are distinct entries, so they commute — hubs do not serialize.
What remains is local to single vertices, and community placement keeps those
vertices on one shard.

Per MCP write op:

| Op | Touches | Effect at commit |
|---|---|---|
| `create_entities` | name binding, new record, id block | name already bound in R → item reported `existing` (R3); else bind |
| `add_observations` | entity record | evaluated against R: fits cap → apply; else `LIMIT_EXCEEDED` (R2: batch fails, nothing written) |
| `delete_observations` | entity record | evaluated against R; absent → `notFound` |
| `create_relations` | two adjacency entries; endpoint existence | endpoint missing in R → `ENDPOINT_MISSING` (batch fails); duplicate → `skippedDuplicates` |
| `delete_relations` | adjacency entries | absent in R → `notFound` |
| `delete_entities` | record, binding, its adjacency halves + neighbours' mirror halves | evaluated against R |
| visits / refcount deltas | counter fields | added onto R's values; never conflict |

## 3. Layer 1 — many writers inside one store

### 3.1 The commit is a function of (request, root)

MCP write ops return only post-commit ledgers; no client sees intermediate
reads inside a write. So a write batch is a pure function of the request and
the root it commits on top of. **The commit re-executes the request against
the latest root R.** No read sets, no version validation: if R moved, the
request is evaluated again against the new R, so every ledger and every error
is exact for the state it actually committed on. (Read-then-write
transactions — a client reading and writing in one txn — do not exist in
today's API; §10 Q3.)

### 3.2 Shared state: `store/CONTROL`

A small `MAP_SHARED` file, accessed only through C11 atomics. It is **not
durable** — it is rebuilt at cold start (§3.7). Layout (sizes indicative):

```
hdr        magic, version, state:u32 {UNINIT, RECOVERING(pid), READY}, epoch:u16
root_word  _Atomic u64 = epoch:12 | txid:40 | cand_slot:12   — the commit order
durable    _Atomic u64 durable_txid                           — max root known fsync'd
ring[R]    in-memory root vectors, slot = txid mod R          — materialized publishes
id_next    _Atomic u64                                        — node-id blocks (fetch_add 1024)
seg[S]     _Atomic u64 watermark                              — extent leasing by fetch_add
freeruns   tagged Treiber stack of free page runs             — from sweep (§3.6)
proc[P]    { pid, starttime, cand (root vector), pins[k], extents[e] }
```

A **root vector** is, per segment: `ptable_root`, `logical_pages`, graph roots
(name index, indirection, …), `watermark`; plus store-wide `id_hwm`, the
per-shard causal context root (§4), and the `txid` itself. ~32 B/segment; a
64-segment store fits in 2 KiB.

### 3.3 Transaction lifecycle

```
begin:   t0 = root_word.txid; R0 = ring[t0]  (help-materialize if needed, §3.4)
         register pin(t0) in proc slot
build:   evaluate the request against R0:
           - COW-touch affected records into private page buffers
           - new physical pages come from the proc's leased extent (bump alloc)
           - ptable entries for touched logical pages point at the new pages
           - strings are carried as BYTES in the op log; sids resolved here
           - new node ids from the proc's leased id block
         pwrite the new pages (page cache only — no fsync yet)
         write candidate vector into proc.cand (txid = t0+1)
publish: CAS(root_word, (e, t0, *), (e, t0+1, my_slot))
           win  → materialize ring[t0+1] from proc.cand; go to durable
           lose → rebase onto t1 = root_word.txid (§3.5), repeat publish
durable: fdatasync every segment file this txn wrote
         pwrite ROOTS slot (t mod RR) = root record t; fdatasync ROOTS
         atomic_max(durable_txid, t)
ack:     reply with the ledgers computed in the winning build
         unpin t0; pages replaced by t are now retired at t
```

Failed CAS attempts never write anything another process can see: their pages
sit in the loser's own extent and are reused for its next build.

### 3.4 Publish: one CAS, with helping

The CAS word names *which proc's candidate* won. Any process that reads
`root_word = (e, t, s)` and finds `ring[t mod R].txid != t` copies
`proc[s].cand` into the ring itself. So a winner that dies between the CAS and
materializing never blocks anyone. A proc does not reuse its `cand` until
`ring[t].txid == t`. The epoch field fences the CAS across cold restarts.

What is ordered: one CAS plus a ~2 KiB copy per commit. That *is* the commit
order every serializable store has; here it is a few hundred nanoseconds, not
the whole transaction. Executing, building, pwrite, fsync and the rebase of
losers all run in parallel.

### 3.5 Rebase — O(my dirty pages), not O(transaction)

On a lost CAS the loser holds pages built on R0, and the root is now R1
(possibly several commits later). For each logical page in its dirty set it
compares `ptable(R0, lpg)` with `ptable(R1, lpg)`:

- **unchanged** — its built page stays valid; only its ptable entry is merged
  onto R1's ptable (entries for different logical pages are independent);
- **changed** — the request's ops on the *structure* that page belongs to
  (one entity's record, one adjacency chain, one B+tree, the string table)
  are re-run against R1, which also re-derives B+tree splits.

Because outcomes are recomputed against R1 (§3.1), a concurrent commit that
took the 2nd observation slot, bound the name, or deleted an endpoint is seen
here, and the ledger or error reflects it. That closes L1 and L3, and S8: page
collisions between different records cost a re-run of the affected structure,
never a lost write.

### 3.6 Allocation and reclamation — no freelist

- **Extents**: a proc leases an extent of a segment by `fetch_add` on
  `seg[s].watermark` (then `fallocate`s its range), or pops a free run from
  `freeruns`. It allocates pages from it by bumping a pointer. Leased extents
  are recorded in its proc slot.
- **Sweep** (any proc, periodically or when watermark growth crosses a
  threshold): mark every physical page reachable from the durable root, the
  current root, every root in `(durable, current]`, and every pinned root
  (ptable pages + mapped data pages; roots share most pages). Free = `[0,
  watermark)` minus marked minus live leased extents, all as of the sweep's
  start. Push free runs to `freeruns`.
- A page retired by commit t stays reachable from root t−1 until that root is
  neither durable-relevant nor pinned, so the sweep frees it only once it is
  safe — the same rule as the old `min_pin_txid` gate, generalized to many
  processes.
- Dead procs: their leased extents stop counting as live, so their unpublished
  pages are swept. Nothing leaks, and there is no per-commit freelist rewrite
  (S4).

Cost at today's scale: ptable ≈ 15 pages at 500K entities; a full mark is a
few ptable scans and a bitmap of ~1 bit/page.

### 3.7 Durability and recovery: `store/ROOTS`

ROOTS replaces both the dual-meta slots and the MANIFEST (S1). It is a ring
file of RR = 1024 pages; root record t lives at `(t mod RR) × 4096`:
`[magic][txid][root vector][crc32c]`. Its slot is fixed by its txid, so
writers never contend for slots.

**Why one later record is enough:** every root is cumulative — t+1 was built
on t. A commit's data pages were `pwrite`n before its CAS, and therefore
before any later commit's `fdatasync` of the same file. So once record t is
fsync'd, all roots ≤ t are durable. That gives the ack rule (`durable_txid ≥
t`) without ordering anyone's fsync behind anyone else's. Concurrent fsyncs
coalesce in the kernel; group commit falls out without a leader.

**Recovery** (cold start only):
1. Pick the max-txid record in ROOTS with a valid crc whose page references
   fit the segment files. That is the store state. It may include commits
   that landed but were never acked — the same {acked, acked+1} property as r3.
2. Rebuild free space by the §3.6 sweep against that single root.
3. Reinitialize CONTROL: new epoch, `id_next = id_hwm`, watermarks from the
   root, empty ring except the recovered root.

Multi-segment transactions need no extra protocol: one root record names
every segment's root.

### 3.8 Process lifecycle

- **Attach**: open/`mmap` CONTROL; claim a proc slot by CAS on its pid field,
  storing pid + process start time (guards against pid reuse).
- **Cold start**: if `state == READY` but no registered proc is alive, CAS
  `READY → RECOVERING(mypid)`, run §3.7, set `READY` with a new epoch. If the
  recovering pid dies, the next attacher CASes the state over to itself. A
  proc that attached during a recovery fails its next CAS on the epoch and
  re-attaches.
- **Death at runtime**: pins dropped, extents swept; a won CAS is materialized
  by helpers; a lost candidate was never visible.

CONTROL needs a local filesystem (shared-mmap atomics do not work over NFS).
Store files live on the host whose processes write them; remote machines reach
them through a daemon on that host (§10 Q2).

### 3.9 Readers, cursors, counters

- **Reads** pin a root (ring copy) and run on the RO `MAP_SHARED` mapping.
  Pins are visible to the sweep, so pinned pages stay byte-stable (`seg_pin`
  already proves this for one process).
- **Cursors** are continuation leases on a pinned root (leases Phase B from
  day one): they stay valid while writers commit. Phase A validate-or-stale
  is retired.
- **Counters** (S6, S7): each proc accumulates visit and refcount deltas in
  memory and adds them as delta ops to its next write commit, or to an idle
  flush commit. Deltas are added onto R's values at build/rebase, so they
  never conflict. They stay in the relaxed class, as before: not durable per
  read.
- **Ids** (S2): blocks of 1024 by `fetch_add`; persisted as `id_hwm` in the
  root record. Ids stay unique and never reused; they are no longer dense.
- **Stamps** (S3): a mirror field's stamp is the commit txid that wrote it,
  assigned by the CAS. No separate counter.

## 4. Layer 2 — across shards

Kept from the seam note: vertex-cut placement (PowerGraph), endpoint-local
adjacency halves, a relation create = two idempotent local commits acked
before the client's op returns, hash-routed name directory, RIBLT
anti-entropy, no coordinator, no cross-shard transaction.

Changed:

- **Edges carry dots** (optimized OR-set, Bieniusa et al. 2012). The shard
  that receives a relation create assigns one dot `(origin shard u16 | seq
  u48)`; both halves store it. The seq comes from a per-shard dot high-water
  mark kept in the root vector, assigned contiguously at build time by the
  winning commit (rebase reassigns).
- **Causal context per shard**: a version vector over origin shards (max
  contiguous seq per origin) plus a small set of seen-but-gapped dots. When a
  peer offers an edge half with dot d during anti-entropy:
  - d **not covered** by my context → new add: apply it;
  - d **covered** and no live entry carries d → I removed it: answer *remove*.
  
  A re-added edge gets a new dot. An unrelated delete can no longer suppress a
  live add (L2). The per-chain `adj_wm` is removed.
- **Delete vs edge create across shards**: the X-half of `B→X` is rejected at
  `primary(X)` if X is deleted there; `primary(B)` then removes its half in a
  compensating local commit and the client gets the error. A crash in between
  leaves a half-edge that anti-entropy removes (X's vertex state is deleted at
  a newer txid than the dot). Acked edges always have both halves.
- **Mirrors**: LWW stays, but only for fields with a single authoritative
  writer (the primary), stamped by the primary's commit txid.

### 4.1 Size (the condition on Lev's ruling)

From `seg_graph4.c` today (`ADJ_ENT = 20`, `ADJ_MAX_ENT = 203`, `ENT_SIZE = 76`):

- per live edge half: +8 B dot → `ADJ_ENT = 28`, ~145 entries per record;
- per entity: −8 B (`adj_wm` removed);
- causal context: one version vector per shard, N × u64 ⇒ N² u64 store-wide,
  N = number of shards; plus a gap set that empties once seqs are contiguous
  again;
- extra dots on one edge only while it is deleted and re-added during an
  in-flight round — collapses to one on merge.

O(E) + O(N²). No n×m structure anywhere.

## 5. Format changes vs r3 (freeze lifted)

| r3 | V4C |
|---|---|
| `seg_meta_t` dual slots with txid + roots | static segment header only; roots live in ROOTS |
| MANIFEST | removed (ROOTS) |
| SEG_KIND_FREELIST + `freelist_root_pgno` | removed (sweep) |
| META `next_node` dense | `id_hwm` in the root record; block-leased ids |
| seg txid = store txid via manifest | txid = root_word sequence |
| entity `adj_wm` u64 | removed |
| `ADJ_ENT` 20 B | 28 B (+dot) |
| — | per-shard causal context (in the root vector / own page) |
| — | `store/CONTROL` (non-durable) |
| `seg_txn_*(segstore_t*)` | txn handles: `seg_txn_t *seg_txn_begin(st)`, all ops on the handle |

## 6. What is ordered, honestly

- One CAS + ~2 KiB copy per commit (the commit order).
- A loser's rebase of the structures whose pages changed under it.
- `fdatasync`: concurrent, coalesced by the kernel; nobody waits for a
  designated flusher.
- Cold-start recovery: exclusive, but only when no process is attached.

Everything else — parsing, evaluation, page building, `pwrite`, sweeping,
reads — runs in parallel. Rough bound from the r3 bench (pin+unpin 3.7K cyc;
a 16-record commit 69K cyc, most of which is now parallel work): the ordered
step is ~1% of one commit's CPU. Fsync remains the throughput limit, as
`Design_DBScaleUp_Roadmap` already noted, and group commit is what lifts it.
To be measured, not assumed (§9).

## 7. Index structures under concurrent commits

- **Name index (B+tree, `cb249c8`)**: inserts and deletes re-run on R at
  rebase; distinct names touch distinct leaves, so most rebases keep their
  pages; same name → compare-and-bind outcome (§2). No whole-index rehash (S5).
- **Trigram / type tables (persisted per the 10-09 ruling)**: posting adds and
  removes are set ops, so they commute.
- **Strings**: interning is resolved at build time against R (ops carry
  bytes), so two writers interning the same new string converge on one sid;
  refcounts are deltas.
- **Adjacency chains**: entry inserts go to the head record of R's chain; a
  changed head means re-running inserts for that chain.

## 8. Cutover: one logical DB, merged from every machine

There are live v3 KB files on several machines, and some records exist only
in some of them (the 10-09 nodes cited by `v4-reconciliation.md` are missing
here). The import therefore **merges all of them** into one V4 store; it does
not pick one:

- entities: union by name;
- relations: union by `(from, type, to)`, keeping the max mtime;
- visit counters: summed (independent sessions; commutative);
- observations: union, deduplicated — see §10 Q1 when the union exceeds 2;
- after cutover, every machine talks to the one store (local processes
  directly, remote ones through the host daemon); no per-machine store files
  remain.

## 9. Gates (nothing merges without them)

1. **No acked write lost** (`Req_GateNoAckedWriteLost`): K processes, the
   *same* entities and edges, kill -9 injected at each step (after pwrite /
   after CAS / before materialize / after root record / before fsync / before
   ack). After recovery: every acked op is present (or removed by a later
   acked op), every visible commit is ≤ the recovered root, every ledger
   matches what happened.
2. **Serializability**: log each commit's request + txid; replay serially in
   txid order on a model; state equal byte-for-byte after every txid.
3. **Reader stability**: pinned reads byte-stable across concurrent commits and
   sweeps; cursors drain fully under a continuous write load.
4. **Sweep safety**: a shadow reachability model asserts no freed page is
   reachable from any durable, current, in-flight or pinned root.
5. **Cross-shard**: the existing convergence fuzz, plus no-acked-edge-lost,
   delete-vs-create races, and the L2 late-add case as a regression.
6. **Crash harness**: prefix replay over the event clock of *several*
   processes (extends the r3 single-writer harness).
7. **Measurement**: commits/s and p50/p99 commit latency vs writer count
   (1…32) on the 500/5K/50K/500K ladder; CAS retry rate; rebase cost.

## 10. Open questions for Lev

- **Q1 Import conflicts.** (a) The observation union exceeds 2 for some
  entity: proposal — keep the 2 newest by `obsMtime` and move the rest onto
  a linked `…__import_overflow` node, listed in an import report. Nothing
  dropped silently. (b) v3 has no tombstones, so an entity deleted on one
  machine but alive on another comes back from the union. Accept it, or use
  per-file mtimes / backups to infer deletions? (c) Same name, different
  `entityType` across files: which wins?
- **Q2 Process model across hosts.** CONTROL needs a local filesystem, so a
  store's writers are processes on its host; remote machines go through that
  host's daemon. OK?
- **Q3 Read-then-write transactions** (e.g. a future TRAVERSE + write): they
  would need read-set validation (SSI-style). Defer until such an op exists?
- **Q4 One commit-order word per store.** If measurement ever shows the CAS
  binding, single-segment commits could get per-segment order words. Proposal:
  measure first (gate 7).
- **Q5 Durability modes.** Keep r3's `SAFE_NOSYNC` (fsync on cadence) as an
  option, or durable-only?

## 11. Build order (each step gated)

1. Txn handles in libsegstore (S9); single-process behaviour unchanged; the
   existing battery stays green.
2. CONTROL + attach / cold start + proc slots.
3. ROOTS replacing dual-meta and MANIFEST; recovery; crash harness (gate 6).
4. CAS publish + helping + rebase; multi-thread, then multi-process fuzz
   (gates 1, 2).
5. Sweep reclamation replacing the freelist (gate 4).
6. graph4 on txn handles: each MCP write op as request-evaluation, ledgers
   computed at build.
7. Counter deltas, strings-as-bytes, id blocks.
8. Leases Phase B on CONTROL pins (gate 3).
9. Layer 2: dots + causal context, `adj_wm` removed, repl codecs updated
   (gate 5).
10. Measurement (gate 7).
11. Merge-all import (after Q1), cutover.

## References

- P. Bailis, A. Fekete, M. J. Franklin, A. Ghodsi, J. M. Hellerstein,
  I. Stoica. *Coordination Avoidance in Database Systems.* PVLDB 8(3), 2014.
- A. Bieniusa, M. Zawirski, N. Preguiça, M. Shapiro, C. Baquero, V. Balegas,
  S. Duarte. *An Optimized Conflict-free Replicated Set.* arXiv:1210.3368, 2012.
- M. Shapiro, N. Preguiça, C. Baquero, M. Zawirski. *Conflict-free Replicated
  Data Types.* SSS 2011.
- J. E. Gonzalez, Y. Low, H. Gu, D. Bickson, C. Guestrin. *PowerGraph:
  Distributed Graph-Parallel Computation on Natural Graphs.* OSDI 2012.
- M. Herlihy. *Wait-Free Synchronization.* TOPLAS 13(1), 1991.
- L. Yang, Y. Gilad, M. Alizadeh. *Practical Rateless Set Reconciliation.*
  SIGCOMM 2024.
