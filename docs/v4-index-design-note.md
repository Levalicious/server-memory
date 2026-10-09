# V4 index layer redesign — design note (libmdbx-style)

Status: DRAFT for Lev's veto (no code until approved). Implements ruling 1+2
of `Decision_Lev_ReconciliationRulings_2026_10_09`; grounded in
`docs/v4-reconciliation.md`. Replaces the carried-over v3 hash designs
(name index, in-memory trigram/type postings) with one persistent,
incrementally-growing, COW-native **table/tree** mechanism — the structure
the Jul-1-era drift dropped and this reconnects.

## 1. Why (one paragraph)

The reconciliation showed the index layer is the one axis where V4's intent
was never carried: the name index is v3's bucket hash (wholesale doubling
rehash, a hard 1017-page cap, cliff at ~183K vs a 245K live KB), and the
trigram/type postings are in-memory rebuild-lazy workarounds for a problem
(array-postings) that a proper structure does not have. This note designs
the replacement: **one generic B+tree over slotted pages**, instantiated as
the catalog, name index, trigram postings, and type postings. Strings (st4)
stay as-is (Lev, 2026-10-09); rank totals become persisted store state
(ruling 4-i).

## 2. Table model

- A **table** is a B+tree whose root ref lives in the catalog; one tree
  module (`seg_tree`) parameterized by: key codec, value codec, and
  comparator (always bytewise `memcmp` over the encoded key — LMDB
  semantics; all keys are encoded so byte order IS the semantic order).
- The **catalog** is itself a tree instance whose root occupies the graph
  segment's existing `nameindex_root` slot (`segstore_nameindex_root` /
  `seg_txn_set_roots` root #1; rename to `catalog_root`). Keys:
  `0x00 META`, `0x01 T:name`, `0x02 T:tri`, `0x03 T:type`. The `indirect_root`
  (root #2) stays reserved/zero — the explicitly deferred shard-seam hook.
- `META` value: `{format_version, structural_total, walker_total,
  ent_count, rel_count}` — totals persisted per ruling 4-i, so ranks are a
  property of the store (stable across restarts and clients), and the graph
  layer opens in O(1) (one page read) instead of scanning.
- Tree roots stored per-table as small values in the catalog; a root split
  just rewrites that catalog value (ordinary tree write, COW).

## 3. Node format (inside existing slotted pages)

- Tree pages reuse the frozen slotted-page mechanics verbatim: one page per
  node, records in slots, `slot ids stable across compact`,
  `seg_page_compact` already runs on every COW touch. No new page format —
  the tree rides `seg_page` as-is. `SEG_KIND_NAMEIDX(3)` is renamed
  **`SEG_KIND_TREE`** (pre-release kind, no stores to migrate); page flags
  hold `LEAF|BRANCH` (+ reserve one for root, LMDB-style).
- **Entries are always stored in key order in the page.** Every tree
  mutation rewrites the node's image in order: read the live entries (in
  order), merge/remove the changed one, write the new ordered image into
  the COW-touched page; split if it no longer fits. This is the COW
  philosophy applied to node contents — the tree never mutates a published
  page, and no new page primitives are needed.
- Entry codecs per tree:
  - name: `[u16 klen][name bytes][u32 eid]`
  - tri:   `[3B trigram][u32 eid]` (fixed 7B, no value — one entry per
    posting; membership = point lookup; posting *list* = prefix range scan)
  - type:  `[u32 type_sid][u32 eid]` (fixed 8B, no value)
  - catalog: `[u8 tag][u8 name...]` → small value
- Branch entries: `[u64 seg_ref (child)][subtree-first-key]`; descent =
  last child whose first-key ≤ k. Node refs = the existing `seg_ref_t`
  (seg|pgno|slot in a u64; `SEG_REF_NULL` = 0 is already guaranteed safe).
- **Growth = incremental splits only.** Leaf overflow → split at median into
  two ordered images; parent overflow likewise; root split adds height.
  No wholesale anything: every insert touches O(log_B N) pages, allocates
  on splits, all inside the caller's txn. The 1017-page cap and the
  doubling rehash die with the old structure (capacity = address space).
- **Delete**: remove entry (rewrite page in order). Underflow (< 25% bytes):
  try a single merge with an adjacent sibling when the pair fits the page —
  one pass, no borrowing. Underfull-but-unmergeable pages are left sparse
  (correct, just not dense); reclaimable later if measurement ever asks.
  Deletions are rare in the KB; correctness-first, simplicity kept.
- **eid stability**: leaf values are graph eids; eids are (lpg,slot) and
  LPG-stable thanks to the ptable indirection — tree page moves never touch
  them. Leases/continuations (replay descriptors over eids) are unaffected.

## 4. Table set

1. **catalog** (root #1) — META + table roots, as above.
2. **name** — name → eid. Serves `g4_lookup` and enumeration. Enumeration
   order becomes **sorted by name** (was hash-bucket order — nothing
   depends on the old order; the MCP layer paginates with explicit sorts +
   fingerprints).
3. **tri** — trigram postings (persistent). Query evaluation keeps the
   proven policy: express AND/OR, **size-sort operands, probe the smallest
   posting set into the rest** — identical algorithm to the current
   in-memory version, but the "sets" are table range-scans/point lookups.
4. **type** — type_sid postings (persistent); `entities_by_type` = prefix
   range scan.
5. **strings: st4 unchanged** (Lev-ruled). st4 keeps refcounted intern
   semantics; its open remains a full scan — **measured** as a follow-up
   (flagged in the reconciliation); a persistent st4 directory would be a
   separate small note if the number says so.

## 5. Write path and maintenance policy

- All index updates happen **immediately, inside the writer's existing
  txn** — the same mstore txn as the entity/edge writes. One commit, one
  consistency story: no stale-index windows, no sync bookkeeping, no
  "rebuild lazily at search" paths left in the codebase.
- This **deletes** the entire decoupling apparatus: `tri_mark` dirty sets,
  `g4_index_sync`, `tidx` lazy builds, the in-memory postings
  (HashSetPostings-era machinery becomes historical: its real lesson was
  "postings must be O(log)-mutable per op" — a B+tree does that natively;
  the array-postings write-kill that motivated decoupling simply cannot
  occur in the new structure).
- Cost honesty: every entity write now touches name + tri (#ngrams in
  fields) + type trees → more pages per txn → write amplification rises
  vs the decoupled peak. This is the libmdbx trade (chosen, ruling 1) and
  is **measured, not assumed**: op_bench before/after with an explicit
  review gate (reads/search must not regress; writes' new steady-state
  reviewed by Lev before sign-off). If measurement demands it, deferred
  sync is the fallback — but only on evidence.

## 6. Consequences / migration

- Deleted: `ni_*` (dir/buckets/rehash), `tri_*` live/dirty machinery,
  `tidx_*`. Kept: st4, adjacency chains, leases, wire, daemon.
- `graph4_open` reads catalog + table roots only; `g4_set_totals` now writes
  through to `META` (durable); the recompute-at-open code is removed.
- `native/v4_import.c` retargets: per-entity payload copy logic survives
  unchanged; index construction becomes tree inserts. The 245K-entity live
  KB import is the acceptance test (plus headroom beyond, since capacity
  caps are gone).
- Format: node format + catalog/META layout become a new normative spec
  subsection (r3.3) once built; stores are pre-release (fresh-only +
  import).
- Tests/gates:
  - `test_segtree`: insert/delete/split fuzz vs an in-memory ordered-model
    (mirrors the segpage fuzz-vs-model pattern); ordered-iterator check.
  - Crash safety: tree writes ride the frozen txn commit ordering; extend
    the existing prefix-replay harness coverage to split-heavy txns
    (existing txn harness composes; no new commit machinery).
  - Parity battery unchanged for behavior; add enumeration-order + lookup
    patches.
  - Acceptance: live-KB import 245K/393K with full validation;
    op_bench before/after reviewed by Lev.

## 7. Non-goals

- The deferred seam arms (ruling 3): logical node-id indirection, wire
  shard-id space — untouched, named deferral stands.
- st4 rework (flagged as separate measurement-driven follow-up).
- Concurrent writers, tree-level locking, page-level rebalancing beyond §3.
- The old v3 stored hashes are NOT migrated in place — v4 stores are
  fresh-only + import (pre-release).

## 8. Open decisions for Lev (small, vetoable)

1. Deletion policy: §3 (remove + single-pass sibling merge, leave sparse
   otherwise) vs full borrow/merge now. Recommendation: as written.
2. Update policy: immediate-in-txn (§5) confirmed, with the op_bench gate —
   yes/no.
3. Kind rename `NAMEIDX(3) → TREE` + the r3.3 normative subsection once
   built — yes/no.
