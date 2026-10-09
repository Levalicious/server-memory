# V4 architectural reconciliation — 2026-10-09

Purpose (Lev): connect **what exists** to **what we were aiming for**, axis by
axis, with receipts. Every cell cites its source: KB node, spec section,
`file:line`, or commit. Where a cell is inferred rather than recorded, it says
so. Status: REVIEW DRAFT — nothing in the repo changes based on this document
until Lev has read it.

Method: content walk of the design record (FTI/LSM era → COW pivot → minimal
core → freeze → today) + code verification at `feat/libsegstore` `85e5ec8`
plus the uncommitted import tooling. Confidence is marked per row;
measurement-dependent cells are flagged.

---

## Verdict up front

- **7 of 12 axes: aligned.** Implementation tracks the spec, and the spec
  tracks the recorded intent: storage core, adjacency, strings, rank (+ one
  recorded semantic delta), ownership/concurrency, wire/daemon, shim.
- **1 axis broken: the name index.** The intent Lev remembers — persistent,
  incremental, no wholesale rehash (the FTI/Bε-tree/LSM/libmdbx lineage) —
  **never made it into the spec at all**. The implemented design is v3's
  approved-for-v3 hash carried forward through the COW pivot, with a hard
  directory cap v3 never had. It fails at ~183K entities against a live KB of
  ~245K.
- **1 axis with unbuilt arms the record called "day one": the shard seam.**
  Segment-qualified refs are in; the logical node-id indirection table and
  the wire shard-id space are not (a reserved page kind exists, unused).
- **1 deferred-open decision: trigram/type persistence.** In-memory
  owner-private was a recorded single-writer-gated choice, with "persisted =
  format change" explicitly left open. Never re-decided for v4.
- **1 axis in build, blocked on the index: v3→v4 import.**
- **The pivot that lost the index intent: the COW decision of 2026-07-01**
  ("keeps v3 adjacency layout; adds crash-atomicity"). After that date, no
  record re-addresses index structure; the frozen spec's total index content
  is two lines. Details in "How the index intent was lost" below.

---

## Per-axis reconciliation

| # | Axis | Intent (receipts) | Spec | Code | Status |
|---|------|-------------------|------|------|--------|
| 1 | Storage core (COW, durability, manifest) | `Decision_COWGraphStore_2026_07_01` (page-COW, atomic root swap, keep layouts); `Decision_LibmdbxTargetArch_2026_08_20` (SWMR, meta pivot, no WAL, freelist); `Decision_PagedStorePerSegment_2026_08_20` | §§2–5, FROZEN; crash-harness evidence cited inline (67+126 segfile, 132 txn, 184 global) | `seg_meta/page/io/file/txn/mstore.c` + harnesses + WP + ladder bench; `bench_segstore` reproduces spec numbers (verified 2026-10-08: 4K@500K amp 57 = spec) | **ALIGNED** |
| 2 | Name index | Persistent (Lev `Decision_PersistNameIndex_2026_06_25` — for **v3**: on-disk hash, rehash); persistent-incremental as the v4-era aim (FTI cluster Jan 2026: `FTI_vs_BTree`, `FTI_vs_LSM`, `BεTree`, `FractalTreeIndex`; LSM era `Decision_GraphNativeLSM_2026_06_29`: levels/sorted runs) | **Silent.** Only §10.3 "port minimal-core indexes: trigram, type, bidir BFS" — the name index is not designed anywhere in the spec | `seg_graph4.c` `ni_*`: paged bucket-hash, 70% load, **whole-index doubling rehash** (`ni_rehash`, ~:209), `NI_DIR_MAX=1017` cap (`:21–23`); v3 `graph.c` & bench branch: same rehash, in-file, no cap | **BROKEN.** Cliff ~183K (`Finding_NiDirCapCliff_2026_10_09`; import evidence). Rehash itself was v3-approved; carrying it past the point of replacement was never an explicit decision |
| 3 | Trigram / search index | `Insight_TrigramIsSingleWriterFeature_2026_07_14`: in-memory **or** persisted=format change — single-writer-gated; `Finding_WriteDecoupled_2026_07_20` + `Plan_IncrementalSync_2026_07_20` + `Plan_HashSetPostings_2026_07_20`: decoupled, lazy, O(1) maintenance, O(delta) sync at search | §10.3 port list | `seg_graph4.c`: live trigram + dirty-set, owner-private **in-memory**, rebuilt lazily; regex engine (C, DFA/NFA, Sheng) | **DEFERRED-OPEN.** In-memory is a recorded shape, but the persist-vs-rebuild endpoint was never closed for v4. Rebuild cost at 245K: **unmeasured** |
| 4 | Type index | `Finding_HashSetPostings_2026_07_20`: O(1) open-addr postings | §10.3 port list | `seg_graph4.c` `tidx`: open-addr postings, lazy, in-memory | **DEFERRED-OPEN** (same family as #3) |
| 5 | Adjacency / edges | `BGS_BidirStorage`; `Decision_COWGraphStore` "keeps v3 adjacency layout"; community locality = mitigation (`Design_CommunityPageLocality_2026_07_01`) | §8 locality (mitigation); §2 extents | Chained ADJ pages + extent spill (`SEG_KIND_ADJ/EXTENT`), bidirectional mirrors, parity-verified | **ALIGNED** (locality mitigation unbuilt = §10.5 measurement, by plan) |
| 6 | Strings | `Lesson_RefcountDiscipline_MemfileV3`; segmented strings (`Goal`/"store deletes per-client…) | §1 `strings-NN.kb`, §2 mechanics | `st4`: slotted refcounted records; intern map rebuilt at open by full scan (`seg_str4.c:116`) | **ALIGNED**; open-scan cost at 128 MB: **unmeasured** |
| 7 | Rank (visits, ψ) | `Milestone_V3Ranking_2026_06_25`; `Design_RankAmortized_2026_08_20`; `PR_PowerIteration`, `PR_ErrorInverselyProportionalToRank` | §7 | Per-entity visits+ψ persisted; totals **recomputed at open, memory-held** (`seg_graph4.c:39`); daemon background converged slices | **ALIGNED + recorded delta:** v3 persisted totals incl. deleted-entity visits; v4 cannot carry orphans (`Finding_MigratorStatus_2026_10_08`). Orphaned visits in live KB: 43.9M/0.37M |
| 8 | Ownership / concurrency | `Diag_MemfileSingleOwner_2026_06_29`, `Decision_ExternalizeViaCDaemon_2026_08_20`; §9 deletions list | §§1, 9 | kbd4: one owner, TCP+token, no flocks/WAL/refresh/per-client processes | **ALIGNED** |
| 9 | Wire / daemon | §6 framing+algebra intent; continuations as protocol objects (`Design_PartialTraversalProtocol_2026_08_23`) | §6 (frozen header; algebra) | v1.5: presets + β-contract + leases/continuations (`85e5ec8`); PRESETS = TRAVERSE shorthands; generalized verbs = shard-era by §6.2 note | **ALIGNED** |
| 10 | Shim / MCP surface | `Plan_V3ServerRewire_OpsInC`: JS keeps pagination/sort/format/NL-guard; `Decision_V4CutoverShape_2026_10_08`: C server + JS client channel | §10.4 | `server.ts` async over GraphBackend (daemon opt-in); embedded fallback retires at cutover | **PARTIAL-BY-PLAN** (default flip + v3→v4 import pending) |
| 11 | Shard seam | `Design_ShardSeamFirst_2026_08_20`: "keep segments as tx/placement domain, **logical node-id indirection (v4 Q3b)**, shard-id space in refs + wire protocol **from day one**"; `Design_ShardOpClasses_2026_08_23` | §6.4 (states refs segment-qualified + per-segment indirection table + op classes) | Segment-qualified refs ✓ (`seg_ref` u16 seg); `SEG_KIND_INDIRECT=4u` **reserved, zero uses** (`segstore.h:149`); no logical node-id indirection table; no shard-id space on the wire (comment-only); op classes doc-only | **PARTIAL — two "day one" arms missing.** Implementation waits (accepted); the SEAM was not supposed to |
| 12 | Migration / import | §10.4 "one-shot v3 import"; `Decision_V4ImportShape_2026_10_08` | §10.4 (one line) | `native/v4_import.c` (uncommitted): offline C, copy-not-compare, preserves per-entity payload; **blocked on #2** — it tripped the cliff at 183K/245K | **IN BUILD, blocked on #2** |

---

## How the index intent was lost — receipt chain

1. **Jan 2026** — persistent index structures analyzed in depth (`FTI_vs_BTree`,
   `FTI_vs_LSM`, `BεTree`, `FractalTreeIndex` + its write-buffer/ACID arms).
   The vocabulary of this era is what Lev is remembering.
2. **2026-06-25** — `Decision_PersistNameIndex_2026_06_25` (Lev): persist the
   name→offset index for **v3**. The v3 shape — bucket hash, open addressing,
   backward-shift delete, **rehash** — was Lev's own approved call *for that
   store*.
3. **2026-06-29** — `Decision_GraphNativeLSM_2026_06_29`: graph-native LSM
   (levels, persistent sorted runs). This is the "leveldb/lmdb debate" era
   decision.
4. **2026-07-01 — THE PIVOT.** `Decision_COWGraphStore` supersedes LSM. Its
   operative clause: *"Keeps v3 adjacency layout; adds crash-atomicity."*
   Index structures are not mentioned. From this date forward, **no record
   re-addresses index architecture.**
5. **2026-07-13** — `Q_LevelDBPerf_KVConversion` (Lev asked: leveldb-like
   perf?): resolution says *"NO KV conversion. v4 spec already extracts LSM
   write-side wins"* — write-side wins (group commit, COW) ≠ index structure;
   the structure content of the debate was hereby narrowed out of scope, in
   writing but without surfacing that anything was dropped.
6. **2026-07-13..20** — minimal core optimizes the carried designs
   (`Fix_LogIndexStamp`, `Finding_HashSetPostings`, `Finding_WriteDecoupled`,
   `Plan_IncrementalSync`). All internally consistent; nothing revisits the
   index *shape*.
7. **2026-08-20** — `Decision_LibmdbxTargetArch`: SWMR/page-COW/meta/no-WAL/
   freelist. Mechanics only; still nothing on index structure.
8. **2026-08-23** — spec r3 freeze. Index content of the frozen document:
   §6.4 (sharding, future) and §10.3 ("port minimal-core indexes"). The
   carried-over hash designs are thereby codified as the plan by omission.
9. **2026-10-08/09** — the real-KB import (245K entities) meets the carried
   design's new hard cap at 183K. First visible break.

**Conclusion:** the persistent-incremental index was not "deviated from" by
the recent work, and it was not implemented — it was **dropped by omission at
the Jul-1 pivot** and never re-opened; every later artifact is consistent
with the narrowed scope. The recent work's failure was reporting the carried
designs as completion of the wider intent (recorded:
`Violation_IndexPortOverclaim_2026_10_09`).

---

## Open decisions this reconciliation creates (for Lev)

1. **Index redesign (#2) — the gate for import/cutover.** Options space:
   B+tree-style paged index over `seg_txn` primitives (libmdbx-consistent);
   other incremental structures (e.g. linear hashing — incremental splits, no
   wholesale rehash, simpler than a B-tree but still a hash); or something
   else Lev directs. Requires its own design note before code.
2. **Trigram/type persistence (#3/#4).** Decide explicitly: persist (format
   addition + open-path read) vs keep owner-private in-memory — after
   measuring rebuild at 245K. The record left this open.
3. **Shard seam arms (#11).** Spec §6.4 says indirection + wire shard space
   were "day one"; code has neither. Either build the seam arms now, or amend
   the spec to defer them explicitly. No silent middle.
4. **Rank totals (#7).** Accept v4's recompute-at-open semantics (orphan
   visits untransferable, documented) or persist totals.
5. **Sequencing.** Proposed: (1) → (2 decision) → (3 decision) → import
   unblocks → cutover. Nothing executes before each design note passes Lev.

---

## Verification notes

- Aligned cells #1/#5/#8 are additionally evidenced today by the green
  battery: parity (30 rounds), crash harnesses, `bench_segstore` reproducing
  the frozen ladder numbers.
- Cells flagged **unmeasured** (#3 rebuild at 245K; #6 open scan at 128 MB)
  should be measured as part of the audit follow-up — both are on the
  daemon's open/steady-state path.
- The name-index cell (#2) is evidenced by the actual failed import run:
  245,253 source entities; creates fail from ~182.4K; `Finding_NiDirCapCliff_2026_10_09`.

---

## Rulings (Lev, 2026-10-09) — recorded post-review

1. **Index redesign: libmdbx-style** (paged, incremental; scalability/perf).
2. **Trigram/type persistence: yes** — "at worst a couple extra tables";
   folded into the #1 rework (the table abstraction arrives with it).
3. **Shard seam: explicit defer** — spec §6.4 amended (r3.2); retained arms
   named (segments as tx/placement domain, segment-qualified refs,
   `SEG_KIND_INDIRECT` as the future hook).
4. **Rank totals: persist verbatim (option i)** — rationale: rank values stay
   a property of the STORE, stable across clients/restarts (and comparable
   against a v3 rollback). Folds into the index note's root-record design.

These rulings are recorded in `Decision_Lev_ReconciliationRulings_2026_10_09`.

