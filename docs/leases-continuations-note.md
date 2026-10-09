# kbd4 leases & continuations — design note v0 (for review)

Status: AGREED 2026-10-08 (Phase A for v4.0; TTL 10 min / cap 256; wire
v1.5) and IMPLEMENTED — spec §4/§6.2 carry the r3.1 addenda. Kept as the
rationale trail. Plan item (2) of Plan_V4Convergence.

## Why

Spec §4: "Leases (owner-internal, bounded) pin snapshots for cursors;
expired cursors re-resolve and say so." Spec §6.2: TRAVERSE/RESUME
continuations, tokens carrying the store txid; v4.0: "continuations
surface only on budget exhaustion."

First real consumer today: `find_path`'s β-contract — an exhausted budget
currently returns only the farthest NODE (a re-anchor). A continuation
token upgrades that to an exact resume (continue the same BFS) while the
anchor stays the re-resolve path. The same object later carries TRAVERSE
continuations; nothing below is find_path-specific.

## Object model

- Lease: `{ id: u64, store_txid, created_ms, last_use_ms, payload }`
  - `payload` = op-specific continuation state (find_path: frontier eids +
    depths + budget spent + direction/maxDepth echo).
- Wire token = the lease id (opaque u64; 0 = none). Clients never mint or
  embed state; ids are validated against the table. Sequential monotonic
  ids are fine under the LAN + bearer-token threat model.
- RAM-only. A kbd4 restart kills all tokens; clients re-anchor from the
  farthest name they already hold (the β-contract always gives them one).

## Bounds (the anti-leak clause)

- `KBD_LEASE_TTL_MS` (default 600000 = 10 min, idle-based, refreshed on use).
- `KBD_LEASE_MAX` (default 256).
- Sweep on the idle poll tick (same slice loop as background rank).
- At cap: evict expired first; if still at cap, DON'T issue — the op
  returns token=0 and the anchor, exactly as today. **Leasing is an
  optimization; its absence never fails an operation.**
- Pin sharing: pins are per distinct `store_txid`, refcounted by leases —
  N cursors on one snapshot share one pin. Memory ∝ distinct live
  snapshots, not cursor count.

## Pinning (phases)

- **Phase A — validate-or-stale (no snapshot views).** RESUME checks
  `mstore_txid == lease.store_txid`; mismatch or expiry is an explicit
  error (TOKEN_STALE / TOKEN_EXPIRED, details carry the anchor name).
  "Re-resolve and say so" is satisfied by the client re-anchoring from the
  farthest it already has. Zero new core machinery.
- **Phase B — pinned reads** (when pagination/cursors move daemon-side or
  sharding lands): hold `seg_pin` objects (`seg_txn.c:253` — the §3
  `{txid, ptable copy}` mechanism, already validated through 20-commit
  churn) for the lease's txid, shared per txid via refcount; continuation
  reads run against the pinned ptables. Note the interaction: live pins
  gate freelist promotion (`min_pin_txid`, `seg_txn.c:75`), so pin
  lifetime defers page reclamation too — the TTL/cap bounds that as well.

## Wire (v1.5)

- `OP_FIND_PATH` reply appends `u64 continuation` (0 = none). Issued only
  on budget exhaustion (§6.2 v4.0 rule); depth-limited misses keep the
  anchor-only β-contract.
- `OP_RESUME = 0x2e` `{ u64 token }` → same reply shape as the origin op
  (shared parsing), with an explicit error payload on failure:
  `[u8 code][str message]`, codes TOKEN_EXPIRED=1, TOKEN_STALE=2,
  TOKEN_UNKNOWN=3.
- The MCP shim keeps today's surface (farthestDiscovered + note); tokens
  are wire substrate it may ignore until a consumer appears.

## Non-goals

- Cross-host resume (shard seam; §6.4 unchanged).
- Persistent leases across daemon restarts.
- Lease listing / admin API.

## Open decisions (Lev)

- (a) v4.0 = Phase A only, or straight to Phase B pins?
- (b) TTL / cap defaults (10 min / 256).
- (c) Wire the RESUME op + tokens in v1.5 now (substrate, with tests and
  the shim ignoring them), or keep only the lease table until the first
  real consumer?
