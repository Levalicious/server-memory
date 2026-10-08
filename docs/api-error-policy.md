# API Error & Result Policy

Status: **adopted 2026-10-08** (implementation ongoing — see [Status](#status)).

This document is the contract for how every MCP tool reports failure and
success. It replaces the original "never fail, funnel the caller into
unfailing completion" approach: with RL-era models, errors are signal — a
silent no-op or a hidden protocol error teaches the caller nothing and lets
wrong beliefs persist (KB: `BGS_API_ErrorInconsistency`,
`Inventory_APISilentDrops_2026_10_08`).

## Rules

**R1 — Visible failures.**
Every failure the caller can act on is returned as a tool-level error:
`isError: true` with a JSON envelope body:

```json
{
  "error": {
    "tool": "create_relations",
    "code": "ENDPOINT_MISSING",
    "message": "2 relation(s) reference nonexistent entities; nothing was created (details below).",
    "details": [
      { "from": "A", "to": "Ghost", "relationType": "USES", "code": "ENDPOINT_MISSING",
        "missing": ["to"], "message": "endpoint(s) not found: to" }
    ]
  }
}
```

Stable codes: `ENTITY_NOT_FOUND`, `TYPE_NOT_FOUND`, `ENDPOINT_MISSING`,
`COLLISION`, `LIMIT_EXCEEDED`, `INVALID_REGEX`, `INVALID_FILE`,
`NO_MATCHES`, `VALIDATION_FAILED`. JSON-RPC protocol errors are reserved for
genuine server faults and are never used for caller-actionable failures.

**R2 — Validate-first, atomic.**
A mutation validates the entire request under the write lock before touching
state, and is all-or-nothing: a batch containing a failing item changes
nothing, and the error lists every offending item in one round-trip.

**R3 — Per-item ledger.**
Every mutating success enumerates per-item outcomes
(`created` / `existing` / `skippedDuplicates` / `notFound` / `deleted` /
`added` / `alreadyPresent`). There is no bare "successfully" response.

**R4 — No silent drops.**
An item that cannot be satisfied errors (R1) unless the caller's intent is
provably satisfied — duplicates are "already there", deleting an absent
thing is "already absent". Those cases succeed **and are reported** (R3).

**R5 — Pagination integrity.**
Cursors identify the result set they came from (a fingerprint now; a store
txid once the owner daemon lands). A stale/mismatched cursor returns a
`CURSOR_STALE` error instead of a silently wrong page.

## Per-op policy

| Tool | On failure | On success |
|---|---|---|
| `create_entities` | same-name-different-data → `COLLISION` (atomic, all offenders); limit breaches → `LIMIT_EXCEEDED` | `{created, existing}` — exact duplicates are success |
| `create_relations` | any endpoint missing → `ENDPOINT_MISSING` (atomic, all offenders) | `{created, skippedDuplicates}` |
| `add_observations` | unknown entity → `ENTITY_NOT_FOUND`; limit breaches → `LIMIT_EXCEEDED` (atomic, all offenders) | per-entity `{entityName, addedObservations, alreadyPresent}` |
| `delete_entities` / `delete_relations` / `delete_observations` | nothing errors — absence is `notFound` (R4) | `{deleted, notFound}` ledgers |
| `sequentialthinking` | unknown `previousCtxId` → `ENTITY_NOT_FOUND` (chain integrity); limits → `LIMIT_EXCEEDED` | `{ctxId, linkedTo}` |
| `kb_load` | bad extension / unreadable file → `INVALID_FILE`; entity & relation stages per their rows | per-stage ledger (document, stats, entity/relation outcomes) |
| `open_nodes` | ALL requested names missing → `ENTITY_NOT_FOUND` | graph result + `missing[]` for partial misses |
| `get_neighbors` | unknown start → `ENTITY_NOT_FOUND` | neighbors (depth 0 = immediate) |
| `search_nodes` | invalid ERE → `INVALID_REGEX`; literal query, zero matches → `NO_MATCHES` | paginated graph |
| `get_entities_by_type` | type absent from the data-derived type set → `TYPE_NOT_FOUND` | entities of that type |
| `random_walk` | unknown start → `ENTITY_NOT_FOUND` | `{entity, path, modeUsed, stopReason}` |
| `find_path` | — (β-contract already reports `targetReached` / `budgetExhausted` / `note`) | unchanged — the reference pattern for R1-style reporting |
| reads (`get_entity_types`, `get_relation_types`, `get_stats`, `get_orphaned_entities`, `validate_graph`, `decode_timestamp`) | unchanged | unchanged |

Decisions of record (Lev, 2026-10-08): duplicates are ledger reports, not
errors; the unified surface is `isError` with protocol errors reserved for
faults; Q1 = atomic-fail on missing endpoints; Q2/Q3 = error.

## Status

- **This change (envelope PR):** R1 (envelope + codes + dispatch conversion),
  R2 (validate-first atomicity for `create_entities`, `create_relations`,
  `add_observations`, `sequentialthinking`), R4 (drops→errors:
  `ENDPOINT_MISSING`, all-missing `open_nodes`, `TYPE_NOT_FOUND`, unknown
  starts), `INVALID_FILE`, `INVALID_REGEX`, `NO_MATCHES` envelope.
- **Ledger PR (next):** R3 result ledgers + report fields (`missing[]`,
  `modeUsed`, `alreadyPresent`), R5 cursor fingerprinting.
- **v4 line (`feat/libsegstore`):** inherits this policy at reconciliation;
  kbd4's batch=txn=commit model is the natural home for R2 atomicity.
