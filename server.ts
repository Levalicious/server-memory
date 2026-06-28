#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { context, propagation, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { randomBytes } from 'crypto';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { Store, DIR_FORWARD, DIR_BACKWARD, type NativeEntity } from './src/store.js';
import { ensureV3 } from './src/migrate.js';
import { validateExtension, loadDocument, type KbLoadResult } from './src/kb_load.js';
import { toolDurationHistogram, traced, tracer } from './src/tracing.js';

/**
 * Result envelope for a single tool dispatch. Mirrors the MCP CallToolResult
 * shape we return from every case; `isError: true` signals a tool-level error
 * that the LLM should see (vs. a thrown error which becomes a hidden JSON-RPC
 * protocol error).
 */
type ToolDispatchResult = {
  content: Array<{ type: 'text'; text: string }>;
  isError?: boolean;
};

/**
 * True if the query contains any regex metacharacter. Used to gate the
 * `search_nodes` natural-language warning: a pure literal that returns no
 * matches is the signal we want to flag, since regex queries (anchors,
 * quantifiers, alternation, classes, etc.) are deliberate and may legitimately
 * match nothing.
 */
const HAS_REGEX_META = /[\\^$.*+?()[\]{}|]/;

// =============================================================================
// find_path memory budget
//
// `findPath` runs a BFS whose state size scales with reachable-within-maxDepth.
// To keep it bounded as the KB grows (or as adversarial maxDepth values come
// in), we cap the BFS at a real byte budget derived from the host's available
// RAM at call time. Hard upper bound by request: never claim more than 80%
// of available memory.
//
// The budget VALUE is `0.80 * MemAvailable` (or the `KB_FIND_PATH_BUDGET_BYTES`
// env override, used by tests to force exhaustion). It is passed to the C BFS,
// which enforces it against its own per-node footprint (graph_find_path_ex).
// =============================================================================

const FIND_PATH_MEMORY_FRACTION = 0.80;

/**
 * Bytes of RAM the host considers "available" — i.e., free + reclaimable
 * cache, the right number for "how much can I grow into without forcing
 * swap." On Linux this is `MemAvailable` from /proc/meminfo, which is the
 * kernel's own answer. On other platforms we fall back to `os.freemem()`,
 * which under-counts (it's `MemFree`, no reclaimable accounting) but is
 * portable. Under-counting is safe — we'll just budget less than we could.
 */
function availableMemoryBytes(): number {
  try {
    const text = fs.readFileSync('/proc/meminfo', 'utf-8');
    const m = /^MemAvailable:\s+(\d+)\s+kB/m.exec(text);
    if (m) return Number(m[1]) * 1024;
  } catch {
    // /proc/meminfo unavailable (macOS, Windows, weird container) — fall through.
  }
  return os.freemem();
}

/**
 * Byte budget for a single `findPath` BFS call. Either:
 *   - `KB_FIND_PATH_BUDGET_BYTES` env override (used by tests + ops), or
 *   - `0.80 * available_memory` from {@link availableMemoryBytes}.
 *
 * Floor of 1 byte to keep arithmetic well-defined; in practice
 * `availableMemoryBytes` is at least tens of MB on any host that can
 * actually run V8.
 */
function findPathBudgetBytes(): number {
  const override = Number(process.env.KB_FIND_PATH_BUDGET_BYTES);
  if (Number.isFinite(override) && override >= 0) {
    return Math.floor(override);
  }
  const bytes = availableMemoryBytes();
  if (!Number.isFinite(bytes) || bytes <= 0) return 1;
  return Math.max(1, Math.floor(bytes * FIND_PATH_MEMORY_FRACTION));
}

// Define memory file path using environment variable with fallback
const defaultMemoryPath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'memory.json');

// If MEMORY_FILE_PATH is just a filename, put it in the same directory as the script
const DEFAULT_MEMORY_FILE_PATH = process.env.MEMORY_FILE_PATH
  ? path.isAbsolute(process.env.MEMORY_FILE_PATH)
    ? process.env.MEMORY_FILE_PATH
    : path.join(path.dirname(fileURLToPath(import.meta.url)), process.env.MEMORY_FILE_PATH)
  : defaultMemoryPath;

// We are storing our memory using entities, relations, and observations in a graph structure
export interface Entity {
  name: string;
  entityType: string;
  observations: string[];
  mtime?: number;        // General modification time (any change)
  obsMtime?: number;     // Observation-specific modification time
}

export interface Relation {
  from: string;
  to: string;
  relationType: string;
  mtime?: number;
}

export interface KnowledgeGraph {
  entities: Entity[];
  relations: Relation[];
}

export interface PaginatedResult<T> {
  items: T[];
  nextCursor: number | null;
  totalCount: number;
}

export type EntitySortField = "mtime" | "obsMtime" | "name" | "pagerank" | "llmrank";
export type SortDirection = "asc" | "desc";

/**
 * Random-walk transition policy.
 *
 *   'merw'    — Maximum-Entropy Random Walk: probability of stepping to
 *               neighbor j is proportional to ψ_j (the cached MERW
 *               eigenvector entry), which biases the walk toward
 *               structurally important nodes. This is the default and
 *               matches the historical behavior. Falls back to uniform
 *               if ψ has not been computed yet (all zeros).
 *   'uniform' — Plain uniform random walk: every eligible neighbor is
 *               equally likely. Useful for unbiased structural sampling
 *               or for comparing against MERW.
 */
export type RandomWalkMode = "merw" | "uniform";

export interface Neighbor {
  name: string;
  mtime?: number;
  obsMtime?: number;
}

/**
 * Sort entities by the specified field and direction.
 * Returns a new array (does not mutate input).
 * Defaults to 'llmrank' when sortBy is undefined.
 *
 * For 'pagerank' sort: uses structural rank (desc by default).
 * For 'llmrank' sort: uses walker rank, falls back to structural rank on tie, then random.
 * Both rank sorts require rankMaps parameter.
 */
function sortEntities(
  entities: Entity[],
  sortBy: EntitySortField = "llmrank",
  sortDir?: SortDirection,
  rankMaps?: { structural: Map<string, number>; walker: Map<string, number> }
): Entity[] {
  const dir = sortDir ?? (sortBy === "name" ? "asc" : "desc");
  const mult = dir === "asc" ? 1 : -1;

  return [...entities].sort((a, b) => {
    if (sortBy === "name") {
      return mult * a.name.localeCompare(b.name);
    }
    if (sortBy === "pagerank") {
      const aRank = rankMaps?.structural.get(a.name) ?? 0;
      const bRank = rankMaps?.structural.get(b.name) ?? 0;
      const diff = aRank - bRank;
      if (diff !== 0) return mult * diff;
      return Math.random() - 0.5; // random tiebreak
    }
    if (sortBy === "llmrank") {
      // Primary: walker rank
      const aWalker = rankMaps?.walker.get(a.name) ?? 0;
      const bWalker = rankMaps?.walker.get(b.name) ?? 0;
      const walkerDiff = aWalker - bWalker;
      if (walkerDiff !== 0) return mult * walkerDiff;
      // Fallback: structural rank
      const aStruct = rankMaps?.structural.get(a.name) ?? 0;
      const bStruct = rankMaps?.structural.get(b.name) ?? 0;
      const structDiff = aStruct - bStruct;
      if (structDiff !== 0) return mult * structDiff;
      // Final: random tiebreak
      return Math.random() - 0.5;
    }
    // For timestamps, treat undefined as 0 (oldest)
    const aVal = a[sortBy] ?? 0;
    const bVal = b[sortBy] ?? 0;
    return mult * (aVal - bVal);
  });
}

/**
 * Sort neighbors by the specified field and direction.
 * Defaults to 'llmrank' when sortBy is undefined.
 */
function sortNeighbors(
  neighbors: Neighbor[],
  sortBy: EntitySortField = "llmrank",
  sortDir?: SortDirection,
  rankMaps?: { structural: Map<string, number>; walker: Map<string, number> }
): Neighbor[] {
  const dir = sortDir ?? (sortBy === "name" ? "asc" : "desc");
  const mult = dir === "asc" ? 1 : -1;

  return [...neighbors].sort((a, b) => {
    if (sortBy === "name") {
      return mult * a.name.localeCompare(b.name);
    }
    if (sortBy === "pagerank") {
      const aRank = rankMaps?.structural.get(a.name) ?? 0;
      const bRank = rankMaps?.structural.get(b.name) ?? 0;
      const diff = aRank - bRank;
      if (diff !== 0) return mult * diff;
      return Math.random() - 0.5;
    }
    if (sortBy === "llmrank") {
      const aWalker = rankMaps?.walker.get(a.name) ?? 0;
      const bWalker = rankMaps?.walker.get(b.name) ?? 0;
      const walkerDiff = aWalker - bWalker;
      if (walkerDiff !== 0) return mult * walkerDiff;
      const aStruct = rankMaps?.structural.get(a.name) ?? 0;
      const bStruct = rankMaps?.structural.get(b.name) ?? 0;
      const structDiff = aStruct - bStruct;
      if (structDiff !== 0) return mult * structDiff;
      return Math.random() - 0.5;
    }
    const aVal = a[sortBy] ?? 0;
    const bVal = b[sortBy] ?? 0;
    return mult * (aVal - bVal);
  });
}

export const MAX_CHARS = 4096;

/**
 * Paginate `items` starting at `cursor`, producing a page no larger than
 * `maxChars` (best-effort).
 *
 * Forward-progress invariant: every call advances the cursor by at least one
 * item, even when that item's JSON exceeds `maxChars`. Without this guarantee,
 * an oversized item at position `cursor` would cause `nextCursor === cursor`,
 * which the model follows back into an identical call → infinite cycle. (See
 * the obsMtime-desc / long-name regression we hit in production.)
 *
 * The byte budget is a soft cap, not a hard one: an oversized lead item is
 * emitted alone, blowing past `maxChars` for that single page only. The
 * alternative (skipping the item, or returning an error) hides data from the
 * caller and makes downstream pagination inconsistent.
 */
function paginateItems<T>(items: T[], cursor: number = 0, maxChars: number = MAX_CHARS): PaginatedResult<T> {
  const result: T[] = [];
  let i = cursor;

  // Calculate overhead for wrapper: {"items":[],"nextCursor":null,"totalCount":123}
  const wrapperTemplate = { items: [] as T[], nextCursor: null as number | null, totalCount: items.length };
  const overhead = JSON.stringify(wrapperTemplate).length;
  let charCount = overhead;

  while (i < items.length) {
    const itemJson = JSON.stringify(items[i]);
    const addedChars = itemJson.length + (result.length > 0 ? 1 : 0); // +1 for comma

    if (charCount + addedChars > maxChars) {
      // Forward-progress guarantee: if we have NOTHING in the page yet, emit
      // this oversized item anyway and advance. Otherwise stop and let the
      // next page (which now has a fresh budget) handle it.
      if (result.length === 0) {
        result.push(items[i]);
        charCount += addedChars;
        i++;
      }
      break;
    }

    result.push(items[i]);
    charCount += addedChars;
    i++;
  }

  // Update nextCursor - recalculate if we stopped early (cursor digits may differ from null)
  const nextCursor = i < items.length ? i : null;

  return {
    items: result,
    nextCursor,
    totalCount: items.length
  };
}

function paginateGraph(graph: KnowledgeGraph, entityCursor: number = 0, relationCursor: number = 0): { entities: PaginatedResult<Entity>; relations: PaginatedResult<Relation> } {
  // Entities and relations have independent cursors, so paginate them
  // independently — each gets the full budget.  The caller already has
  // previously-returned pages and only needs the next page of whichever
  // section it is advancing.
  return {
    entities: paginateItems(graph.entities, entityCursor),
    relations: paginateItems(graph.relations, relationCursor),
  };
}

// The KnowledgeGraphManager class contains all operations to interact with the knowledge graph
export class KnowledgeGraphManager {
  private db: Store;

  constructor(memoryFilePath: string = DEFAULT_MEMORY_FILE_PATH) {
    // Derive binary file paths from the base path
    const dir = path.dirname(memoryFilePath);
    const base = path.basename(memoryFilePath, path.extname(memoryFilePath));
    const graphPath = path.join(dir, `${base}.graph`);
    const strPath = path.join(dir, `${base}.strings`);

    // Auto-migrate an old (v1/v2) KB to v3 in place before opening — the same
    // transparent-on-open contract the old code used for v1->v2. ensureV3 holds
    // a migration flock across detection AND migration, so concurrent startups
    // serialize (no empty-clobber, no double-migrate). The migrator and the old
    // addon load only if a migration actually fires.
    ensureV3(graphPath, strPath);

    // The C store opens both files and self-locks around its own init. It owns
    // the memfile, string table, and name index.
    this.db = new Store(graphPath, strPath);

    // Initial structural sampling + MERW under an exclusive lock (the C ops
    // mutate the graph file; withWriteLock syncs on exit).
    this.withWriteLock(() => {
      if (this.db.entityCount() > 0) {
        this.db.structuralSample(1, 0.85);
        this.db.computeMerwPsi(0.85, 200, 1e-8);
      }
    });
  }

  /**
   * Run the loadDocument pipeline under an exclusive (write) lock.
   *
   * `loadDocument` calls `st.intern(...)` for every word it sees, which
   * invokes `mf.alloc()` on the strings file whenever a new string is added.
   * That is a write to shared state, so we need an exclusive lock — a shared
   * (read) lock would let multiple processes hammer the strings allocator
   * simultaneously and corrupt its free list (manifesting as a SIGSEGV in
   * `memfile_alloc`). The lock now covers BOTH files (see
   * {@link GraphFile.lockExclusive}).
   *
   * The full pipeline is wrapped in a `kb.load_document` span; the inner
   * `loadDocument` emits per-stage child spans (normalize → chunking →
   * IDF → TextRank → assemble) so users can attribute time to specific
   * stages without spelunking flame graphs.
   */
  prepareDocumentLoad(text: string, title: string, topK: number): KbLoadResult {
    return traced(
      'kb.load_document',
      {
        'kb.load.title': title,
        'kb.load.input_chars': text.length,
        'kb.load.top_k': topK,
      },
      (span) => this.withWriteLock(() => {
        const result = loadDocument(text, title, this.corpusShim() as never, topK);
        span.setAttribute('kb.load.chunks', result.stats.chunks);
        span.setAttribute('kb.load.sentences', result.stats.sentences);
        span.setAttribute('kb.load.unique_words', result.stats.uniqueWords);
        span.setAttribute('kb.load.index_highlights', result.stats.indexHighlights);
        span.setAttribute('kb.load.entities', result.entities.length);
        span.setAttribute('kb.load.relations', result.relations.length);
        return result;
      }),
    );
  }

  // --- Locking helpers ---

  /**
   * Acquire a shared (read) lock on BOTH the graph and string-table files
   * (via {@link GraphFile.lockShared}), refresh mappings (in case another
   * process grew the files), rebuild the name index, run the callback, then
   * release the lock.
   *
   * The name index is rebuilt unconditionally on every acquisition. The
   * previous "skip rebuild if entity count is unchanged" optimization was
   * incorrect under multi-process load: if another process did
   * delete-then-create, the entity count can be unchanged while the freed
   * slot has been recycled to a different name. The stale nameIndex entry
   * would then dereference into reused free-list memory and downstream
   * derivations (e.g. {@link AdjEntry.targetOffset}) could yield wildly
   * out-of-bounds offsets.
   *
   * Wrapped in a `kb.lock.read` span. The `kb.lock.wait_ms` attribute records
   * how long we blocked acquiring the OS-level shared lock — useful for
   * spotting contention from concurrent writers.
   */
  private withReadLock<T>(fn: () => T): T {
    return traced('kb.lock.read', {}, (span) => {
      const acquireStart = process.hrtime.bigint();
      this.db.lockShared();
      const waitMs = Number(process.hrtime.bigint() - acquireStart) / 1e6;
      span.setAttribute('kb.lock.wait_ms', waitMs);
      try {
        this.db.refresh();
        span.setAttribute('kb.entity_count', this.db.entityCount());
        return fn();
      } finally {
        this.db.unlock();
      }
    });
  }

  /**
   * Acquire an exclusive (write) lock on BOTH the graph and string-table
   * files (via {@link GraphFile.lockExclusive}), refresh mappings, rebuild
   * name index, run the callback, sync both files, then release the lock.
   *
   * Required for any callback that mutates the strings file (allocation via
   * `st.intern` of a previously-unseen string) as well as for graph
   * mutations — withReadLock on the graph fd alone does NOT serialize
   * strings-file mutations because flock(2) is per-fd.
   *
   * Like {@link withReadLock}, the name index is rebuilt unconditionally to
   * pick up any concurrent delete+create that another process may have
   * performed since this process last held a lock.
   *
   * Wrapped in a `kb.lock.write` span. The `kb.lock.wait_ms` attribute records
   * blocking time waiting for the exclusive lock.
   */
  private withWriteLock<T>(fn: () => T): T {
    return traced('kb.lock.write', {}, (span) => {
      const acquireStart = process.hrtime.bigint();
      this.db.lockExclusive();
      const waitMs = Number(process.hrtime.bigint() - acquireStart) / 1e6;
      span.setAttribute('kb.lock.wait_ms', waitMs);
      try {
        this.db.refresh();
        span.setAttribute('kb.entity_count', this.db.entityCount());
        const result = fn();
        this.db.sync();
        return result;
      } finally {
        this.db.unlock();
      }
    });
  }

  // --- kb_load corpus shim --------------------------------------------------

  /**
   * Minimal StringTable-shaped shim for loadDocument's IDF corpus pass: yields
   * every entity's name/type/observation strings from the C store. The real
   * string table now lives in C; loadDocument only consumes `entries()`.
   */
  private corpusShim(): { entries: () => Generator<{ id: bigint; text: string; refcount: number }> } {
    const db = this.db;
    return {
      *entries() {
        for (const off of db.listEntities()) {
          const r = db.readEntity(off);
          yield { id: off, text: r.name, refcount: 1 };
          yield { id: off, text: r.type, refcount: 1 };
          for (const o of r.observations) yield { id: off, text: o, refcount: 1 };
        }
      },
    };
  }

  /** Build rank maps from the binary store for pagerank/llmrank sorting.
   *  NOTE: Must be called inside a lock (read or write).
   *
   *  Always-on `kb.rank.read` child span: this is a hot path called from every
   *  read tool that sorts by rank, and surfaces the cost of reading the
   *  per-entity rank fields under the active lock.
   */
  private getRankMapsUnlocked(): { structural: Map<string, number>; walker: Map<string, number> } {
    return traced('kb.rank.read', { 'kb.entity_count': this.db.entityCount() }, () => {
      const structural = new Map<string, number>();
      const walker = new Map<string, number>();
      const structTotal = this.db.structuralTotal();
      const walkerTotal = this.db.walkerTotal();

      for (const offset of this.db.listEntities()) {
        const rec = this.db.readEntity(offset);
        structural.set(rec.name, structTotal > 0n ? Number(rec.structuralVisits) / Number(structTotal) : 0);
        walker.set(rec.name, walkerTotal > 0n ? Number(rec.walkerVisits) / Number(walkerTotal) : 0);
      }

      return { structural, walker };
    });
  }

  /** Build rank maps (acquires read lock). */
  getRankMaps(): { structural: Map<string, number>; walker: Map<string, number> } {
    return this.withReadLock(() => this.getRankMapsUnlocked());
  }

  /** Increment walker visit count for a list of entity names */
  recordWalkerVisits(names: string[]): void {
    traced('kb.walker.record', { 'kb.walker.count': names.length }, () => {
      this.withWriteLock(() => {
        for (const name of names) {
          const offset = this.db.lookup(name);
          if (offset !== 0n) {
            this.db.incWalkerVisit(offset);
          }
        }
      });
    });
  }

  /**
   * Re-run structural sampling and MERW eigenvector computation (call after
   * graph mutations).
   *
   * Emits two child spans under the active span: `kb.rank.pagerank`
   * (structural sampling — PageRank-style random walks) and `kb.rank.merw`
   * (Maximum-Entropy Random Walk eigenvector). Skipped entirely on an empty
   * graph so the spans aren't emitted for trivial no-op resamples.
   */
  resample(): void {
    this.withWriteLock(() => {
      if (this.db.entityCount() === 0) return;
      traced(
        'kb.rank.pagerank',
        { 'kb.entity_count': this.db.entityCount() },
        () => this.db.structuralSample(1, 0.85),
      );
      traced(
        'kb.rank.merw',
        { 'kb.entity_count': this.db.entityCount() },
        () => this.db.computeMerwPsi(0.85, 200, 1e-8),
      );
    });
  }

  /** Convert a native entity record to the public Entity interface */
  private recordToEntity(rec: NativeEntity): Entity {
    const entity: Entity = { name: rec.name, entityType: rec.type, observations: rec.observations };
    const mtime = Number(rec.mtime);
    const obsMtime = Number(rec.obsMtime);
    if (mtime > 0) entity.mtime = mtime;
    if (obsMtime > 0) entity.obsMtime = obsMtime;
    return entity;
  }

  /** Get all entities as Entity objects (preserves node log order = insertion order) */
  private getAllEntities(): Entity[] {
    return this.db.listEntities().map(o => this.recordToEntity(this.db.readEntity(o)));
  }

  /** Get all relations by scanning adjacency lists (forward edges only to avoid duplication) */
  private getAllRelations(): Relation[] {
    const relations: Relation[] = [];
    for (const offset of this.db.listEntities()) {
      const fromName = this.db.entityName(offset);
      for (const edge of this.db.edges(offset)) {
        if (edge.direction !== DIR_FORWARD) continue;
        const toName = this.db.entityName(edge.target);
        const r: Relation = { from: fromName, to: toName, relationType: edge.relType };
        const mtime = Number(edge.mtime);
        if (mtime > 0) r.mtime = mtime;
        relations.push(r);
      }
    }
    return relations;
  }

  /** Load the full graph (entities + relations) */
  private loadGraph(): KnowledgeGraph {
    return {
      entities: this.getAllEntities(),
      relations: this.getAllRelations(),
    };
  }

  async createEntities(entities: Entity[]): Promise<Entity[]> {
    // Validate observation limits (can do outside lock)
    for (const entity of entities) {
      if (entity.observations.length > 2) {
        throw new Error(`Entity "${entity.name}" has ${entity.observations.length} observations. Maximum allowed is 2.`);
      }
      for (const obs of entity.observations) {
        if (obs.length > 140) {
          throw new Error(`Observation in entity "${entity.name}" exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
        }
      }
    }

    return this.withWriteLock(() => {
      const now = BigInt(Date.now());
      const newEntities: Entity[] = [];

      for (const e of entities) {
        const existingOffset = this.db.lookup(e.name);
        if (existingOffset !== 0n) {
          const existing = this.recordToEntity(this.db.readEntity(existingOffset));
          const sameType = existing.entityType === e.entityType;
          const sameObs = existing.observations.length === e.observations.length &&
            existing.observations.every((o, i) => o === e.observations[i]);
          if (sameType && sameObs) continue;
          throw new Error(`Entity "${e.name}" already exists with different data (type: "${existing.entityType}" vs "${e.entityType}", observations: ${existing.observations.length} vs ${e.observations.length})`);
        }

        const offset = this.db.createEntity(e.name, e.entityType, now);
        for (const obs of e.observations) {
          this.db.addObservation(offset, obs, now);
        }

        const newEntity: Entity = {
          ...e,
          mtime: Number(now),
          obsMtime: e.observations.length > 0 ? Number(now) : undefined,
        };
        newEntities.push(newEntity);
      }

      return newEntities;
    });
  }

  async createRelations(relations: Relation[]): Promise<Relation[]> {
    return this.withWriteLock(() => {
      const now = BigInt(Date.now());
      const newRelations: Relation[] = [];

      for (const r of relations) {
        const fromOffset = this.db.lookup(r.from);
        const toOffset = this.db.lookup(r.to);
        if (fromOffset === 0n || toOffset === 0n) continue;

        const isDuplicate = this.db.edges(fromOffset).some(e =>
          e.direction === DIR_FORWARD && e.target === toOffset && e.relType === r.relationType
        );
        if (isDuplicate) continue;

        // C owns the bidirectional edges + relType interning/refcounts.
        this.db.createRelation(fromOffset, toOffset, r.relationType, now);
        newRelations.push({ ...r, mtime: Number(now) });
      }

      return newRelations;
    });
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
    return this.withWriteLock(() => {
      const results: { entityName: string; addedObservations: string[] }[] = [];

      for (const o of observations) {
        const offset = this.db.lookup(o.entityName);
        if (offset === 0n) {
          throw new Error(`Entity with name ${o.entityName} not found`);
        }

        for (const obs of o.contents) {
          if (obs.length > 140) {
            throw new Error(`Observation for "${o.entityName}" exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
          }
        }

        const existingObs = this.db.readEntity(offset).observations;
        const newObservations = o.contents.filter(content => !existingObs.includes(content));

        if (existingObs.length + newObservations.length > 2) {
          throw new Error(`Adding ${newObservations.length} observations to "${o.entityName}" would exceed limit of 2 (currently has ${existingObs.length}).`);
        }

        const now = BigInt(Date.now());
        for (const obs of newObservations) {
          this.db.addObservation(offset, obs, now);
        }

        results.push({ entityName: o.entityName, addedObservations: newObservations });
      }

      return results;
    });
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    this.withWriteLock(() => {
      for (const name of entityNames) {
        const offset = this.db.lookup(name);
        if (offset === 0n) continue;
        // C deletes the record + adjacency, drops mirror edges, and releases
        // every string ref (name/type/obs + relType per edge + mirror).
        this.db.deleteEntity(offset);
      }
    });
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    this.withWriteLock(() => {
      const now = BigInt(Date.now());
      for (const d of deletions) {
        const offset = this.db.lookup(d.entityName);
        if (offset === 0n) continue;

        for (const obs of d.observations) {
          this.db.removeObservation(offset, obs, now);
        }
      }
    });
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    this.withWriteLock(() => {
      for (const r of relations) {
        const fromOffset = this.db.lookup(r.from);
        const toOffset = this.db.lookup(r.to);
        if (fromOffset === 0n || toOffset === 0n) continue;
        // C removes both directed edges and releases the two relType refs.
        this.db.deleteRelation(fromOffset, toOffset, r.relationType);
      }
    });
  }

  /**
   * Regex-based entity search.
   *
   * Internally tries to extract a trigram filter from the regex source via
   * {@link extractRequiredTrigramFilter}. If extraction succeeds we use the
   * trigram index to narrow the candidate set before running the regex; if
   * not (regex contains metacharacters we don't safely handle), we fall back
   * to the linear scan. Same external contract — caller still passes a
   * regex; the speedup is invisible.
   */
  async searchNodes(
    query: string,
    sortBy?: EntitySortField,
    sortDir?: SortDirection,
    direction: 'forward' | 'backward' | 'any' = 'forward',
  ): Promise<KnowledgeGraph> {
    // Validate the pattern with JS RegExp semantics so callers still get the
    // "Invalid regex pattern" contract; the actual match runs in C (POSIX ERE).
    try {
      new RegExp(query, 'i');
    } catch {
      throw new Error(`Invalid regex pattern: ${query}`);
    }

    return traced(
      'kb.search_nodes',
      {
        'kb.search.direction': direction,
        'kb.search.query_length': query.length,
        ...(sortBy ? { 'kb.search.sort_by': sortBy } : {}),
      },
      (span) => this.withReadLock(() => {
        const filteredEntities = this.db.search(query).map(o => this.recordToEntity(this.db.readEntity(o)));
        const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

        const allRelations = this.getAllRelations();
        const filteredRelations = allRelations.filter(r => {
          if (direction === 'forward') return filteredEntityNames.has(r.from);
          if (direction === 'backward') return filteredEntityNames.has(r.to);
          return filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to);
        });

        span.setAttribute('kb.search.used_trigram', false);
        span.setAttribute('kb.search.scanned.entities', this.db.entityCount());
        span.setAttribute('kb.search.scanned.relations', allRelations.length);
        span.setAttribute('kb.search.matched.entities', filteredEntities.length);
        span.setAttribute('kb.search.matched.relations', filteredRelations.length);

        const rankMaps = this.getRankMapsUnlocked();
        return {
          entities: sortEntities(filteredEntities, sortBy, sortDir, rankMaps),
          relations: filteredRelations,
        };
      }),
    );
  }

  async openNodes(names: string[], direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<KnowledgeGraph> {
    return this.withReadLock(() => {
      const filteredEntities: Entity[] = [];
      const offsetByName = new Map<string, bigint>();
      for (const name of names) {
        const offset = this.db.lookup(name);
        if (offset === 0n) continue;
        filteredEntities.push(this.recordToEntity(this.db.readEntity(offset)));
        offsetByName.set(name, offset);
      }

      const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

      const filteredRelations: Relation[] = [];
      for (const name of filteredEntityNames) {
        const offset = offsetByName.get(name)!;
        for (const edge of this.db.edges(offset)) {
          if (edge.direction !== DIR_FORWARD && edge.direction !== DIR_BACKWARD) continue;

          const targetName = this.db.entityName(edge.target);
          const relationType = edge.relType;
          const mtime = Number(edge.mtime);

          if (edge.direction === DIR_FORWARD) {
            if (direction === 'backward') continue;
            const r: Relation = { from: name, to: targetName, relationType };
            if (mtime > 0) r.mtime = mtime;
            filteredRelations.push(r);
          } else {
            if (direction === 'forward') continue;
            if (direction === 'any' && !filteredEntityNames.has(targetName)) continue;
            const r: Relation = { from: targetName, to: name, relationType };
            if (mtime > 0) r.mtime = mtime;
            filteredRelations.push(r);
          }
        }
      }

      return { entities: filteredEntities, relations: filteredRelations };
    });
  }

  async getNeighbors(
    entityName: string,
    depth: number = 1,
    sortBy?: EntitySortField,
    sortDir?: SortDirection,
    direction: 'forward' | 'backward' | 'any' = 'forward'
  ): Promise<Neighbor[]> {
    return traced(
      'kb.get_neighbors',
      {
        'kb.traversal.depth': depth,
        'kb.traversal.direction': direction,
        ...(sortBy ? { 'kb.traversal.sort_by': sortBy } : {}),
      },
      (span) => this.withReadLock(() => {
        const startOffset = this.db.lookup(entityName);
        if (startOffset === 0n) {
          span.setAttribute('kb.traversal.start_found', false);
          return [];
        }
        span.setAttribute('kb.traversal.start_found', true);

        // C BFS returns neighbor offsets within `depth` hops, excluding start.
        // The old TS semantics were one hop deeper (depth=0 returned immediate
        // neighbors), so request depth+1 from C to match.
        const neighbors: Neighbor[] = this.db.neighbors(startOffset, depth + 1, direction).map(off => {
          const rec = this.db.readEntity(off);
          const mtime = Number(rec.mtime);
          const obsMtime = Number(rec.obsMtime);
          const n: Neighbor = { name: rec.name };
          if (mtime > 0) n.mtime = mtime;
          if (obsMtime > 0) n.obsMtime = obsMtime;
          return n;
        });

        span.setAttribute('kb.traversal.neighbor_count', neighbors.length);

        const rankMaps = this.getRankMapsUnlocked();
        return sortNeighbors(neighbors, sortBy, sortDir, rankMaps);
      }),
    );
  }

  /**
   * Search result for `findPath`.
   *
   *   - `path`: the relation sequence the call returns. When
   *     `targetReached` is true this is the (shortest) path from
   *     `fromEntity` to `toEntity`. When `targetReached` is false it is
   *     a best-effort exploration path ending at `farthestDiscovered`
   *     — see contract notes on {@link KnowledgeGraphManager#findPath}.
   *
   *   - `targetReached`: did BFS actually arrive at `toEntity`? When
   *     false the caller must NOT treat `path` as a path to `toEntity`;
   *     it is a path to `farthestDiscovered` instead.
   *
   *   - `budgetExhausted`: did we stop because we hit the per-call
   *     memory cap (vs. because the frontier emptied within `maxDepth`)?
   *     Disambiguates the two `targetReached === false` modes.
   *
   *   - `farthestDiscovered`: the deepest node BFS expanded toward, in
   *     BFS-order tie-breaking — natural anchor for a follow-up
   *     `find_path(farthestDiscovered, toEntity, …)` retry from the
   *     LLM's side.
   *
   *   - `budgetBytes`: the byte budget that was in effect on this call,
   *     for the response message and OTel attribute.
   */
  async findPath(fromEntity: string, toEntity: string, maxDepth: number = 5, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<{
    path: Relation[];
    targetReached: boolean;
    budgetExhausted: boolean;
    farthestDiscovered?: string;
    budgetBytes: number;
  }> {
    return traced(
      'kb.find_path',
      {
        'kb.traversal.max_depth': maxDepth,
        'kb.traversal.direction': direction,
      },
      // Shortest-path search via BFS, parent-pointer reconstruction.
      //
      // Predecessor was a DFS that called `visited.delete(current)` on
      // backtrack — that turned `visited` into a per-path no-revisit guard
      // instead of a global one, so a `find_path` against an unreachable
      // (or far-away) target enumerated EVERY simple path of length
      // ≤ maxDepth before giving up. On a hub node (b ≈ 30-100), depth=5
      // is `b^5` = 10^7..10^10 path-expansions, each doing a `st.get()`
      // deserialization → minutes-to-hours of pure JS holding the read
      // lock. Caught in production 2026-05-16 (PID 2282706, 27 min,
      // 91% CPU, stack rooted at `findPath → dfs → dfs → …`).
      //
      // BFS by design enqueues every reachable node at most once, so
      // total work is O(N+E) capped by the BFS frontier reaching
      // maxDepth. As a bonus, BFS returns the *shortest* path — which is
      // what users expect from a tool literally called `find_path` with a
      // `maxDepth` arg. DFS gave the first path the recursion discovered,
      // not the shortest.
      (span) => this.withReadLock(() => {
        const budgetBytes = findPathBudgetBytes();

        // from === to: trivial 0-hop path; no edges. Only path-length-zero
        // case that counts as targetReached.
        if (fromEntity === toEntity) {
          span.setAttribute('kb.traversal.path_length', 0);
          span.setAttribute('kb.traversal.path_found', true);
          span.setAttribute('kb.traversal.target_reached', true);
          return { path: [], targetReached: true, budgetExhausted: false, budgetBytes };
        }

        const fromOffset = this.db.lookup(fromEntity);
        const toOffset = this.db.lookup(toEntity);
        if (fromOffset === 0n || toOffset === 0n) {
          span.setAttribute('kb.traversal.path_found', false);
          span.setAttribute('kb.traversal.target_reached', false);
          return { path: [], targetReached: false, budgetExhausted: false, budgetBytes };
        }

        // C BFS: shortest path to target, or a best-effort path to the
        // farthest-discovered node when the target isn't reached (β-contract).
        // The byte budget bounds the C BFS just as it bounded the old JS BFS;
        // KB_FIND_PATH_BUDGET_BYTES flows in via findPathBudgetBytes().
        const res = this.db.findPath(fromOffset, toOffset, maxDepth, direction, BigInt(budgetBytes));
        const found = res.targetReached;
        const nodePath = res.path;

        const path: Relation[] = [];
        for (let i = 0; i + 1 < nodePath.length; i++) {
          const cur = nodePath[i];
          const next = nodePath[i + 1];
          const e = this.db.edges(cur).find(ed => ed.target === next && (
            direction === 'forward' ? ed.direction === DIR_FORWARD :
            direction === 'backward' ? ed.direction === DIR_BACKWARD :
            (ed.direction === DIR_FORWARD || ed.direction === DIR_BACKWARD)
          ));
          if (!e) continue;
          const curName = this.db.entityName(cur);
          const nextName = this.db.entityName(next);
          const rel: Relation = e.direction === DIR_FORWARD
            ? { from: curName, to: nextName, relationType: e.relType }
            : { from: nextName, to: curName, relationType: e.relType };
          const mtime = Number(e.mtime);
          if (mtime > 0) rel.mtime = mtime;
          path.push(rel);
        }

        const farthestDiscovered = (!found && res.farthest !== 0n)
          ? this.db.entityName(res.farthest) : undefined;

        span.setAttribute('kb.traversal.path_length', path.length);
        span.setAttribute('kb.traversal.path_found', found);
        span.setAttribute('kb.traversal.target_reached', found);
        span.setAttribute('kb.traversal.budget_bytes', budgetBytes);
        span.setAttribute('kb.traversal.budget_exhausted', res.budgetExhausted);
        if (farthestDiscovered !== undefined) span.setAttribute('kb.traversal.farthest_discovered', farthestDiscovered);
        return {
          path,
          targetReached: found,
          budgetExhausted: res.budgetExhausted,
          farthestDiscovered,
          budgetBytes,
        };
      }),
    );
  }

  async getEntitiesByType(entityType: string, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    return this.withReadLock(() => {
      const filtered = this.getAllEntities().filter(e => e.entityType === entityType);
      const rankMaps = this.getRankMapsUnlocked();
      return sortEntities(filtered, sortBy, sortDir, rankMaps);
    });
  }

  async getEntityTypes(): Promise<string[]> {
    return this.withReadLock(() => {
      const types = new Set(this.getAllEntities().map(e => e.entityType));
      return Array.from(types).sort();
    });
  }

  async getRelationTypes(): Promise<string[]> {
    return this.withReadLock(() => {
      const types = new Set(this.getAllRelations().map(r => r.relationType));
      return Array.from(types).sort();
    });
  }

  async getStats(): Promise<{ entityCount: number; relationCount: number; entityTypes: number; relationTypes: number }> {
    return this.withReadLock(() => {
      const entities = this.getAllEntities();
      const relations = this.getAllRelations();
      const entityTypes = new Set(entities.map(e => e.entityType));
      const relationTypes = new Set(relations.map(r => r.relationType));

      return {
        entityCount: entities.length,
        relationCount: relations.length,
        entityTypes: entityTypes.size,
        relationTypes: relationTypes.size,
      };
    });
  }

  async getOrphanedEntities(strict: boolean = false, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    return traced(
      'kb.get_orphaned_entities',
      { 'kb.orphan.strict': strict },
      (span) => this.withReadLock(() => {
        const entities = this.getAllEntities();

        if (!strict) {
          const connectedEntityNames = new Set<string>();
          const relations = this.getAllRelations();
          relations.forEach(r => {
            connectedEntityNames.add(r.from);
            connectedEntityNames.add(r.to);
          });
          const orphans = entities.filter(e => !connectedEntityNames.has(e.name));
          span.setAttribute('kb.entity_count', entities.length);
          span.setAttribute('kb.relation_count', relations.length);
          span.setAttribute('kb.orphan.count', orphans.length);
          const rankMaps = this.getRankMapsUnlocked();
          return sortEntities(orphans, sortBy, sortDir, rankMaps);
        }

        const neighbors = new Map<string, Set<string>>();
        entities.forEach(e => neighbors.set(e.name, new Set()));
        const relations = this.getAllRelations();
        relations.forEach(r => {
          neighbors.get(r.from)?.add(r.to);
          neighbors.get(r.to)?.add(r.from);
        });

        const connectedToSelf = new Set<string>();
        const queue: string[] = ['Self'];

        while (queue.length > 0) {
          const current = queue.shift()!;
          if (connectedToSelf.has(current)) continue;
          connectedToSelf.add(current);

          const currentNeighbors = neighbors.get(current);
          if (currentNeighbors) {
            for (const neighbor of currentNeighbors) {
              if (!connectedToSelf.has(neighbor)) {
                queue.push(neighbor);
              }
            }
          }
        }

        const orphans = entities.filter(e => !connectedToSelf.has(e.name));
        span.setAttribute('kb.entity_count', entities.length);
        span.setAttribute('kb.relation_count', relations.length);
        span.setAttribute('kb.orphan.count', orphans.length);
        span.setAttribute('kb.orphan.connected_to_self', connectedToSelf.size);
        const rankMaps = this.getRankMapsUnlocked();
        return sortEntities(orphans, sortBy, sortDir, rankMaps);
      }),
    );
  }

  async validateGraph(): Promise<{ missingEntities: string[]; observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] }> {
    return this.withReadLock(() => {
      const entities = this.getAllEntities();
      const relations = this.getAllRelations();
      const entityNames = new Set(entities.map(e => e.name));
      const missingEntities = new Set<string>();
      const observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] = [];

      relations.forEach(r => {
        if (!entityNames.has(r.from)) missingEntities.add(r.from);
        if (!entityNames.has(r.to)) missingEntities.add(r.to);
      });

      entities.forEach(e => {
        const oversizedObservations: number[] = [];
        e.observations.forEach((obs, idx) => {
          if (obs.length > 140) oversizedObservations.push(idx);
        });

        if (e.observations.length > 2 || oversizedObservations.length > 0) {
          observationViolations.push({
            entity: e.name,
            count: e.observations.length,
            oversizedObservations,
          });
        }
      });

      return { missingEntities: Array.from(missingEntities), observationViolations };
    });
  }

  async randomWalk(
    start: string,
    depth: number = 3,
    seed?: string,
    direction: 'forward' | 'backward' | 'any' = 'forward',
    mode: RandomWalkMode = 'merw',
  ): Promise<{ entity: string; path: string[] }> {
    return traced(
      'kb.random_walk',
      {
        'kb.traversal.depth': depth,
        'kb.traversal.direction': direction,
        'kb.walker.mode': mode,
        'kb.walker.seeded': seed !== undefined,
      },
      (span) => this.withReadLock(() => {
        const startOffset = this.db.lookup(start);
        if (startOffset === 0n) {
          throw new Error(`Start entity not found: ${start}`);
        }

        // Seeded walk: hash the string seed to a u64 the C RNG can use. A
        // seed of 0 means "use the global RNG" (unseeded), so hashSeed (never
        // 0) keeps seeded walks reproducible.
        const seedU64 = seed !== undefined ? BigInt(this.hashSeed(seed) >>> 0) : 0n;
        const pathOffsets = this.db.randomWalk(startOffset, depth, direction, mode === 'merw', seedU64);
        const pathNames = pathOffsets.map(o => this.db.entityName(o));

        span.setAttribute('kb.walker.steps_taken', pathNames.length - 1);
        span.setAttribute('kb.walker.truncated', pathNames.length - 1 < depth);
        return { entity: pathNames[pathNames.length - 1], path: pathNames };
      }),
    );
  }

  private hashSeed(seed: string): number {
    let hash = 0;
    for (let i = 0; i < seed.length; i++) {
      const char = seed.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash || 1;
  }

  decodeTimestamp(timestamp?: number, relative: boolean = false): { timestamp: number; iso8601: string; formatted: string; relative?: string } {
    const ts = timestamp ?? Date.now();
    const date = new Date(ts);

    const result: { timestamp: number; iso8601: string; formatted: string; relative?: string } = {
      timestamp: ts,
      iso8601: date.toISOString(),
      formatted: date.toUTCString(),
    };

    if (relative) {
      const now = Date.now();
      const diffMs = now - ts;
      const diffSec = Math.abs(diffMs) / 1000;
      const diffMin = diffSec / 60;
      const diffHour = diffMin / 60;
      const diffDay = diffHour / 24;

      let relStr: string;
      if (diffSec < 60) {
        relStr = `${Math.floor(diffSec)} seconds`;
      } else if (diffMin < 60) {
        relStr = `${Math.floor(diffMin)} minutes`;
      } else if (diffHour < 24) {
        relStr = `${Math.floor(diffHour)} hours`;
      } else if (diffDay < 30) {
        relStr = `${Math.floor(diffDay)} days`;
      } else if (diffDay < 365) {
        relStr = `${Math.floor(diffDay / 30)} months`;
      } else {
        relStr = `${Math.floor(diffDay / 365)} years`;
      }

      result.relative = diffMs >= 0 ? `${relStr} ago` : `in ${relStr}`;
    }

    return result;
  }

  async addThought(observations: string[], previousCtxId?: string): Promise<{ ctxId: string }> {
    // Validate observations (can do outside lock)
    if (observations.length > 2) {
      throw new Error(`Thought has ${observations.length} observations. Maximum allowed is 2.`);
    }
    for (const obs of observations) {
      if (obs.length > 140) {
        throw new Error(`Observation exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
      }
    }

    return this.withWriteLock(() => {
      const now = BigInt(Date.now());
      const ctxId = randomBytes(12).toString('hex');

      const offset = this.db.createEntity(ctxId, 'Thought', now);
      for (const obs of observations) {
        this.db.addObservation(offset, obs, now);
      }

      if (previousCtxId) {
        const prevOffset = this.db.lookup(previousCtxId);
        if (prevOffset !== 0n) {
          // prev --follows--> new, and new --preceded_by--> prev. C creates
          // both directed edges per relation and owns the refcounts.
          this.db.createRelation(prevOffset, offset, 'follows', now);
          this.db.createRelation(offset, prevOffset, 'preceded_by', now);
        }
      }

      return { ctxId };
    });
  }

  /** Close the underlying binary store files */
  close(): void {
    this.db.close();
  }
}

/**
 * Creates a configured MCP server instance with all tools registered.
 * @param memoryFilePath Optional path to the memory file (defaults to MEMORY_FILE_PATH env var or memory.json)
 */
export function createServer(memoryFilePath?: string): Server {
  const knowledgeGraphManager = new KnowledgeGraphManager(memoryFilePath);

  const server = new Server({
    name: "memory-server",
    icons: [
      { src: "data:image/svg+xml;base64,PHN2ZyBmaWxsPSJjdXJyZW50Q29sb3IiIGZpbGwtcnVsZT0iZXZlbm9kZCIgaGVpZ2h0PSIxZW0iIHN0eWxlPSJmbGV4Om5vbmU7bGluZS1oZWlnaHQ6MSIgdmlld0JveD0iMCAwIDI0IDI0IiB3aWR0aD0iMWVtIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjx0aXRsZT5Nb2RlbENvbnRleHRQcm90b2NvbDwvdGl0bGU+PHBhdGggZD0iTTIwLjEgNS41MlYxLjVoLS4xOGMtMy4zNi4xNS02LjE1IDIuMzEtNy44MyA0LjAybC0uMDkuMDktLjA5LS4wOUMxMC4yIDMuODEgNy40NCAxLjY1IDQuMDggMS41SDMuOXY0LjAySDB2Ni45M2MwIDEuNjguMDYgMy4zNi4xOCA0Ljc0YTUuNTcgNS41NyAwIDAgMCA1LjE5IDUuMWMyLjEzLjEyIDQuMzguMjEgNi42My4yMXM0LjUtLjA5IDYuNjMtLjI0YTUuNTcgNS41NyAwIDAgMCA1LjE5LTUuMWMuMTItMS4zOC4xOC0zLjA2LjE4LTQuNzR2LTYuOXptMCA2LjkzYzAgMS41OS0uMDYgMy4xNS0uMTggNC40MS0uMDkuODEtLjc1IDEuNDctMS41NiAxLjVhOTAgOTAgMCAwIDEtMTIuNzIgMGMtLjgxLS4wMy0xLjUtLjY5LTEuNTYtMS41LS4xMi0xLjI2LS4xOC0yLjg1LS4xOC00LjQxVjUuNTJjMi44Mi4xMiA1LjY0IDMuMTUgNi40OCA0LjMyTDEyIDEyLjA5bDEuNjItMi4yNWMuODQtMS4yIDMuNjYtNC4yIDYuNDgtNC4zMnoiLz48L3N2Zz4=",
        mimeType: "image/svg+xml",
        sizes: ["any"]
      }
    ],
    version: "0.0.27",
  }, {
    capabilities: {
      tools: {},
    },
  });

  // Close binary store on server close
  server.onclose = () => {
    knowledgeGraphManager.close();
  };

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "create_entities",
        description: "Create multiple new entities in the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {
            entities: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  name: { type: "string", description: "The name of the entity" },
                  entityType: { type: "string", description: "The type of the entity" },
                  observations: { 
                    type: "array", 
                    items: { type: "string", maxLength: 140 },
                    maxItems: 2,
                    description: "Observations associated with the entity. MAX 2 observations, each MAX 140 characters."
                  },
                },
                required: ["name", "entityType", "observations"],
              },
            },
          },
          required: ["entities"],
        },
      },
      {
        name: "create_relations",
        description: "Create multiple new relations between entities in the knowledge graph. Relations should be in active voice",
        inputSchema: {
          type: "object",
          properties: {
            relations: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  from: { type: "string", description: "The name of the entity where the relation starts" },
                  to: { type: "string", description: "The name of the entity where the relation ends" },
                  relationType: { type: "string", description: "The type of the relation" },
                },
                required: ["from", "to", "relationType"],
              },
            },
          },
          required: ["relations"],
        },
      },
      {
        name: "add_observations",
        description: "Add new observations to existing entities in the knowledge graph. Entities are limited to 2 total observations.",
        inputSchema: {
          type: "object",
          properties: {
            observations: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  entityName: { type: "string", description: "The name of the entity to add the observations to" },
                  contents: { 
                    type: "array", 
                    items: { type: "string", maxLength: 140 },
                    description: "Observations to add. Each MAX 140 characters. Entity total MAX 2 observations."
                  },
                },
                required: ["entityName", "contents"],
              },
            },
          },
          required: ["observations"],
        },
      },
      {
        name: "delete_entities",
        description: "Delete multiple entities and their associated relations from the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {
            entityNames: { 
              type: "array", 
              items: { type: "string" },
              description: "An array of entity names to delete" 
            },
          },
          required: ["entityNames"],
        },
      },
      {
        name: "delete_observations",
        description: "Delete specific observations from entities in the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {
            deletions: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  entityName: { type: "string", description: "The name of the entity containing the observations" },
                  observations: { 
                    type: "array", 
                    items: { type: "string" },
                    description: "An array of observations to delete"
                  },
                },
                required: ["entityName", "observations"],
              },
            },
          },
          required: ["deletions"],
        },
      },
      {
        name: "delete_relations",
        description: "Delete multiple relations from the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {
            relations: { 
              type: "array", 
              items: {
                type: "object",
                properties: {
                  from: { type: "string", description: "The name of the entity where the relation starts" },
                  to: { type: "string", description: "The name of the entity where the relation ends" },
                  relationType: { type: "string", description: "The type of the relation" },
                },
                required: ["from", "to", "relationType"],
              },
              description: "An array of relations to delete" 
            },
          },
          required: ["relations"],
        },
      },
      {
        name: "search_nodes",
        description: "Search for nodes in the knowledge graph using a regex pattern. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            query: { type: "string", description: "Regex pattern to match against entity names, types, and observations." },
            direction: { type: "string", enum: ["forward", "backward", "any"], description: "Edge direction filter for returned relations. Default: forward" },
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name", "pagerank", "llmrank"], description: "Sort field for entities. Omit for insertion order." },
            sortDir: { type: "string", enum: ["asc", "desc"], description: "Sort direction. Default: desc for timestamps, asc for name." },
            entityCursor: { type: "number", description: "Cursor for entity pagination (from previous response's nextCursor)" },
            relationCursor: { type: "number", description: "Cursor for relation pagination" },
          },
          required: ["query"],
        },
      },
      {
        name: "open_nodes",
        description: "Open specific nodes in the knowledge graph by their names. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            names: {
              type: "array",
              items: { type: "string" },
              description: "An array of entity names to retrieve",
            },
            direction: { type: "string", enum: ["forward", "backward", "any"], description: "Edge direction filter for returned relations. Default: forward" },
            entityCursor: { type: "number", description: "Cursor for entity pagination" },
            relationCursor: { type: "number", description: "Cursor for relation pagination" },
          },
          required: ["names"],
        },
      },
      {
        name: "get_neighbors",
        description: "Get names of neighboring entities connected to a specific entity within a given depth. Returns neighbor names with timestamps for sorting. Use open_nodes to get full entity data. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            entityName: { type: "string", description: "The name of the entity to find neighbors for" },
            depth: { type: "number", description: "Maximum depth to traverse (default: 1)", default: 1 },
            direction: { type: "string", enum: ["forward", "backward", "any"], description: "Edge direction to follow. Default: forward" },
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name", "pagerank", "llmrank"], description: "Sort field for neighbors. Omit for arbitrary order." },
            sortDir: { type: "string", enum: ["asc", "desc"], description: "Sort direction. Default: desc for timestamps, asc for name." },
            cursor: { type: "number", description: "Cursor for pagination" },
          },
          required: ["entityName"],
        },
      },
      {
        name: "find_path",
        description: "Find a path between two entities in the knowledge graph. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            fromEntity: { type: "string", description: "The name of the starting entity" },
            toEntity: { type: "string", description: "The name of the target entity" },
            maxDepth: { type: "number", description: "Maximum depth to search (default: 5)", default: 5 },
            direction: { type: "string", enum: ["forward", "backward", "any"], description: "Edge direction to follow. Default: forward" },
            cursor: { type: "number", description: "Cursor for pagination" },
          },
          required: ["fromEntity", "toEntity"],
        },
      },
      {
        name: "get_entities_by_type",
        description: "Get all entities of a specific type. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            entityType: { type: "string", description: "The type of entities to retrieve" },
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name", "pagerank", "llmrank"], description: "Sort field for entities. Omit for insertion order." },
            sortDir: { type: "string", enum: ["asc", "desc"], description: "Sort direction. Default: desc for timestamps, asc for name." },
            cursor: { type: "number", description: "Cursor for pagination" },
          },
          required: ["entityType"],
        },
      },
      {
        name: "get_entity_types",
        description: "Get all unique entity types in the knowledge graph. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            cursor: { type: "number", description: "Cursor for pagination" },
          },
        },
      },
      {
        name: "get_relation_types",
        description: "Get all unique relation types in the knowledge graph. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            cursor: { type: "number", description: "Cursor for pagination" },
          },
        },
      },
      {
        name: "get_stats",
        description: "Get statistics about the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {},
        },
      },
      {
        name: "get_orphaned_entities",
        description: "Get entities that have no relations (orphaned entities). In strict mode, returns entities not connected to 'Self' entity. Results are paginated (max 4096 chars).",
        inputSchema: {
          type: "object",
          properties: {
            strict: { type: "boolean", description: "If true, returns entities not connected to 'Self' (directly or indirectly). Default: false" },
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name", "pagerank", "llmrank"], description: "Sort field for entities. Omit for insertion order." },
            sortDir: { type: "string", enum: ["asc", "desc"], description: "Sort direction. Default: desc for timestamps, asc for name." },
            cursor: { type: "number", description: "Cursor for pagination" },
          },
        },
      },
      {
        name: "validate_graph",
        description: "Validate the knowledge graph. Returns missing entities referenced in relations and observation limit violations (>2 observations or >140 chars). Each list is independently paginated (max 4096 chars per list).",
        inputSchema: {
          type: "object",
          properties: {
            entitiesCursor: { type: "number", description: "Cursor for the missingEntities list" },
            violationsCursor: { type: "number", description: "Cursor for the observationViolations list" },
          },
        },
      },
      {
        name: "decode_timestamp",
        description: "Decode a millisecond timestamp to human-readable UTC format. If no timestamp provided, returns the current time. Use this to interpret mtime/obsMtime values from entities.",
        inputSchema: {
          type: "object",
          properties: {
            timestamp: { type: "number", description: "Millisecond timestamp to decode. If omitted, returns current time." },
            relative: { type: "boolean", description: "If true, include relative time (e.g., '3 days ago'). Default: false" },
          },
        },
      },
      {
        name: "random_walk",
        description: "Perform a random walk from a starting entity, following random relations. Returns the terminal entity name and the path taken. Useful for serendipitous exploration of the knowledge graph.",
        inputSchema: {
          type: "object",
          properties: {
            start: { type: "string", description: "Name of the entity to start the walk from." },
            depth: { type: "number", description: "Number of steps to take. Default: 3" },
            seed: { type: "string", description: "Optional seed for reproducible walks." },
            direction: { type: "string", enum: ["forward", "backward", "any"], description: "Edge direction to follow. Default: forward" },
            mode: {
              type: "string",
              enum: ["merw", "uniform"],
              description:
                "Transition policy. 'merw' (default) weights each step by ψ (the cached Maximum-Entropy Random Walk eigenvector), biasing the walk toward structurally important nodes; falls back to uniform if ψ is not yet computed. 'uniform' samples each eligible neighbor with equal probability — useful for unbiased exploration or as a baseline for comparison.",
            },
          },
          required: ["start"],
        },
      },
      {
        name: "sequentialthinking",
        description: `Record a thought in the knowledge graph. Creates a Thought entity with observations and links it to the previous thought if provided. Returns the new thought's context ID for chaining.

Use this to build chains of reasoning that persist in the graph. Each thought can have up to 2 observations (max 140 chars each).`,
        inputSchema: {
          type: "object",
          properties: {
            previousCtxId: { 
              type: "string", 
              description: "Context ID of the previous thought to chain from. Omit for first thought in a chain." 
            },
            observations: { 
              type: "array", 
              items: { type: "string", maxLength: 140 },
              maxItems: 2,
              description: "Observations for this thought (max 2, each max 140 chars)" 
            },
          },
          required: ["observations"],
        },
      },
      {
        name: "kb_load",
        description: `Load a plaintext document into the knowledge graph. Chunks the text into entities connected by a doubly-linked chain, runs sentence TextRank to identify the most important sentences, and creates an index entity per key phrase that links directly to the chunk containing that sentence.

The file MUST be plaintext (.txt, .tex, .md, source code, etc.). For PDFs, use pdftotext first. For other binary formats, convert to text before calling this tool.`,
        inputSchema: {
          type: "object",
          properties: {
            filePath: {
              type: "string",
              description: "Absolute path to the plaintext file to load. Must have a plaintext extension (.txt, .tex, .md, .py, .ts, etc.).",
            },
            title: {
              type: "string",
              description: "Optional title for the document entity. Defaults to the filename without extension.",
            },
            topK: {
              type: "number",
              description: "Number of top-ranked sentences to highlight in the index (default: 15).",
            },
          },
          required: ["filePath"],
        },
      },
    ],
  };
});

  /**
   * Dispatch a single tool call. Extracted from the request handler so the
   * handler can wrap it with span/metric instrumentation without duplicating
   * the per-tool logic. Returns the MCP `CallToolResult` shape; thrown errors
   * become JSON-RPC protocol errors (hidden from the model), while
   * `{ isError: true }` returns are visible tool-level errors.
   */
  async function dispatch(name: string, args: Record<string, unknown>): Promise<ToolDispatchResult> {
    switch (name) {
      case "create_entities": {
        const result = await knowledgeGraphManager.createEntities(args.entities as Entity[]);
        knowledgeGraphManager.resample(); // Re-run structural sampling after graph mutation
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
      }
      case "create_relations": {
        const result = await knowledgeGraphManager.createRelations(args.relations as Relation[]);
        knowledgeGraphManager.resample(); // Re-run structural sampling after graph mutation
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
      }
      case "add_observations":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.addObservations(args.observations as { entityName: string; contents: string[] }[]), null, 2) }] };
      case "delete_entities":
        await knowledgeGraphManager.deleteEntities(args.entityNames as string[]);
        knowledgeGraphManager.resample(); // Re-run structural sampling after graph mutation
        return { content: [{ type: "text", text: "Entities deleted successfully" }] };
      case "delete_observations":
        await knowledgeGraphManager.deleteObservations(args.deletions as { entityName: string; observations: string[] }[]);
        return { content: [{ type: "text", text: "Observations deleted successfully" }] };
      case "delete_relations":
        await knowledgeGraphManager.deleteRelations(args.relations as Relation[]);
        knowledgeGraphManager.resample(); // Re-run structural sampling after graph mutation
        return { content: [{ type: "text", text: "Relations deleted successfully" }] };
      case "search_nodes": {
        const query = args.query as string;
        const graph = await knowledgeGraphManager.searchNodes(
          query,
          args.sortBy as EntitySortField | undefined,
          args.sortDir as SortDirection | undefined,
          (args.direction as 'forward' | 'backward' | 'any') ?? 'forward',
        );

        // Natural-language guard: literal queries (no regex metacharacters) that
        // produce zero matches are almost always the LLM mistaking this tool
        // for a vector-search/NL-search endpoint. Surface a tool-level error
        // (visible to the model) with a regex suggestion. Skip walker-visit
        // recording on this path so failed NL queries don't bias llmrank.
        if (!HAS_REGEX_META.test(query) && graph.entities.length === 0 && graph.relations.length === 0) {
          const suggested = query.trim().split(/\s+/).filter(Boolean).join('|');
          const suggestion = suggested && suggested !== query
            ? ` For multiple terms try ${JSON.stringify(suggested)}.`
            : '';
          return {
            content: [{
              type: "text",
              text: `No matches for ${JSON.stringify(query)}. search_nodes uses POSIX Extended Regular Expressions (ERE), case-sensitive — not natural language, and not JS/PCRE regex (use [0-9] not \\d, [[:alpha:]] not \\w; no lookahead or backreferences).${suggestion} You can also browse with get_entities_by_type, get_neighbors, or random_walk.`,
            }],
            isError: true,
          };
        }

        // Record walker visits for entities that will be returned to the LLM
        knowledgeGraphManager.recordWalkerVisits(graph.entities.map(e => e.name));
        return { content: [{ type: "text", text: JSON.stringify(paginateGraph(graph, args.entityCursor as number ?? 0, args.relationCursor as number ?? 0)) }] };
      }
      case "open_nodes": {
        const graph = await knowledgeGraphManager.openNodes(args.names as string[], (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
        // Record walker visits for opened nodes
        knowledgeGraphManager.recordWalkerVisits(graph.entities.map(e => e.name));
        return { content: [{ type: "text", text: JSON.stringify(paginateGraph(graph, args.entityCursor as number ?? 0, args.relationCursor as number ?? 0)) }] };
      }
      case "get_neighbors": {
        const neighbors = await knowledgeGraphManager.getNeighbors(args.entityName as string, args.depth as number ?? 1, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
        // Record walker visits for returned neighbors
        knowledgeGraphManager.recordWalkerVisits(neighbors.map(n => n.name));
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(neighbors, args.cursor as number ?? 0)) }] };
      }
      case "find_path": {
        const toEntityName = args.toEntity as string;
        const result = await knowledgeGraphManager.findPath(args.fromEntity as string, toEntityName, args.maxDepth as number, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
        const paginated = paginateItems(result.path, args.cursor as number ?? 0);
        // β-contract response. Pagination of `path` is unchanged; the
        // result wrapper additionally carries `targetReached` (did we
        // actually reach `toEntity`?), `budgetExhausted` (did we stop
        // because of memory pressure?), and `farthestDiscovered` (the
        // deepest node BFS expanded — anchor for a retry). A `note`
        // string explains the situation in natural language when the
        // search didn't reach the asked-for target, so the LLM can
        // adapt without needing to understand the schema beyond
        // reading the message.
        let note: string | undefined;
        if (result.budgetExhausted) {
          note = `find_path memory budget (${result.budgetBytes} bytes) was exhausted before reaching '${toEntityName}'. ` +
                 `The returned 'path' is a best-effort exploration that ended at ` +
                 `'${result.farthestDiscovered ?? '(no expansion)'}' — call find_path again with ` +
                 `fromEntity='${result.farthestDiscovered ?? args.fromEntity}' to continue the search, or set a smaller maxDepth.`;
        } else if (!result.targetReached) {
          note = result.farthestDiscovered === undefined
            ? `find_path could not expand any edges from '${args.fromEntity}' in direction '${(args.direction as string) ?? 'forward'}'. ` +
              `The 'path' is empty. Check the entity name and direction; the source may have no matching outgoing relations.`
            : `find_path could not reach '${toEntityName}' within maxDepth=${args.maxDepth ?? 5}. ` +
              `The returned 'path' is a best-effort exploration that ended at '${result.farthestDiscovered}'. ` +
              `Call find_path again with fromEntity='${result.farthestDiscovered}' to continue toward the target, ` +
              `or increase maxDepth.`;
        }
        return { content: [{ type: "text", text: JSON.stringify({
          ...paginated,
          targetReached: result.targetReached,
          budgetExhausted: result.budgetExhausted,
          ...(result.farthestDiscovered !== undefined && { farthestDiscovered: result.farthestDiscovered }),
          ...(note !== undefined && { note }),
        }) }] };
      }
      case "get_entities_by_type": {
        const entities = await knowledgeGraphManager.getEntitiesByType(args.entityType as string, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(entities, args.cursor as number ?? 0)) }] };
      }
      case "get_entity_types": {
        const types = await knowledgeGraphManager.getEntityTypes();
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(types, args.cursor as number ?? 0)) }] };
      }
      case "get_relation_types": {
        const types = await knowledgeGraphManager.getRelationTypes();
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(types, args.cursor as number ?? 0)) }] };
      }
      case "get_stats":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getStats(), null, 2) }] };
      case "get_orphaned_entities": {
        const entities = await knowledgeGraphManager.getOrphanedEntities(args.strict as boolean ?? false, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(entities, args.cursor as number ?? 0)) }] };
      }
      case "validate_graph": {
        const report = await knowledgeGraphManager.validateGraph();
        return { content: [{ type: "text", text: JSON.stringify({
          missingEntities: paginateItems(report.missingEntities, args.entitiesCursor as number ?? 0),
          observationViolations: paginateItems(report.observationViolations, args.violationsCursor as number ?? 0),
        }) }] };
      }
      case "decode_timestamp":
        return { content: [{ type: "text", text: JSON.stringify(knowledgeGraphManager.decodeTimestamp(args.timestamp as number | undefined, args.relative as boolean ?? false)) }] };
      case "random_walk": {
        const result = await knowledgeGraphManager.randomWalk(
          args.start as string,
          args.depth as number ?? 3,
          args.seed as string | undefined,
          (args.direction as 'forward' | 'backward' | 'any') ?? 'forward',
          (args.mode as RandomWalkMode) ?? 'merw',
        );
        return { content: [{ type: "text", text: JSON.stringify(result) }] };
      }
      case "sequentialthinking": {
        const result = await knowledgeGraphManager.addThought(
          args.observations as string[],
          args.previousCtxId as string | undefined
        );
        return { content: [{ type: "text", text: JSON.stringify(result) }] };
      }
      case "kb_load": {
        const filePath = args.filePath as string;

        // Validate extension
        validateExtension(filePath);

        // Read file
        let text: string;
        try {
          text = fs.readFileSync(filePath, 'utf-8');
        } catch (err: unknown) {
          throw new Error(`Failed to read file: ${err instanceof Error ? err.message : String(err)}`);
        }

        // Derive title
        const title = (args.title as string) ?? path.basename(filePath, path.extname(filePath));
        const topK = (args.topK as number) ?? 15;

        // Run the pipeline (reads string table under read lock)
        const loadResult = knowledgeGraphManager.prepareDocumentLoad(text, title, topK);

        // Insert into KB
        const entities = await knowledgeGraphManager.createEntities(
          loadResult.entities.map(e => ({
            name: e.name,
            entityType: e.entityType,
            observations: e.observations,
          }))
        );
        const relations = await knowledgeGraphManager.createRelations(
          loadResult.relations.map(r => ({
            from: r.from,
            to: r.to,
            relationType: r.relationType,
          }))
        );
        knowledgeGraphManager.resample();

        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              document: title,
              stats: loadResult.stats,
              entitiesCreated: entities.length,
              relationsCreated: relations.length,
            }, null, 2),
          }],
        };
      }
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args, _meta } = request.params;

    if (!args) {
      throw new Error(`No arguments provided for tool: ${name}`);
    }

    // Per MCP SEP-414 + OTel SemConv for MCP, clients propagate W3C Trace
    // Context (traceparent / tracestate / baggage) inside `params._meta`. The
    // SDK validates `_meta` with `z.core.$loose` so unknown keys pass through
    // untouched. Here we read them as a plain carrier object and let the
    // global propagator (W3CTraceContextPropagator + W3CBaggagePropagator,
    // registered by NodeSDK when enabled) extract a parent context. When the
    // SDK is disabled, propagation.extract is a no-op and the resulting span
    // is also a no-op — no overhead and no behavior change.
    const meta = (_meta ?? {}) as Record<string, unknown>;
    const carrier: Record<string, string> = {};
    if (typeof meta.traceparent === 'string') carrier.traceparent = meta.traceparent;
    if (typeof meta.tracestate  === 'string') carrier.tracestate  = meta.tracestate;
    if (typeof meta.baggage     === 'string') carrier.baggage     = meta.baggage;
    const parentCtx = propagation.extract(context.active(), carrier);

    return tracer.startActiveSpan(
      `tools/call ${name}`,
      {
        kind: SpanKind.SERVER,
        attributes: {
          'rpc.system': 'mcp',
          'rpc.service': 'tools',
          'rpc.method': name,
          'mcp.method': 'tools/call',
          'mcp.tool.name': name,
        },
      },
      parentCtx,
      async (span) => {
        const startNs = process.hrtime.bigint();
        let errorType: string | undefined;
        try {
          const result = await dispatch(name, args);
          if (result.isError) {
            errorType = 'tool_error';
            span.setAttribute('error.type', errorType);
            span.setStatus({ code: SpanStatusCode.ERROR, message: 'isError' });
          } else {
            span.setStatus({ code: SpanStatusCode.OK });
          }
          return result;
        } catch (err) {
          errorType = (err as Error)?.constructor?.name ?? 'Error';
          span.setAttribute('error.type', errorType);
          span.recordException(err as Error);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: (err as Error)?.message ?? String(err),
          });
          throw err;
        } finally {
          const seconds = Number(process.hrtime.bigint() - startNs) / 1e9;
          const histAttrs: Record<string, string> = {
            'rpc.system': 'mcp',
            'rpc.method': name,
          };
          if (errorType) histAttrs['error.type'] = errorType;
          toolDurationHistogram.record(seconds, histAttrs);
          span.end();
        }
      }
    );
  });

  return server;
}
