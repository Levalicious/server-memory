#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { context, propagation, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { randomBytes } from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { GraphFile, DIR_FORWARD, DIR_BACKWARD, type EntityRecord, type AdjEntry } from './src/graphfile.js';
import { StringTable } from './src/stringtable.js';
import { structuralSample } from './src/pagerank.js';
import { computeMerwPsi } from './src/merw.js';
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
  private gf: GraphFile;
  private st: StringTable;
  /** In-memory name→offset index, rebuilt from node log on every lock acquisition */
  private nameIndex: Map<string, bigint>;

  constructor(memoryFilePath: string = DEFAULT_MEMORY_FILE_PATH) {
    // Derive binary file paths from the base path
    const dir = path.dirname(memoryFilePath);
    const base = path.basename(memoryFilePath, path.extname(memoryFilePath));
    const graphPath = path.join(dir, `${base}.graph`);
    const strPath = path.join(dir, `${base}.strings`);

    // Subobject constructors self-lock around their own init/migration paths
    // (see StringTable / GraphFile constructors). They may grow the underlying
    // files, so we cannot hold an outer lock across these calls.
    this.st = new StringTable(strPath);
    this.gf = new GraphFile(graphPath, this.st);
    this.nameIndex = new Map();

    // Build the in-memory name index and run initial structural sampling +
    // MERW under an exclusive lock that covers BOTH the graph and strings
    // files. withWriteLock's prelude calls rebuildNameIndex() unconditionally,
    // so the name map is populated by the time the callback runs.
    // structuralSample + computeMerwPsi mutate the graph file; withWriteLock
    // syncs both files on exit.
    this.withWriteLock(() => {
      if (this.nameIndex.size > 0) {
        structuralSample(this.gf, 1, 0.85);
        computeMerwPsi(this.gf);
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
        const result = loadDocument(text, title, this.st, topK);
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
      this.gf.lockShared();
      const waitMs = Number(process.hrtime.bigint() - acquireStart) / 1e6;
      span.setAttribute('kb.lock.wait_ms', waitMs);
      try {
        this.gf.refresh();
        this.st.refresh();
        this.rebuildNameIndex();
        span.setAttribute('kb.entity_count', this.nameIndex.size);
        return fn();
      } finally {
        this.gf.unlock();
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
      this.gf.lockExclusive();
      const waitMs = Number(process.hrtime.bigint() - acquireStart) / 1e6;
      span.setAttribute('kb.lock.wait_ms', waitMs);
      try {
        this.gf.refresh();
        this.st.refresh();
        this.rebuildNameIndex();
        span.setAttribute('kb.entity_count', this.nameIndex.size);
        const result = fn();
        this.gf.sync();
        this.st.sync();
        return result;
      } finally {
        this.gf.unlock();
      }
    });
  }

  /** Rebuild the in-memory name→offset index from the node log. */
  private rebuildNameIndex(): void {
    this.nameIndex.clear();
    const offsets = this.gf.getAllEntityOffsets();
    for (const offset of offsets) {
      const rec = this.gf.readEntity(offset);
      const name = this.st.get(BigInt(rec.nameId));
      this.nameIndex.set(name, offset);
    }
  }

  /** Build rank maps from the binary store for pagerank/llmrank sorting.
   *  NOTE: Must be called inside a lock (read or write).
   *
   *  Always-on `kb.rank.read` child span: this is a hot path called from every
   *  read tool that sorts by rank, and surfaces the cost of reading the
   *  per-entity rank fields under the active lock.
   */
  private getRankMapsUnlocked(): { structural: Map<string, number>; walker: Map<string, number> } {
    return traced('kb.rank.read', { 'kb.entity_count': this.nameIndex.size }, () => {
      const structural = new Map<string, number>();
      const walker = new Map<string, number>();
      const structTotal = this.gf.getStructuralTotal();
      const walkerTotal = this.gf.getWalkerTotal();

      for (const [name, offset] of this.nameIndex) {
        const rec = this.gf.readEntity(offset);
        structural.set(name, structTotal > 0n ? Number(rec.structuralVisits) / Number(structTotal) : 0);
        walker.set(name, walkerTotal > 0n ? Number(rec.walkerVisits) / Number(walkerTotal) : 0);
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
          const offset = this.nameIndex.get(name);
          if (offset !== undefined) {
            this.gf.incrementWalkerVisit(offset);
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
      if (this.nameIndex.size === 0) return;
      traced(
        'kb.rank.pagerank',
        { 'kb.entity_count': this.nameIndex.size },
        () => structuralSample(this.gf, 1, 0.85),
      );
      traced(
        'kb.rank.merw',
        { 'kb.entity_count': this.nameIndex.size },
        () => computeMerwPsi(this.gf),
      );
    });
  }

  /** Convert an EntityRecord to the public Entity interface */
  private recordToEntity(rec: EntityRecord): Entity {
    const name = this.st.get(BigInt(rec.nameId));
    const entityType = this.st.get(BigInt(rec.typeId));
    const observations: string[] = [];
    if (rec.obs0Id !== 0) observations.push(this.st.get(BigInt(rec.obs0Id)));
    if (rec.obs1Id !== 0) observations.push(this.st.get(BigInt(rec.obs1Id)));

    const entity: Entity = { name, entityType, observations };
    const mtime = Number(rec.mtime);
    const obsMtime = Number(rec.obsMtime);
    if (mtime > 0) entity.mtime = mtime;
    if (obsMtime > 0) entity.obsMtime = obsMtime;
    return entity;
  }

  /** Get all entities as Entity objects (preserves node log order = insertion order) */
  private getAllEntities(): Entity[] {
    const offsets = this.gf.getAllEntityOffsets();
    return offsets.map(o => this.recordToEntity(this.gf.readEntity(o)));
  }

  /** Get all relations by scanning adjacency lists (forward edges only to avoid duplication) */
  private getAllRelations(): Relation[] {
    const relations: Relation[] = [];
    const offsets = this.gf.getAllEntityOffsets();
    for (const offset of offsets) {
      const rec = this.gf.readEntity(offset);
      const fromName = this.st.get(BigInt(rec.nameId));
      const edges = this.gf.getEdges(offset);
      for (const edge of edges) {
        if (edge.direction !== DIR_FORWARD) continue;
        const targetRec = this.gf.readEntity(edge.targetOffset);
        const toName = this.st.get(BigInt(targetRec.nameId));
        const relationType = this.st.get(BigInt(edge.relTypeId));
        const r: Relation = { from: fromName, to: toName, relationType };
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
        const existingOffset = this.nameIndex.get(e.name);
        if (existingOffset !== undefined) {
          const existing = this.recordToEntity(this.gf.readEntity(existingOffset));
          const sameType = existing.entityType === e.entityType;
          const sameObs = existing.observations.length === e.observations.length &&
            existing.observations.every((o, i) => o === e.observations[i]);
          if (sameType && sameObs) continue;
          throw new Error(`Entity "${e.name}" already exists with different data (type: "${existing.entityType}" vs "${e.entityType}", observations: ${existing.observations.length} vs ${e.observations.length})`);
        }

        const obsMtime = e.observations.length > 0 ? now : 0n;
        const rec = this.gf.createEntity(e.name, e.entityType, now, obsMtime);

        for (const obs of e.observations) {
          this.gf.addObservation(rec.offset, obs, now);
        }

        if (e.observations.length > 0) {
          const updated = this.gf.readEntity(rec.offset);
          updated.mtime = now;
          updated.obsMtime = now;
          this.gf.updateEntity(updated);
        }

        this.nameIndex.set(e.name, rec.offset);

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
      const fromOffsets = new Set<bigint>();

      for (const r of relations) {
        const fromOffset = this.nameIndex.get(r.from);
        const toOffset = this.nameIndex.get(r.to);
        if (fromOffset === undefined || toOffset === undefined) continue;

        const existingEdges = this.gf.getEdges(fromOffset);
        const relTypeId = Number(this.st.find(r.relationType) ?? -1n);
        const isDuplicate = existingEdges.some(e =>
          e.direction === DIR_FORWARD &&
          e.targetOffset === toOffset &&
          e.relTypeId === relTypeId
        );
        if (isDuplicate) continue;

        const rTypeId = Number(this.st.intern(r.relationType));
        const forwardEntry: AdjEntry = {
          targetOffset: toOffset,
          direction: DIR_FORWARD,
          relTypeId: rTypeId,
          mtime: now,
        };
        this.gf.addEdge(fromOffset, forwardEntry);

        const rTypeId2 = Number(this.st.intern(r.relationType));
        const backwardEntry: AdjEntry = {
          targetOffset: fromOffset,
          direction: DIR_BACKWARD,
          relTypeId: rTypeId2,
          mtime: now,
        };
        this.gf.addEdge(toOffset, backwardEntry);

        fromOffsets.add(fromOffset);
        newRelations.push({ ...r, mtime: Number(now) });
      }

      for (const offset of fromOffsets) {
        const rec = this.gf.readEntity(offset);
        rec.mtime = now;
        this.gf.updateEntity(rec);
      }

      return newRelations;
    });
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
    return this.withWriteLock(() => {
      const results: { entityName: string; addedObservations: string[] }[] = [];

      for (const o of observations) {
        const offset = this.nameIndex.get(o.entityName);
        if (offset === undefined) {
          throw new Error(`Entity with name ${o.entityName} not found`);
        }

        for (const obs of o.contents) {
          if (obs.length > 140) {
            throw new Error(`Observation for "${o.entityName}" exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
          }
        }

        const rec = this.gf.readEntity(offset);
        const existingObs: string[] = [];
        if (rec.obs0Id !== 0) existingObs.push(this.st.get(BigInt(rec.obs0Id)));
        if (rec.obs1Id !== 0) existingObs.push(this.st.get(BigInt(rec.obs1Id)));

        const newObservations = o.contents.filter(content => !existingObs.includes(content));

        if (existingObs.length + newObservations.length > 2) {
          throw new Error(`Adding ${newObservations.length} observations to "${o.entityName}" would exceed limit of 2 (currently has ${existingObs.length}).`);
        }

        const now = BigInt(Date.now());
        for (const obs of newObservations) {
          this.gf.addObservation(offset, obs, now);
        }

        results.push({ entityName: o.entityName, addedObservations: newObservations });
      }

      return results;
    });
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    this.withWriteLock(() => {
      for (const name of entityNames) {
        const offset = this.nameIndex.get(name);
        if (offset === undefined) continue;

        const edges = this.gf.getEdges(offset);
        for (const edge of edges) {
          const reverseDir = edge.direction === DIR_FORWARD ? DIR_BACKWARD : DIR_FORWARD;
          this.gf.removeEdge(edge.targetOffset, offset, edge.relTypeId, reverseDir);
          this.st.release(BigInt(edge.relTypeId));
        }

        this.gf.deleteEntity(offset);
        this.nameIndex.delete(name);
      }
    });
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    this.withWriteLock(() => {
      const now = BigInt(Date.now());
      for (const d of deletions) {
        const offset = this.nameIndex.get(d.entityName);
        if (offset === undefined) continue;

        for (const obs of d.observations) {
          this.gf.removeObservation(offset, obs, now);
        }
      }
    });
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    this.withWriteLock(() => {
      for (const r of relations) {
        const fromOffset = this.nameIndex.get(r.from);
        const toOffset = this.nameIndex.get(r.to);
        if (fromOffset === undefined || toOffset === undefined) continue;

        const relTypeId = this.st.find(r.relationType);
        if (relTypeId === null) continue;

        const removedForward = this.gf.removeEdge(fromOffset, toOffset, Number(relTypeId), DIR_FORWARD);
        if (removedForward) this.st.release(relTypeId);

        const removedBackward = this.gf.removeEdge(toOffset, fromOffset, Number(relTypeId), DIR_BACKWARD);
        if (removedBackward) this.st.release(relTypeId);
      }
    });
  }

  // Regex-based search function
  async searchNodes(query: string, sortBy?: EntitySortField, sortDir?: SortDirection, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<KnowledgeGraph> {
    let regex: RegExp;
    try {
      regex = new RegExp(query, 'i');
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
        const allEntities = this.getAllEntities();

        const filteredEntities = allEntities.filter(e =>
          regex.test(e.name) ||
          regex.test(e.entityType) ||
          e.observations.some(o => regex.test(o))
        );

        const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

        const allRelations = this.getAllRelations();
        const filteredRelations = allRelations.filter(r => {
          if (direction === 'forward') return filteredEntityNames.has(r.from);
          if (direction === 'backward') return filteredEntityNames.has(r.to);
          return filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to);
        });

        // Aggregate scan/match stats — the LLM-visible knobs that explain why
        // a query was slow: how big the haystack is and how big the result.
        span.setAttribute('kb.search.scanned.entities', allEntities.length);
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
      for (const name of names) {
        const offset = this.nameIndex.get(name);
        if (offset === undefined) continue;
        filteredEntities.push(this.recordToEntity(this.gf.readEntity(offset)));
      }

      const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

      const filteredRelations: Relation[] = [];
      for (const name of filteredEntityNames) {
        const offset = this.nameIndex.get(name)!;
        const edges = this.gf.getEdges(offset);
        for (const edge of edges) {
          if (edge.direction !== DIR_FORWARD && edge.direction !== DIR_BACKWARD) continue;

          const targetRec = this.gf.readEntity(edge.targetOffset);
          const targetName = this.st.get(BigInt(targetRec.nameId));
          const relationType = this.st.get(BigInt(edge.relTypeId));
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
        const startOffset = this.nameIndex.get(entityName);
        if (startOffset === undefined) {
          span.setAttribute('kb.traversal.start_found', false);
          return [];
        }
        span.setAttribute('kb.traversal.start_found', true);

        const visited = new Set<string>();
        const neighborNames = new Set<string>();
        let edgesScanned = 0;

        const traverse = (currentName: string, currentDepth: number): void => {
          if (currentDepth > depth || visited.has(currentName)) return;
          visited.add(currentName);

          const offset = this.nameIndex.get(currentName);
          if (offset === undefined) return;

          const edges = this.gf.getEdges(offset);
          for (const edge of edges) {
            edgesScanned++;
            if (direction === 'forward' && edge.direction !== DIR_FORWARD) continue;
            if (direction === 'backward' && edge.direction !== DIR_BACKWARD) continue;

            const targetRec = this.gf.readEntity(edge.targetOffset);
            const neighborName = this.st.get(BigInt(targetRec.nameId));
            neighborNames.add(neighborName);

            if (currentDepth < depth) {
              traverse(neighborName, currentDepth + 1);
            }
          }
        };

        traverse(entityName, 0);
        neighborNames.delete(entityName);

        const neighbors: Neighbor[] = Array.from(neighborNames).map(name => {
          const offset = this.nameIndex.get(name);
          if (!offset) return { name };
          const rec = this.gf.readEntity(offset);
          const mtime = Number(rec.mtime);
          const obsMtime = Number(rec.obsMtime);
          const n: Neighbor = { name };
          if (mtime > 0) n.mtime = mtime;
          if (obsMtime > 0) n.obsMtime = obsMtime;
          return n;
        });

        // Aggregate traversal stats: the size of the BFS frontier the
        // resulting neighbor set, and how many edges we walked through. Cheap
        // to compute and explains slow calls without per-step span fanout.
        span.setAttribute('kb.traversal.visited_count', visited.size);
        span.setAttribute('kb.traversal.neighbor_count', neighbors.length);
        span.setAttribute('kb.traversal.edges_scanned', edgesScanned);

        const rankMaps = this.getRankMapsUnlocked();
        return sortNeighbors(neighbors, sortBy, sortDir, rankMaps);
      }),
    );
  }

  async findPath(fromEntity: string, toEntity: string, maxDepth: number = 5, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<Relation[]> {
    return traced(
      'kb.find_path',
      {
        'kb.traversal.max_depth': maxDepth,
        'kb.traversal.direction': direction,
      },
      (span) => this.withReadLock(() => {
        const visited = new Set<string>();
        let edgesScanned = 0;
        let nodesExpanded = 0;

        const dfs = (current: string, target: string, pathSoFar: Relation[], depth: number): Relation[] | null => {
          if (depth > maxDepth || visited.has(current)) return null;
          if (current === target) return pathSoFar;

          visited.add(current);
          nodesExpanded++;

          const offset = this.nameIndex.get(current);
          if (offset === undefined) { visited.delete(current); return null; }

          const edges = this.gf.getEdges(offset);
          for (const edge of edges) {
            edgesScanned++;
            if (direction === 'forward' && edge.direction !== DIR_FORWARD) continue;
            if (direction === 'backward' && edge.direction !== DIR_BACKWARD) continue;

            const targetRec = this.gf.readEntity(edge.targetOffset);
            const nextName = this.st.get(BigInt(targetRec.nameId));
            const relationType = this.st.get(BigInt(edge.relTypeId));
            const mtime = Number(edge.mtime);

            let rel: Relation;
            if (edge.direction === DIR_FORWARD) {
              rel = { from: current, to: nextName, relationType };
            } else {
              rel = { from: nextName, to: current, relationType };
            }
            if (mtime > 0) rel.mtime = mtime;

            const result = dfs(nextName, target, [...pathSoFar, rel], depth + 1);
            if (result) return result;
          }

          visited.delete(current);
          return null;
        };

        const path = dfs(fromEntity, toEntity, [], 0) || [];
        span.setAttribute('kb.traversal.nodes_expanded', nodesExpanded);
        span.setAttribute('kb.traversal.edges_scanned', edgesScanned);
        span.setAttribute('kb.traversal.path_length', path.length);
        span.setAttribute('kb.traversal.path_found', path.length > 0);
        return path;
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
        const startOffset = this.nameIndex.get(start);
        if (!startOffset) {
          throw new Error(`Start entity not found: ${start}`);
        }

        // Create seeded RNG if seed provided
        let rngState = seed ? this.hashSeed(seed) : null;
        const random = (): number => {
          if (rngState !== null) {
            rngState ^= rngState << 13;
            rngState ^= rngState >>> 17;
            rngState ^= rngState << 5;
            return (rngState >>> 0) / 0xFFFFFFFF;
          } else {
            return randomBytes(4).readUInt32BE() / 0xFFFFFFFF;
          }
        };

        const pathNames: string[] = [start];
        let current = start;
        let edgesScanned = 0;
        let truncated = false;

        for (let i = 0; i < depth; i++) {
          const offset = this.nameIndex.get(current);
          if (!offset) { truncated = true; break; }

          const edges = this.gf.getEdges(offset);
          const candidates: { name: string; psi: number }[] = [];

          for (const edge of edges) {
            edgesScanned++;
            if (direction === 'forward' && edge.direction !== DIR_FORWARD) continue;
            if (direction === 'backward' && edge.direction !== DIR_BACKWARD) continue;

            const targetRec = this.gf.readEntity(edge.targetOffset);
            const neighborName = this.st.get(BigInt(targetRec.nameId));
            if (neighborName !== current && this.nameIndex.has(neighborName)) {
              candidates.push({ name: neighborName, psi: targetRec.psi });
            }
          }

          // Deduplicate: keep max psi per name (multiple edge types to same target)
          const byName = new Map<string, number>();
          for (const c of candidates) {
            const existing = byName.get(c.name);
            if (existing === undefined || c.psi > existing) {
              byName.set(c.name, c.psi);
            }
          }

          if (byName.size === 0) { truncated = true; break; }

          const neighborArr = Array.from(byName.entries());

          // Compute total ψ once; both modes need it (uniform skips it, but
          // we still want to know whether ψ is populated for telemetry).
          let totalPsi = 0;
          for (const [, psi] of neighborArr) totalPsi += psi;

          let chosen: string;
          if (mode === 'merw' && totalPsi > 0) {
            // MERW-weighted sampling: probability proportional to ψ_j.
            // (The ψ_i denominator is constant for all neighbors and
            //  cancels in normalization.)
            const r = random() * totalPsi;
            let cumulative = 0;
            chosen = neighborArr[neighborArr.length - 1][0]; // fallback
            for (const [name, psi] of neighborArr) {
              cumulative += psi;
              if (r <= cumulative) {
                chosen = name;
                break;
              }
            }
          } else {
            // mode === 'uniform', or MERW with ψ not yet computed (all zero).
            // Plain uniform sampling over the deduplicated neighbor set.
            const idx = Math.floor(random() * neighborArr.length);
            chosen = neighborArr[idx][0];
          }

          current = chosen;
          pathNames.push(current);
        }

        span.setAttribute('kb.walker.steps_taken', pathNames.length - 1);
        span.setAttribute('kb.walker.edges_scanned', edgesScanned);
        span.setAttribute('kb.walker.truncated', truncated);
        return { entity: current, path: pathNames };
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

      const obsMtime = observations.length > 0 ? now : 0n;
      const rec = this.gf.createEntity(ctxId, 'Thought', now, obsMtime);
      for (const obs of observations) {
        this.gf.addObservation(rec.offset, obs, now);
      }
      if (observations.length > 0) {
        const updated = this.gf.readEntity(rec.offset);
        updated.mtime = now;
        updated.obsMtime = now;
        this.gf.updateEntity(updated);
      }
      this.nameIndex.set(ctxId, rec.offset);

      if (previousCtxId) {
        const prevOffset = this.nameIndex.get(previousCtxId);
        if (prevOffset !== undefined) {
          const prevRec = this.gf.readEntity(prevOffset);
          prevRec.mtime = now;
          this.gf.updateEntity(prevRec);

          const followsTypeId = Number(this.st.intern('follows'));
          this.gf.addEdge(prevOffset, { targetOffset: rec.offset, direction: DIR_FORWARD, relTypeId: followsTypeId, mtime: now });
          const followsTypeId2 = Number(this.st.intern('follows'));
          this.gf.addEdge(rec.offset, { targetOffset: prevOffset, direction: DIR_BACKWARD, relTypeId: followsTypeId2, mtime: now });

          const precededByTypeId = Number(this.st.intern('preceded_by'));
          this.gf.addEdge(rec.offset, { targetOffset: prevOffset, direction: DIR_FORWARD, relTypeId: precededByTypeId, mtime: now });
          const precededByTypeId2 = Number(this.st.intern('preceded_by'));
          this.gf.addEdge(prevOffset, { targetOffset: rec.offset, direction: DIR_BACKWARD, relTypeId: precededByTypeId2, mtime: now });
        }
      }

      return { ctxId };
    });
  }

  /** Close the underlying binary store files */
  close(): void {
    this.gf.close();
    this.st.close();
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
    version: "0.0.23",
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
        description: "Get all unique entity types in the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {},
        },
      },
      {
        name: "get_relation_types",
        description: "Get all unique relation types in the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {},
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
        description: "Validate the knowledge graph. Returns missing entities referenced in relations and observation limit violations (>2 observations or >140 chars).",
        inputSchema: {
          type: "object",
          properties: {},
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
        const graph = await knowledgeGraphManager.searchNodes(query, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');

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
              text: `No matches for ${JSON.stringify(query)}. search_nodes uses regex (case-insensitive), not natural language.${suggestion} You can also browse with get_entities_by_type, get_neighbors, or random_walk.`,
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
        const found = await knowledgeGraphManager.findPath(args.fromEntity as string, args.toEntity as string, args.maxDepth as number, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(found, args.cursor as number ?? 0)) }] };
      }
      case "get_entities_by_type": {
        const entities = await knowledgeGraphManager.getEntitiesByType(args.entityType as string, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(entities, args.cursor as number ?? 0)) }] };
      }
      case "get_entity_types":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getEntityTypes(), null, 2) }] };
      case "get_relation_types":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getRelationTypes(), null, 2) }] };
      case "get_stats":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getStats(), null, 2) }] };
      case "get_orphaned_entities": {
        const entities = await knowledgeGraphManager.getOrphanedEntities(args.strict as boolean ?? false, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
        return { content: [{ type: "text", text: JSON.stringify(paginateItems(entities, args.cursor as number ?? 0)) }] };
      }
      case "validate_graph":
        return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.validateGraph(), null, 2) }] };
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
