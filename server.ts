#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { randomBytes } from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { GraphFile, DIR_FORWARD, DIR_BACKWARD, EntityRecord, AdjEntry } from './src/graphfile.js';
import { StringTable } from './src/stringtable.js';
import { structuralSample } from './src/pagerank.js';

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

export const MAX_CHARS = 2048;

function paginateItems<T>(items: T[], cursor: number = 0, maxChars: number = MAX_CHARS): PaginatedResult<T> {
  const result: T[] = [];
  let i = cursor;
  
  // Calculate overhead for wrapper: {"items":[],"nextCursor":null,"totalCount":123}
  const wrapperTemplate = { items: [] as T[], nextCursor: null as number | null, totalCount: items.length };
  let overhead = JSON.stringify(wrapperTemplate).length;
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
  // Build incrementally, measuring actual serialized size
  const entityCount = graph.entities.length;
  const relationCount = graph.relations.length;
  
  // Start with empty result to measure base overhead
  const emptyResult = {
    entities: { items: [] as Entity[], nextCursor: null as number | null, totalCount: entityCount },
    relations: { items: [] as Relation[], nextCursor: null as number | null, totalCount: relationCount }
  };
  let currentSize = JSON.stringify(emptyResult).length;
  
  const resultEntities: Entity[] = [];
  const resultRelations: Relation[] = [];
  let entityIdx = entityCursor;
  let relationIdx = relationCursor;
  
  // Add entities until we hit the limit
  while (entityIdx < graph.entities.length) {
    const entity = graph.entities[entityIdx];
    const entityJson = JSON.stringify(entity);
    const addedChars = entityJson.length + (resultEntities.length > 0 ? 1 : 0);
    
    if (currentSize + addedChars > MAX_CHARS) {
      break;
    }
    
    resultEntities.push(entity);
    currentSize += addedChars;
    entityIdx++;
  }
  
  // Add relations with remaining space
  while (relationIdx < graph.relations.length) {
    const relation = graph.relations[relationIdx];
    const relationJson = JSON.stringify(relation);
    const addedChars = relationJson.length + (resultRelations.length > 0 ? 1 : 0);
    
    if (currentSize + addedChars > MAX_CHARS) {
      break;
    }
    
    resultRelations.push(relation);
    currentSize += addedChars;
    relationIdx++;
  }
  
  return {
    entities: {
      items: resultEntities,
      nextCursor: entityIdx < graph.entities.length ? entityIdx : null,
      totalCount: entityCount
    },
    relations: {
      items: resultRelations,
      nextCursor: relationIdx < graph.relations.length ? relationIdx : null,
      totalCount: relationCount
    }
  };
}

// The KnowledgeGraphManager class contains all operations to interact with the knowledge graph
export class KnowledgeGraphManager {
  private gf: GraphFile;
  private st: StringTable;
  /** In-memory name→offset index, rebuilt from node log on open */
  private nameIndex: Map<string, bigint>;

  constructor(memoryFilePath: string = DEFAULT_MEMORY_FILE_PATH) {
    // Derive binary file paths from the base path
    const dir = path.dirname(memoryFilePath);
    const base = path.basename(memoryFilePath, path.extname(memoryFilePath));
    const graphPath = path.join(dir, `${base}.graph`);
    const strPath = path.join(dir, `${base}.strings`);

    this.st = new StringTable(strPath);
    this.gf = new GraphFile(graphPath, this.st);
    this.nameIndex = new Map();
    this.rebuildNameIndex();

    // Run initial structural sampling if graph is non-empty
    if (this.nameIndex.size > 0) {
      structuralSample(this.gf, 1, 0.85);
    }
  }

  /** Rebuild the in-memory name→offset index from the node log */
  private rebuildNameIndex(): void {
    this.nameIndex.clear();
    const offsets = this.gf.getAllEntityOffsets();
    for (const offset of offsets) {
      const rec = this.gf.readEntity(offset);
      const name = this.st.get(BigInt(rec.nameId));
      this.nameIndex.set(name, offset);
    }
  }

  /** Build rank maps from the binary store for pagerank/llmrank sorting */
  getRankMaps(): { structural: Map<string, number>; walker: Map<string, number> } {
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
  }

  /** Increment walker visit count for a list of entity names */
  recordWalkerVisits(names: string[]): void {
    for (const name of names) {
      const offset = this.nameIndex.get(name);
      if (offset !== undefined) {
        this.gf.incrementWalkerVisit(offset);
      }
    }
  }

  /** Re-run structural sampling (call after graph mutations) */
  resample(): void {
    if (this.nameIndex.size > 0) {
      structuralSample(this.gf, 1, 0.85);
    }
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
    // Validate observation limits
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

    const now = BigInt(Date.now());
    const newEntities: Entity[] = [];

    for (const e of entities) {
      const existingOffset = this.nameIndex.get(e.name);
      if (existingOffset !== undefined) {
        // Check for exact match — skip silently if identical
        const existing = this.recordToEntity(this.gf.readEntity(existingOffset));
        const sameType = existing.entityType === e.entityType;
        const sameObs = existing.observations.length === e.observations.length &&
          existing.observations.every((o, i) => o === e.observations[i]);
        if (sameType && sameObs) continue; // exact match, skip
        throw new Error(`Entity "${e.name}" already exists with different data (type: "${existing.entityType}" vs "${e.entityType}", observations: ${existing.observations.length} vs ${e.observations.length})`);
      }

      const obsMtime = e.observations.length > 0 ? now : 0n;
      const rec = this.gf.createEntity(e.name, e.entityType, now, obsMtime);

      // Add observations
      for (const obs of e.observations) {
        this.gf.addObservation(rec.offset, obs, now);
      }

      // Fix mtime back (addObservation clobbers it)
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

    this.gf.sync();
    return newEntities;
  }

  async createRelations(relations: Relation[]): Promise<Relation[]> {
    const now = BigInt(Date.now());
    const newRelations: Relation[] = [];

    // Collect 'from' entity offsets to update mtime
    const fromOffsets = new Set<bigint>();

    for (const r of relations) {
      const fromOffset = this.nameIndex.get(r.from);
      const toOffset = this.nameIndex.get(r.to);
      if (fromOffset === undefined || toOffset === undefined) continue;

      // Check for duplicate
      const existingEdges = this.gf.getEdges(fromOffset);
      const relTypeId = Number(this.st.find(r.relationType) ?? -1n);
      const isDuplicate = existingEdges.some(e =>
        e.direction === DIR_FORWARD &&
        e.targetOffset === toOffset &&
        e.relTypeId === relTypeId
      );
      if (isDuplicate) continue;

      // Intern the relation type (needs refcount)
      const rTypeId = Number(this.st.intern(r.relationType));

      // Add forward edge on 'from' entity
      const forwardEntry: AdjEntry = {
        targetOffset: toOffset,
        direction: DIR_FORWARD,
        relTypeId: rTypeId,
        mtime: now,
      };
      this.gf.addEdge(fromOffset, forwardEntry);

      // Add backward edge on 'to' entity
      // Intern again to bump refcount for the backward edge
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

    // Update mtime on 'from' entities
    for (const offset of fromOffsets) {
      const rec = this.gf.readEntity(offset);
      rec.mtime = now;
      this.gf.updateEntity(rec);
    }

    this.gf.sync();
    return newRelations;
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
    const results: { entityName: string; addedObservations: string[] }[] = [];

    for (const o of observations) {
      const offset = this.nameIndex.get(o.entityName);
      if (offset === undefined) {
        throw new Error(`Entity with name ${o.entityName} not found`);
      }

      // Validate observation character limits
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

      // Validate total observation count
      if (existingObs.length + newObservations.length > 2) {
        throw new Error(`Adding ${newObservations.length} observations to "${o.entityName}" would exceed limit of 2 (currently has ${existingObs.length}).`);
      }

      const now = BigInt(Date.now());
      for (const obs of newObservations) {
        this.gf.addObservation(offset, obs, now);
      }

      results.push({ entityName: o.entityName, addedObservations: newObservations });
    }

    this.gf.sync();
    return results;
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    for (const name of entityNames) {
      const offset = this.nameIndex.get(name);
      if (offset === undefined) continue;

      // Remove all edges that reference this entity from OTHER entities' adj lists
      const edges = this.gf.getEdges(offset);
      for (const edge of edges) {
        // Remove the reverse edge from the other entity
        const reverseDir = edge.direction === DIR_FORWARD ? DIR_BACKWARD : DIR_FORWARD;
        this.gf.removeEdge(edge.targetOffset, offset, edge.relTypeId, reverseDir);
        // Release the relType string ref for the reverse edge
        this.st.release(BigInt(edge.relTypeId));
      }

      this.gf.deleteEntity(offset);
      this.nameIndex.delete(name);
    }

    this.gf.sync();
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    const now = BigInt(Date.now());
    for (const d of deletions) {
      const offset = this.nameIndex.get(d.entityName);
      if (offset === undefined) continue;

      for (const obs of d.observations) {
        this.gf.removeObservation(offset, obs, now);
      }
    }

    this.gf.sync();
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    for (const r of relations) {
      const fromOffset = this.nameIndex.get(r.from);
      const toOffset = this.nameIndex.get(r.to);
      if (fromOffset === undefined || toOffset === undefined) continue;

      const relTypeId = this.st.find(r.relationType);
      if (relTypeId === null) continue;

      // Remove forward edge from 'from'
      const removedForward = this.gf.removeEdge(fromOffset, toOffset, Number(relTypeId), DIR_FORWARD);
      if (removedForward) this.st.release(relTypeId);

      // Remove backward edge from 'to'
      const removedBackward = this.gf.removeEdge(toOffset, fromOffset, Number(relTypeId), DIR_BACKWARD);
      if (removedBackward) this.st.release(relTypeId);
    }

    this.gf.sync();
  }

  // Regex-based search function
  async searchNodes(query: string, sortBy?: EntitySortField, sortDir?: SortDirection, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<KnowledgeGraph> {
    let regex: RegExp;
    try {
      regex = new RegExp(query, 'i'); // case-insensitive
    } catch (e) {
      throw new Error(`Invalid regex pattern: ${query}`);
    }

    const allEntities = this.getAllEntities();

    // Filter entities
    const filteredEntities = allEntities.filter(e =>
      regex.test(e.name) ||
      regex.test(e.entityType) ||
      e.observations.some(o => regex.test(o))
    );

    // Create a Set of filtered entity names for quick lookup
    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

    // Get all relations and filter based on direction
    const allRelations = this.getAllRelations();
    const filteredRelations = allRelations.filter(r => {
      if (direction === 'forward') return filteredEntityNames.has(r.from);
      if (direction === 'backward') return filteredEntityNames.has(r.to);
      return filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to); // 'any' = both endpoints in set
    });

    const rankMaps = this.getRankMaps();
    return {
      entities: sortEntities(filteredEntities, sortBy, sortDir, rankMaps),
      relations: filteredRelations,
    };
  }

  async openNodes(names: string[], direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<KnowledgeGraph> {
    const filteredEntities: Entity[] = [];
    for (const name of names) {
      const offset = this.nameIndex.get(name);
      if (offset === undefined) continue;
      filteredEntities.push(this.recordToEntity(this.gf.readEntity(offset)));
    }

    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));

    // Collect relations from these entities
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
          // DIR_BACKWARD: this entity is 'to', target is 'from'
          if (direction === 'forward') continue;
          if (direction === 'any' && !filteredEntityNames.has(targetName)) continue;
          const r: Relation = { from: targetName, to: name, relationType };
          if (mtime > 0) r.mtime = mtime;
          filteredRelations.push(r);
        }
      }
    }

    return { entities: filteredEntities, relations: filteredRelations };
  }

  async getNeighbors(
    entityName: string,
    depth: number = 1,
    sortBy?: EntitySortField,
    sortDir?: SortDirection,
    direction: 'forward' | 'backward' | 'any' = 'forward'
  ): Promise<Neighbor[]> {
    const startOffset = this.nameIndex.get(entityName);
    if (startOffset === undefined) return [];

    const visited = new Set<string>();
    const neighborNames = new Set<string>();

    const traverse = (currentName: string, currentDepth: number) => {
      if (currentDepth > depth || visited.has(currentName)) return;
      visited.add(currentName);

      const offset = this.nameIndex.get(currentName);
      if (offset === undefined) return;

      const edges = this.gf.getEdges(offset);
      for (const edge of edges) {
        // Filter by direction
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

    // Remove the starting entity from neighbors
    neighborNames.delete(entityName);

    // Build neighbor objects with timestamps
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

    const rankMaps = this.getRankMaps();
    return sortNeighbors(neighbors, sortBy, sortDir, rankMaps);
  }

  async findPath(fromEntity: string, toEntity: string, maxDepth: number = 5, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<Relation[]> {
    const visited = new Set<string>();

    const dfs = (current: string, target: string, pathSoFar: Relation[], depth: number): Relation[] | null => {
      if (depth > maxDepth || visited.has(current)) return null;
      if (current === target) return pathSoFar;

      visited.add(current);

      const offset = this.nameIndex.get(current);
      if (offset === undefined) { visited.delete(current); return null; }

      const edges = this.gf.getEdges(offset);
      for (const edge of edges) {
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

    return dfs(fromEntity, toEntity, [], 0) || [];
  }

  async getEntitiesByType(entityType: string, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    const filtered = this.getAllEntities().filter(e => e.entityType === entityType);
    const rankMaps = this.getRankMaps();
    return sortEntities(filtered, sortBy, sortDir, rankMaps);
  }

  async getEntityTypes(): Promise<string[]> {
    const types = new Set(this.getAllEntities().map(e => e.entityType));
    return Array.from(types).sort();
  }

  async getRelationTypes(): Promise<string[]> {
    const types = new Set(this.getAllRelations().map(r => r.relationType));
    return Array.from(types).sort();
  }

  async getStats(): Promise<{ entityCount: number; relationCount: number; entityTypes: number; relationTypes: number }> {
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
  }

  async getOrphanedEntities(strict: boolean = false, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    const entities = this.getAllEntities();

    if (!strict) {
      // Simple mode: entities with no relations at all
      const connectedEntityNames = new Set<string>();
      const relations = this.getAllRelations();
      relations.forEach(r => {
        connectedEntityNames.add(r.from);
        connectedEntityNames.add(r.to);
      });
      const orphans = entities.filter(e => !connectedEntityNames.has(e.name));
      const rankMaps = this.getRankMaps();
      return sortEntities(orphans, sortBy, sortDir, rankMaps);
    }

    // Strict mode: entities not connected to "Self" (directly or indirectly)
    const neighbors = new Map<string, Set<string>>();
    entities.forEach(e => neighbors.set(e.name, new Set()));
    const relations = this.getAllRelations();
    relations.forEach(r => {
      neighbors.get(r.from)?.add(r.to);
      neighbors.get(r.to)?.add(r.from);
    });

    // BFS from Self
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
    const rankMaps = this.getRankMaps();
    return sortEntities(orphans, sortBy, sortDir, rankMaps);
  }

  async validateGraph(): Promise<{ missingEntities: string[]; observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] }> {
    const entities = this.getAllEntities();
    const relations = this.getAllRelations();
    const entityNames = new Set(entities.map(e => e.name));
    const missingEntities = new Set<string>();
    const observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] = [];

    // Check for missing entities in relations
    relations.forEach(r => {
      if (!entityNames.has(r.from)) missingEntities.add(r.from);
      if (!entityNames.has(r.to)) missingEntities.add(r.to);
    });

    // Check for observation limit violations
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
  }

  async randomWalk(start: string, depth: number = 3, seed?: string, direction: 'forward' | 'backward' | 'any' = 'forward'): Promise<{ entity: string; path: string[] }> {
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

    for (let i = 0; i < depth; i++) {
      const offset = this.nameIndex.get(current);
      if (!offset) break;

      const edges = this.gf.getEdges(offset);
      const validNeighbors = new Set<string>();

      for (const edge of edges) {
        if (direction === 'forward' && edge.direction !== DIR_FORWARD) continue;
        if (direction === 'backward' && edge.direction !== DIR_BACKWARD) continue;

        const targetRec = this.gf.readEntity(edge.targetOffset);
        const neighborName = this.st.get(BigInt(targetRec.nameId));
        if (neighborName !== current) validNeighbors.add(neighborName);
      }

      const neighborArr = Array.from(validNeighbors).filter(n => this.nameIndex.has(n));
      if (neighborArr.length === 0) break;

      const idx = Math.floor(random() * neighborArr.length);
      current = neighborArr[idx];
      pathNames.push(current);
    }

    return { entity: current, path: pathNames };
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
    // Validate observations
    if (observations.length > 2) {
      throw new Error(`Thought has ${observations.length} observations. Maximum allowed is 2.`);
    }
    for (const obs of observations) {
      if (obs.length > 140) {
        throw new Error(`Observation exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
      }
    }

    const now = BigInt(Date.now());
    const ctxId = randomBytes(12).toString('hex');

    // Create thought entity
    const obsMtime = observations.length > 0 ? now : 0n;
    const rec = this.gf.createEntity(ctxId, 'Thought', now, obsMtime);
    for (const obs of observations) {
      this.gf.addObservation(rec.offset, obs, now);
    }
    // Fix mtime
    if (observations.length > 0) {
      const updated = this.gf.readEntity(rec.offset);
      updated.mtime = now;
      updated.obsMtime = now;
      this.gf.updateEntity(updated);
    }
    this.nameIndex.set(ctxId, rec.offset);

    // Link to previous thought if it exists
    if (previousCtxId) {
      const prevOffset = this.nameIndex.get(previousCtxId);
      if (prevOffset !== undefined) {
        // Update mtime on previous entity
        const prevRec = this.gf.readEntity(prevOffset);
        prevRec.mtime = now;
        this.gf.updateEntity(prevRec);

        // follows: previous -> new
        const followsTypeId = Number(this.st.intern('follows'));
        this.gf.addEdge(prevOffset, { targetOffset: rec.offset, direction: DIR_FORWARD, relTypeId: followsTypeId, mtime: now });
        const followsTypeId2 = Number(this.st.intern('follows'));
        this.gf.addEdge(rec.offset, { targetOffset: prevOffset, direction: DIR_BACKWARD, relTypeId: followsTypeId2, mtime: now });

        // preceded_by: new -> previous
        const precededByTypeId = Number(this.st.intern('preceded_by'));
        this.gf.addEdge(rec.offset, { targetOffset: prevOffset, direction: DIR_FORWARD, relTypeId: precededByTypeId, mtime: now });
        const precededByTypeId2 = Number(this.st.intern('preceded_by'));
        this.gf.addEdge(prevOffset, { targetOffset: rec.offset, direction: DIR_BACKWARD, relTypeId: precededByTypeId2, mtime: now });
      }
    }

    this.gf.sync();
    return { ctxId };
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
    version: "0.0.12",
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
        description: "Search for nodes in the knowledge graph using a regex pattern. Results are paginated (max 512 chars).",
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
        description: "Open specific nodes in the knowledge graph by their names. Results are paginated (max 512 chars).",
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
        description: "Get names of neighboring entities connected to a specific entity within a given depth. Returns neighbor names with timestamps for sorting. Use open_nodes to get full entity data. Results are paginated (max 512 chars).",
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
        description: "Find a path between two entities in the knowledge graph. Results are paginated (max 512 chars).",
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
        description: "Get all entities of a specific type. Results are paginated (max 512 chars).",
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
        description: "Get entities that have no relations (orphaned entities). In strict mode, returns entities not connected to 'Self' entity. Results are paginated (max 512 chars).",
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
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (!args) {
    throw new Error(`No arguments provided for tool: ${name}`);
  }

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
      const graph = await knowledgeGraphManager.searchNodes(args.query as string, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
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
      const path = await knowledgeGraphManager.findPath(args.fromEntity as string, args.toEntity as string, args.maxDepth as number, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
      return { content: [{ type: "text", text: JSON.stringify(paginateItems(path, args.cursor as number ?? 0)) }] };
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
      const result = await knowledgeGraphManager.randomWalk(args.start as string, args.depth as number ?? 3, args.seed as string | undefined, (args.direction as 'forward' | 'backward' | 'any') ?? 'forward');
      return { content: [{ type: "text", text: JSON.stringify(result) }] };
    }
    case "sequentialthinking": {
      const result = await knowledgeGraphManager.addThought(
        args.observations as string[],
        args.previousCtxId as string | undefined
      );
      return { content: [{ type: "text", text: JSON.stringify(result) }] };
    }
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

  return server;
}
