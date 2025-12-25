#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from 'fs';
import { randomBytes } from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import lockfile from 'proper-lockfile';

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

export type EntitySortField = "mtime" | "obsMtime" | "name";
export type SortDirection = "asc" | "desc";

export interface Neighbor {
  name: string;
  mtime?: number;
  obsMtime?: number;
}

/**
 * Sort entities by the specified field and direction.
 * Returns a new array (does not mutate input).
 * If sortBy is undefined, returns the original array (no sorting - preserves insertion order).
 */
function sortEntities(
  entities: Entity[],
  sortBy?: EntitySortField,
  sortDir?: SortDirection
): Entity[] {
  if (!sortBy) return entities; // No sorting - preserve current behavior

  const dir = sortDir ?? (sortBy === "name" ? "asc" : "desc");
  const mult = dir === "asc" ? 1 : -1;

  return [...entities].sort((a, b) => {
    if (sortBy === "name") {
      return mult * a.name.localeCompare(b.name);
    }
    // For timestamps, treat undefined as 0 (oldest)
    const aVal = a[sortBy] ?? 0;
    const bVal = b[sortBy] ?? 0;
    return mult * (aVal - bVal);
  });
}

/**
 * Sort neighbors by the specified field and direction.
 * If sortBy is undefined, returns the original array (no sorting).
 */
function sortNeighbors(
  neighbors: Neighbor[],
  sortBy?: EntitySortField,
  sortDir?: SortDirection
): Neighbor[] {
  if (!sortBy) return neighbors;

  const dir = sortDir ?? (sortBy === "name" ? "asc" : "desc");
  const mult = dir === "asc" ? 1 : -1;

  return [...neighbors].sort((a, b) => {
    if (sortBy === "name") {
      return mult * a.name.localeCompare(b.name);
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
  bclCtr: number = 0;
  bclTerm: string = "";
  private memoryFilePath: string;

  constructor(memoryFilePath: string = DEFAULT_MEMORY_FILE_PATH) {
    this.memoryFilePath = memoryFilePath;
  }

  private async withLock<T>(fn: () => Promise<T>): Promise<T> {
    // Ensure file exists for locking
    try {
      await fs.access(this.memoryFilePath);
    } catch {
      await fs.writeFile(this.memoryFilePath, "");
    }
    const release = await lockfile.lock(this.memoryFilePath, { retries: { retries: 5, minTimeout: 100 } });
    try {
      return await fn();
    } finally {
      await release();
    }
  }

  private async loadGraph(): Promise<KnowledgeGraph> {
    try {
      const data = await fs.readFile(this.memoryFilePath, "utf-8");
      const lines = data.split("\n").filter(line => line.trim() !== "");
      return lines.reduce((graph: KnowledgeGraph, line) => {
        const item = JSON.parse(line);
        if (item.type === "entity") graph.entities.push(item as Entity);
        if (item.type === "relation") graph.relations.push(item as Relation);
        return graph;
      }, { entities: [], relations: [] });
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code === "ENOENT") {
        return { entities: [], relations: [] };
      }
      throw error;
    }
  }

  private async saveGraph(graph: KnowledgeGraph): Promise<void> {
    const lines = [
      ...graph.entities.map(e => JSON.stringify({ type: "entity", ...e })),
      ...graph.relations.map(r => JSON.stringify({ type: "relation", ...r })),
    ];
    const content = lines.join("\n") + (lines.length > 0 ? "\n" : "");
    await fs.writeFile(this.memoryFilePath, content);
  }

  async createEntities(entities: Entity[]): Promise<Entity[]> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      
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
      
      const now = Date.now();
      const newEntities = entities
        .filter(e => !graph.entities.some(existingEntity => existingEntity.name === e.name))
        .map(e => ({ ...e, mtime: now, obsMtime: e.observations.length > 0 ? now : undefined }));
      graph.entities.push(...newEntities);
      await this.saveGraph(graph);
      return newEntities;
    });
  }

  async createRelations(relations: Relation[]): Promise<Relation[]> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      const now = Date.now();
      
      // Update mtime on 'from' entities when relations are added
      const fromEntityNames = new Set(relations.map(r => r.from));
      graph.entities.forEach(e => {
        if (fromEntityNames.has(e.name)) {
          e.mtime = now;
        }
      });
      
      const newRelations = relations
        .filter(r => !graph.relations.some(existingRelation => 
          existingRelation.from === r.from && 
          existingRelation.to === r.to && 
          existingRelation.relationType === r.relationType
        ))
        .map(r => ({ ...r, mtime: now }));
      graph.relations.push(...newRelations);
      await this.saveGraph(graph);
      return newRelations;
    });
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      const results = observations.map(o => {
        const entity = graph.entities.find(e => e.name === o.entityName);
        if (!entity) {
          throw new Error(`Entity with name ${o.entityName} not found`);
        }
        
        // Validate observation character limits
        for (const obs of o.contents) {
          if (obs.length > 140) {
            throw new Error(`Observation for "${o.entityName}" exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
          }
        }
        
        const newObservations = o.contents.filter(content => !entity.observations.includes(content));
        
        // Validate total observation count
        if (entity.observations.length + newObservations.length > 2) {
          throw new Error(`Adding ${newObservations.length} observations to "${o.entityName}" would exceed limit of 2 (currently has ${entity.observations.length}).`);
        }
        
        entity.observations.push(...newObservations);
        if (newObservations.length > 0) {
          const now = Date.now();
          entity.mtime = now;
          entity.obsMtime = now;
        }
        return { entityName: o.entityName, addedObservations: newObservations };
      });
      await this.saveGraph(graph);
      return results;
    });
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      graph.entities = graph.entities.filter(e => !entityNames.includes(e.name));
      graph.relations = graph.relations.filter(r => !entityNames.includes(r.from) && !entityNames.includes(r.to));
      await this.saveGraph(graph);
    });
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      const now = Date.now();
      deletions.forEach(d => {
        const entity = graph.entities.find(e => e.name === d.entityName);
        if (entity) {
          const originalLen = entity.observations.length;
          entity.observations = entity.observations.filter(o => !d.observations.includes(o));
          if (entity.observations.length !== originalLen) {
            entity.mtime = now;
            entity.obsMtime = now;
          }
        }
      });
      await this.saveGraph(graph);
    });
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      graph.relations = graph.relations.filter(r => !relations.some(delRelation => 
        r.from === delRelation.from && 
        r.to === delRelation.to && 
        r.relationType === delRelation.relationType
      ));
      await this.saveGraph(graph);
    });
  }

  // Regex-based search function
  async searchNodes(query: string, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    let regex: RegExp;
    try {
      regex = new RegExp(query, 'i'); // case-insensitive
    } catch (e) {
      throw new Error(`Invalid regex pattern: ${query}`);
    }
    
    // Filter entities
    const filteredEntities = graph.entities.filter(e => 
      regex.test(e.name) ||
      regex.test(e.entityType) ||
      e.observations.some(o => regex.test(o))
    );
  
    // Create a Set of filtered entity names for quick lookup
    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));
  
    // Filter relations to only include those between filtered entities
    const filteredRelations = graph.relations.filter(r => 
      filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to)
    );
  
    const filteredGraph: KnowledgeGraph = {
      entities: sortEntities(filteredEntities, sortBy, sortDir),
      relations: filteredRelations,
    };
  
    return filteredGraph;
  }

  async openNodesFiltered(names: string[]): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    // Filter entities
    const filteredEntities = graph.entities.filter(e => names.includes(e.name));
  
    // Create a Set of filtered entity names for quick lookup
    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));
  
    // Filter relations to only include those between filtered entities
    const filteredRelations = graph.relations.filter(r => 
      filteredEntityNames.has(r.from) && filteredEntityNames.has(r.to)
    );
  
    const filteredGraph: KnowledgeGraph = {
      entities: filteredEntities,
      relations: filteredRelations,
    };
  
    return filteredGraph;
  }

  async openNodes(names: string[]): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    // Filter entities
    const filteredEntities = graph.entities.filter(e => names.includes(e.name));
  
    // Create a Set of filtered entity names for quick lookup
    const filteredEntityNames = new Set(filteredEntities.map(e => e.name));
  
    // Filter relations to only include those between filtered entities
    const filteredRelations = graph.relations.filter(r => filteredEntityNames.has(r.from));
  
    const filteredGraph: KnowledgeGraph = {
      entities: filteredEntities,
      relations: filteredRelations,
    };
  
    return filteredGraph;
  }

  async getNeighbors(
    entityName: string, 
    depth: number = 1, 
    sortBy?: EntitySortField, 
    sortDir?: SortDirection
  ): Promise<Neighbor[]> {
    const graph = await this.loadGraph();
    const visited = new Set<string>();
    const neighborNames = new Set<string>();
    
    const traverse = (currentName: string, currentDepth: number) => {
      if (currentDepth > depth || visited.has(currentName)) return;
      visited.add(currentName);
      
      // Find all relations involving this entity
      const connectedRelations = graph.relations.filter(r => 
        r.from === currentName || r.to === currentName
      );
      
      // Collect neighbor names
      connectedRelations.forEach(r => {
        const neighborName = r.from === currentName ? r.to : r.from;
        neighborNames.add(neighborName);
      });
      
      if (currentDepth < depth) {
        // Traverse to connected entities
        connectedRelations.forEach(r => {
          const nextEntity = r.from === currentName ? r.to : r.from;
          traverse(nextEntity, currentDepth + 1);
        });
      }
    };
    
    traverse(entityName, 0);
    
    // Remove the starting entity from neighbors (it's not its own neighbor)
    neighborNames.delete(entityName);
    
    // Build neighbor objects with timestamps
    const entityMap = new Map(graph.entities.map(e => [e.name, e]));
    const neighbors: Neighbor[] = Array.from(neighborNames).map(name => {
      const entity = entityMap.get(name);
      return {
        name,
        mtime: entity?.mtime,
        obsMtime: entity?.obsMtime,
      };
    });
    
    return sortNeighbors(neighbors, sortBy, sortDir);
  }

  async findPath(fromEntity: string, toEntity: string, maxDepth: number = 5): Promise<Relation[]> {
    const graph = await this.loadGraph();
    const visited = new Set<string>();
    
    const dfs = (current: string, target: string, path: Relation[], depth: number): Relation[] | null => {
      if (depth > maxDepth || visited.has(current)) return null;
      if (current === target) return path;
      
      visited.add(current);
      
      const outgoingRelations = graph.relations.filter(r => r.from === current);
      for (const relation of outgoingRelations) {
        const result = dfs(relation.to, target, [...path, relation], depth + 1);
        if (result) return result;
      }
      
      visited.delete(current);
      return null;
    };
    
    return dfs(fromEntity, toEntity, [], 0) || [];
  }

  async getEntitiesByType(entityType: string, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    const graph = await this.loadGraph();
    const filtered = graph.entities.filter(e => e.entityType === entityType);
    return sortEntities(filtered, sortBy, sortDir);
  }

  async getEntityTypes(): Promise<string[]> {
    const graph = await this.loadGraph();
    const types = new Set(graph.entities.map(e => e.entityType));
    return Array.from(types).sort();
  }

  async getRelationTypes(): Promise<string[]> {
    const graph = await this.loadGraph();
    const types = new Set(graph.relations.map(r => r.relationType));
    return Array.from(types).sort();
  }

  async getStats(): Promise<{ entityCount: number; relationCount: number; entityTypes: number; relationTypes: number }> {
    const graph = await this.loadGraph();
    const entityTypes = new Set(graph.entities.map(e => e.entityType));
    const relationTypes = new Set(graph.relations.map(r => r.relationType));
    
    return {
      entityCount: graph.entities.length,
      relationCount: graph.relations.length,
      entityTypes: entityTypes.size,
      relationTypes: relationTypes.size
    };
  }

  async getOrphanedEntities(strict: boolean = false, sortBy?: EntitySortField, sortDir?: SortDirection): Promise<Entity[]> {
    const graph = await this.loadGraph();
    
    if (!strict) {
      // Simple mode: entities with no relations at all
      const connectedEntityNames = new Set<string>();
      graph.relations.forEach(r => {
        connectedEntityNames.add(r.from);
        connectedEntityNames.add(r.to);
      });
      const orphans = graph.entities.filter(e => !connectedEntityNames.has(e.name));
      return sortEntities(orphans, sortBy, sortDir);
    }
    
    // Strict mode: entities not connected to "Self" (directly or indirectly)
    // Build adjacency list (bidirectional)
    const neighbors = new Map<string, Set<string>>();
    graph.entities.forEach(e => neighbors.set(e.name, new Set()));
    graph.relations.forEach(r => {
      neighbors.get(r.from)?.add(r.to);
      neighbors.get(r.to)?.add(r.from);
    });
    
    // BFS from Self to find all connected entities
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
    
    // Return entities not connected to Self (excluding Self itself if it exists)
    const orphans = graph.entities.filter(e => !connectedToSelf.has(e.name));
    return sortEntities(orphans, sortBy, sortDir);
  }

  async validateGraph(): Promise<{ missingEntities: string[]; observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] }> {
    const graph = await this.loadGraph();
    const entityNames = new Set(graph.entities.map(e => e.name));
    const missingEntities = new Set<string>();
    const observationViolations: { entity: string; count: number; oversizedObservations: number[] }[] = [];
    
    // Check for missing entities in relations
    graph.relations.forEach(r => {
      if (!entityNames.has(r.from)) {
        missingEntities.add(r.from);
      }
      if (!entityNames.has(r.to)) {
        missingEntities.add(r.to);
      }
    });
    
    // Check for observation limit violations
    graph.entities.forEach(e => {
      const oversizedObservations: number[] = [];
      e.observations.forEach((obs, idx) => {
        if (obs.length > 140) {
          oversizedObservations.push(idx);
        }
      });
      
      if (e.observations.length > 2 || oversizedObservations.length > 0) {
        observationViolations.push({
          entity: e.name,
          count: e.observations.length,
          oversizedObservations
        });
      }
    });
    
    return {
      missingEntities: Array.from(missingEntities),
      observationViolations
    };
  }

  // BCL (Binary Combinatory Logic) evaluator
  async evaluateBCL(program: string, maxSteps: number): Promise<{ result: string; info: string; halted: boolean, errored: boolean }> {
    let stepCount = 0;
    let max_size = program.length;

    let mode: number = 0;
    let ctr: number = 1;
    let t0: string = program;
    let t1: string = '';
    let t2: string = '';
    let t3: string = '';
    let t4: string = '';

    while (stepCount < maxSteps) {
      if (t0.length == 0) break;
      let b = t0[0]; t0 = t0.slice(1);
      if (mode === 0) {
        t1 += b;
        let size = t1.length + t0.length;
        if (size > max_size) max_size = size;
        if (t1.slice(-4) === '1100') {
          mode = 1;
          t1 = t1.slice(0, -4);
        } else if (t1.slice(-5) === '11101') {
          mode = 3;
          t1 = t1.slice(0, -5);
        }
      } else if (mode === 1) {
        t2 += b;
        if (b == '1') {
          ctr += 1;
        } else if (b == '0') {
          ctr -= 1;
          t2 += t0[0]; t0 = t0.slice(1);
        }
        if (ctr === 0) {
          mode = 2;
          ctr = 1;
        }
      } else if (mode === 2) {
        if (b == '1') {
          ctr += 1;
        } else if (b == '0') {
          ctr -= 1;
          t0 = t0.slice(1);
        }
        if (ctr === 0) {
          t0 = t2 + t0;
          t2 = '';
          mode = 0;
          ctr = 1;
          stepCount += 1;
        }
      } else if (mode === 3) {
        t2 += b;
        if (b == '1') {
          ctr += 1;
        } else if (b == '0') {
          ctr -= 1;
          t2 += t0[0]; t0 = t0.slice(1);
        }
        if (ctr === 0) {
          mode = 4;
          ctr = 1;
        }
      } else if (mode === 4) {
        t3 += b;
        if (b == '1') {
          ctr += 1;
        } else if (b == '0') {
          ctr -= 1;
          t3 += t0[0]; t0 = t0.slice(1);
        }
        if (ctr === 0) {
          mode = 5;
          ctr = 1;
        }
      } else if (mode === 5) {
        t4 += b;
        if (b == '1') {
          ctr += 1;
        } else if (b == '0') {
          ctr -= 1;
          t4 += t0[0]; t0 = t0.slice(1);
        }
        if (ctr === 0) {
          t0 = '11' + t2 + t4 + '1' + t3 + t4 + t0;
          t2 = '';
          t3 = '';
          t4 = '';
          mode = 0;
          ctr = 1;
          stepCount += 1;
        }
      }
    }
    
    const halted = stepCount < maxSteps;
    return {
      result: t1,
      info: `${stepCount} steps, max size ${max_size}`,
      halted,
      errored: halted && mode != 0,
    };
  }

  async addBCLTerm(term: string): Promise<string> {
    const termset = ["1", "00", "01"];
    if (!term || term.trim() === "") {
      throw new Error("BCL term cannot be empty");
    }
    // Term can be 1, 00, 01, or K, S, App (application)
    const validTerms = ["1", "App", "00", "K", "01", "S"];
    if (!validTerms.includes(term)) {
      throw new Error(`Invalid BCL term: ${term}\nExpected one of: ${validTerms.join(", ")}`);
    }
    let processedTerm = 0;
    if (term === "00" || term === "K") processedTerm = 1;
    else if (term === "01" || term === "S") processedTerm = 2;
    this.bclTerm += termset[processedTerm];
    if (processedTerm === 0) {
      if (this.bclCtr === 0) this.bclCtr += 1;
      this.bclCtr += 1;
    } else {
      this.bclCtr -= 1;
    }
    if (this.bclCtr <= 0) {
      const constructedProgram = this.bclTerm;
      this.bclCtr = 0;
      this.bclTerm = "";
      return `Constructed Program: ${constructedProgram}`;
    } else {
      return `Need ${this.bclCtr} more term(s) to complete the program.`;
    }
  }

  async clearBCLTerm(): Promise<void> {
    this.bclCtr = 0;
    this.bclTerm = "";
  }

  async addThought(observations: string[], previousCtxId?: string): Promise<{ ctxId: string }> {
    return this.withLock(async () => {
      const graph = await this.loadGraph();
      
      // Validate observations
      if (observations.length > 2) {
        throw new Error(`Thought has ${observations.length} observations. Maximum allowed is 2.`);
      }
      for (const obs of observations) {
        if (obs.length > 140) {
          throw new Error(`Observation exceeds 140 characters (${obs.length} chars): "${obs.substring(0, 50)}..."`);
        }
      }
      
      // Generate new context ID (24-char hex)
      const now = Date.now();
      const ctxId = randomBytes(12).toString('hex');
      
      // Create thought entity
      const thoughtEntity: Entity = {
        name: ctxId,
        entityType: "Thought",
        observations,
        mtime: now,
        obsMtime: observations.length > 0 ? now : undefined,
      };
      graph.entities.push(thoughtEntity);
      
      // Link to previous thought if it exists
      if (previousCtxId) {
        const prevEntity = graph.entities.find(e => e.name === previousCtxId);
        if (prevEntity) {
          // Update mtime on previous entity since we're adding a relation from it
          prevEntity.mtime = now;
          // Bidirectional chain: previous -> new (follows) and new -> previous (preceded_by)
          graph.relations.push(
            { from: previousCtxId, to: ctxId, relationType: "follows", mtime: now },
            { from: ctxId, to: previousCtxId, relationType: "preceded_by", mtime: now }
          );
        }
      }
      
      await this.saveGraph(graph);
      return { ctxId };
    });
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
    version: "0.0.4",
  }, {
    capabilities: {
      tools: {},
    },
  });

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
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name"], description: "Sort field for entities. Omit for insertion order." },
            sortDir: { type: "string", enum: ["asc", "desc"], description: "Sort direction. Default: desc for timestamps, asc for name." },
            entityCursor: { type: "number", description: "Cursor for entity pagination (from previous response's nextCursor)" },
            relationCursor: { type: "number", description: "Cursor for relation pagination" },
          },
          required: ["query"],
        },
      },
      {
        name: "open_nodes_filtered",
        description: "Open specific nodes in the knowledge graph by their names, filtering relations to only those between the opened nodes. Results are paginated (max 512 chars).",
        inputSchema: {
          type: "object",
          properties: {
            names: {
              type: "array",
              items: { type: "string" },
              description: "An array of entity names to retrieve",
            },
            entityCursor: { type: "number", description: "Cursor for entity pagination" },
            relationCursor: { type: "number", description: "Cursor for relation pagination" },
          },
          required: ["names"],
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
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name"], description: "Sort field for neighbors. Omit for arbitrary order." },
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
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name"], description: "Sort field for entities. Omit for insertion order." },
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
            sortBy: { type: "string", enum: ["mtime", "obsMtime", "name"], description: "Sort field for entities. Omit for insertion order." },
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
        name: "evaluate_bcl",
        description: "Evaluate a Binary Combinatory Logic (BCL) program",
        inputSchema: {
          type: "object",
          properties: {
            program: { type: "string", description: "The BCL program as a binary string (syntax: T:=00|01|1TT) 00=K, 01=S, 1=application." },
            maxSteps: { type: "number", description: "Maximum number of reduction steps to perform (default: 1000000)", default: 1000000 },
          },
          required: ["program"],
        },
      },
      {
        name: "add_bcl_term",
        description: "Add a BCL term to the constructor, maintaining valid syntax. Returns completion status.",
        inputSchema: {
          type: "object",
          properties: {
            term: { 
              type: "string", 
              description: "BCL term to add. Valid values: '1' or 'App' (application), '00' or 'K' (K combinator), '01' or 'S' (S combinator)" 
            },
          },
          required: ["term"],
        },
      },
      {
        name: "clear_bcl_term",
        description: "Clear the current BCL term being constructed and reset the constructor state",
        inputSchema: {
          type: "object",
          properties: {},
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
    case "create_entities":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.createEntities(args.entities as Entity[]), null, 2) }] };
    case "create_relations":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.createRelations(args.relations as Relation[]), null, 2) }] };
    case "add_observations":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.addObservations(args.observations as { entityName: string; contents: string[] }[]), null, 2) }] };
    case "delete_entities":
      await knowledgeGraphManager.deleteEntities(args.entityNames as string[]);
      return { content: [{ type: "text", text: "Entities deleted successfully" }] };
    case "delete_observations":
      await knowledgeGraphManager.deleteObservations(args.deletions as { entityName: string; observations: string[] }[]);
      return { content: [{ type: "text", text: "Observations deleted successfully" }] };
    case "delete_relations":
      await knowledgeGraphManager.deleteRelations(args.relations as Relation[]);
      return { content: [{ type: "text", text: "Relations deleted successfully" }] };
    case "search_nodes": {
      const graph = await knowledgeGraphManager.searchNodes(args.query as string, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
      return { content: [{ type: "text", text: JSON.stringify(paginateGraph(graph, args.entityCursor as number ?? 0, args.relationCursor as number ?? 0)) }] };
    }
    case "open_nodes_filtered": {
      const graph = await knowledgeGraphManager.openNodesFiltered(args.names as string[]);
      return { content: [{ type: "text", text: JSON.stringify(paginateGraph(graph, args.entityCursor as number ?? 0, args.relationCursor as number ?? 0)) }] };
    }
    case "open_nodes": {
      const graph = await knowledgeGraphManager.openNodes(args.names as string[]);
      return { content: [{ type: "text", text: JSON.stringify(paginateGraph(graph, args.entityCursor as number ?? 0, args.relationCursor as number ?? 0)) }] };
    }
    case "get_neighbors": {
      const neighbors = await knowledgeGraphManager.getNeighbors(args.entityName as string, args.depth as number ?? 1, args.sortBy as EntitySortField | undefined, args.sortDir as SortDirection | undefined);
      return { content: [{ type: "text", text: JSON.stringify(paginateItems(neighbors, args.cursor as number ?? 0)) }] };
    }
    case "find_path": {
      const path = await knowledgeGraphManager.findPath(args.fromEntity as string, args.toEntity as string, args.maxDepth as number);
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
    case "evaluate_bcl":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.evaluateBCL(args.program as string, args.maxSteps as number), null, 2) }] };
    case "add_bcl_term":
      return { content: [{ type: "text", text: await knowledgeGraphManager.addBCLTerm(args.term as string) }] };
    case "clear_bcl_term":
      await knowledgeGraphManager.clearBCLTerm();
      return { content: [{ type: "text", text: "BCL term constructor cleared successfully" }] };
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
