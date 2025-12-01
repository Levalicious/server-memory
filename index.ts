#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Define memory file path using environment variable with fallback
const defaultMemoryPath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'memory.json');

// If MEMORY_FILE_PATH is just a filename, put it in the same directory as the script
const MEMORY_FILE_PATH = process.env.MEMORY_FILE_PATH
  ? path.isAbsolute(process.env.MEMORY_FILE_PATH)
    ? process.env.MEMORY_FILE_PATH
    : path.join(path.dirname(fileURLToPath(import.meta.url)), process.env.MEMORY_FILE_PATH)
  : defaultMemoryPath;

// We are storing our memory using entities, relations, and observations in a graph structure
interface Entity {
  name: string;
  entityType: string;
  observations: string[];
}

interface Relation {
  from: string;
  to: string;
  relationType: string;
}

interface KnowledgeGraph {
  entities: Entity[];
  relations: Relation[];
}

// The KnowledgeGraphManager class contains all operations to interact with the knowledge graph
class KnowledgeGraphManager {
  bclCtr: number = 0;
  bclTerm: string = "";

  private async loadGraph(): Promise<KnowledgeGraph> {
    try {
      const data = await fs.readFile(MEMORY_FILE_PATH, "utf-8");
      const lines = data.split("\n").filter(line => line.trim() !== "");
      return lines.reduce((graph: KnowledgeGraph, line) => {
        const item = JSON.parse(line);
        if (item.type === "entity") graph.entities.push(item as Entity);
        if (item.type === "relation") graph.relations.push(item as Relation);
        return graph;
      }, { entities: [], relations: [] });
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error as any).code === "ENOENT") {
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
    await fs.writeFile(MEMORY_FILE_PATH, lines.join("\n"));
  }

  async createEntities(entities: Entity[]): Promise<Entity[]> {
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
    
    const newEntities = entities.filter(e => !graph.entities.some(existingEntity => existingEntity.name === e.name));
    graph.entities.push(...newEntities);
    await this.saveGraph(graph);
    return newEntities;
  }

  async createRelations(relations: Relation[]): Promise<Relation[]> {
    const graph = await this.loadGraph();
    const newRelations = relations.filter(r => !graph.relations.some(existingRelation => 
      existingRelation.from === r.from && 
      existingRelation.to === r.to && 
      existingRelation.relationType === r.relationType
    ));
    graph.relations.push(...newRelations);
    await this.saveGraph(graph);
    return newRelations;
  }

  async addObservations(observations: { entityName: string; contents: string[] }[]): Promise<{ entityName: string; addedObservations: string[] }[]> {
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
      return { entityName: o.entityName, addedObservations: newObservations };
    });
    await this.saveGraph(graph);
    return results;
  }

  async deleteEntities(entityNames: string[]): Promise<void> {
    const graph = await this.loadGraph();
    graph.entities = graph.entities.filter(e => !entityNames.includes(e.name));
    graph.relations = graph.relations.filter(r => !entityNames.includes(r.from) && !entityNames.includes(r.to));
    await this.saveGraph(graph);
  }

  async deleteObservations(deletions: { entityName: string; observations: string[] }[]): Promise<void> {
    const graph = await this.loadGraph();
    deletions.forEach(d => {
      const entity = graph.entities.find(e => e.name === d.entityName);
      if (entity) {
        entity.observations = entity.observations.filter(o => !d.observations.includes(o));
      }
    });
    await this.saveGraph(graph);
  }

  async deleteRelations(relations: Relation[]): Promise<void> {
    const graph = await this.loadGraph();
    graph.relations = graph.relations.filter(r => !relations.some(delRelation => 
      r.from === delRelation.from && 
      r.to === delRelation.to && 
      r.relationType === delRelation.relationType
    ));
    await this.saveGraph(graph);
  }

  // Regex-based search function
  async searchNodes(query: string): Promise<KnowledgeGraph> {
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
      entities: filteredEntities,
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

  async getNeighbors(entityName: string, depth: number = 1, withEntities: boolean = false): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    const visited = new Set<string>();
    const resultEntities = new Map<string, Entity>();
    const resultRelations = new Map<string, Relation>(); // Deduplicate relations
    
    const relationKey = (r: Relation) => `${r.from}|${r.relationType}|${r.to}`;
    
    const traverse = (currentName: string, currentDepth: number) => {
      if (currentDepth > depth || visited.has(currentName)) return;
      visited.add(currentName);
      
      if (withEntities) {
        const entity = graph.entities.find(e => e.name === currentName);
        if (entity) {
          resultEntities.set(currentName, entity);
        }
      }
      
      // Find all relations involving this entity
      const connectedRelations = graph.relations.filter(r => 
        r.from === currentName || r.to === currentName
      );
      
      connectedRelations.forEach(r => resultRelations.set(relationKey(r), r));
      
      if (currentDepth < depth) {
        // Traverse to connected entities
        connectedRelations.forEach(r => {
          const nextEntity = r.from === currentName ? r.to : r.from;
          traverse(nextEntity, currentDepth + 1);
        });
      }
    };
    
    traverse(entityName, 0);
    
    return {
      entities: Array.from(resultEntities.values()),
      relations: Array.from(resultRelations.values())
    };
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

  async getEntitiesByType(entityType: string): Promise<Entity[]> {
    const graph = await this.loadGraph();
    return graph.entities.filter(e => e.entityType === entityType);
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

  async getOrphanedEntities(): Promise<Entity[]> {
    const graph = await this.loadGraph();
    const connectedEntityNames = new Set<string>();
    
    graph.relations.forEach(r => {
      connectedEntityNames.add(r.from);
      connectedEntityNames.add(r.to);
    });
    
    return graph.entities.filter(e => !connectedEntityNames.has(e.name));
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
}

const knowledgeGraphManager = new KnowledgeGraphManager();


// The server instance and tools exposed to Claude
const server = new Server({
  name: "memory-server",
  icons: [
    { src: "data:image/svg+xml;base64,PHN2ZyBmaWxsPSJjdXJyZW50Q29sb3IiIGZpbGwtcnVsZT0iZXZlbm9kZCIgaGVpZ2h0PSIxZW0iIHN0eWxlPSJmbGV4Om5vbmU7bGluZS1oZWlnaHQ6MSIgdmlld0JveD0iMCAwIDI0IDI0IiB3aWR0aD0iMWVtIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjx0aXRsZT5Nb2RlbENvbnRleHRQcm90b2NvbDwvdGl0bGU+PHBhdGggZD0iTTIwLjEgNS41MlYxLjVoLS4xOGMtMy4zNi4xNS02LjE1IDIuMzEtNy44MyA0LjAybC0uMDkuMDktLjA5LS4wOUMxMC4yIDMuODEgNy40NCAxLjY1IDQuMDggMS41SDMuOXY0LjAySDB2Ni45M2MwIDEuNjguMDYgMy4zNi4xOCA0Ljc0YTUuNTcgNS41NyAwIDAgMCA1LjE5IDUuMWMyLjEzLjEyIDQuMzguMjEgNi42My4yMXM0LjUtLjA5IDYuNjMtLjI0YTUuNTcgNS41NyAwIDAgMCA1LjE5LTUuMWMuMTItMS4zOC4xOC0zLjA2LjE4LTQuNzR2LTYuOXptMCA2LjkzYzAgMS41OS0uMDYgMy4xNS0uMTggNC40MS0uMDkuODEtLjc1IDEuNDctMS41NiAxLjVhOTAgOTAgMCAwIDEtMTIuNzIgMGMtLjgxLS4wMy0xLjUtLjY5LTEuNTYtMS41LS4xMi0xLjI2LS4xOC0yLjg1LS4xOC00LjQxVjUuNTJjMi44Mi4xMiA1LjY0IDMuMTUgNi40OCA0LjMyTDEyIDEyLjA5bDEuNjItMi4yNWMuODQtMS4yIDMuNjYtNC4yIDYuNDgtNC4zMnoiLz48L3N2Zz4=",
      mimeType: "image/svg+xml",
      sizes: ["any"]
    }
  ],
  version: "0.0.4",
},    {
    capabilities: {
      tools: {},
    },
  },);

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
        description: "Search for nodes in the knowledge graph using a regex pattern",
        inputSchema: {
          type: "object",
          properties: {
            query: { type: "string", description: "Regex pattern to match against entity names, types, and observations. Use | for alternatives (e.g., 'Taranis|wheel'). Special regex characters must be escaped for literal matching." },
          },
          required: ["query"],
        },
      },
      {
        name: "open_nodes_filtered",
        description: "Open specific nodes in the knowledge graph by their names, filtering relations to only those between the opened nodes",
        inputSchema: {
          type: "object",
          properties: {
            names: {
              type: "array",
              items: { type: "string" },
              description: "An array of entity names to retrieve",
            },
          },
          required: ["names"],
        },
      },
      {
        name: "open_nodes",
        description: "Open specific nodes in the knowledge graph by their names",
        inputSchema: {
          type: "object",
          properties: {
            names: {
              type: "array",
              items: { type: "string" },
              description: "An array of entity names to retrieve",
            },
          },
          required: ["names"],
        },
      },
      {
        name: "get_neighbors",
        description: "Get neighboring entities connected to a specific entity within a given depth",
        inputSchema: {
          type: "object",
          properties: {
            entityName: { type: "string", description: "The name of the entity to find neighbors for" },
            depth: { type: "number", description: "Maximum depth to traverse (default: 0)", default: 0 },
            withEntities: { type: "boolean", description: "If true, include full entity data. Default returns only relations for lightweight structure exploration.", default: false },
          },
          required: ["entityName"],
        },
      },
      {
        name: "find_path",
        description: "Find a path between two entities in the knowledge graph",
        inputSchema: {
          type: "object",
          properties: {
            fromEntity: { type: "string", description: "The name of the starting entity" },
            toEntity: { type: "string", description: "The name of the target entity" },
            maxDepth: { type: "number", description: "Maximum depth to search (default: 5)", default: 5 },
          },
          required: ["fromEntity", "toEntity"],
        },
      },
      {
        name: "get_entities_by_type",
        description: "Get all entities of a specific type",
        inputSchema: {
          type: "object",
          properties: {
            entityType: { type: "string", description: "The type of entities to retrieve" },
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
        description: "Get entities that have no relations (orphaned entities)",
        inputSchema: {
          type: "object",
          properties: {},
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
    case "search_nodes":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.searchNodes(args.query as string), null, 2) }] };
    case "open_nodes_filtered":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.openNodesFiltered(args.names as string[]), null, 2) }] };
    case "open_nodes":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.openNodes(args.names as string[]), null, 2) }] };
    case "get_neighbors":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getNeighbors(args.entityName as string, args.depth as number, args.withEntities as boolean), null, 2) }] };
    case "find_path":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.findPath(args.fromEntity as string, args.toEntity as string, args.maxDepth as number), null, 2) }] };
    case "get_entities_by_type":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getEntitiesByType(args.entityType as string), null, 2) }] };
    case "get_entity_types":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getEntityTypes(), null, 2) }] };
    case "get_relation_types":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getRelationTypes(), null, 2) }] };
    case "get_stats":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getStats(), null, 2) }] };
    case "get_orphaned_entities":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getOrphanedEntities(), null, 2) }] };
    case "validate_graph":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.validateGraph(), null, 2) }] };
    case "evaluate_bcl":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.evaluateBCL(args.program as string, args.maxSteps as number), null, 2) }] };
    case "add_bcl_term":
      return { content: [{ type: "text", text: await knowledgeGraphManager.addBCLTerm(args.term as string) }] };
    case "clear_bcl_term":
      await knowledgeGraphManager.clearBCLTerm();
      return { content: [{ type: "text", text: "BCL term constructor cleared successfully" }] };
    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Knowledge Graph MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});
