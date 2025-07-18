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
      const newObservations = o.contents.filter(content => !entity.observations.includes(content));
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

  async readGraph(): Promise<KnowledgeGraph> {
    return this.loadGraph();
  }

  // Very basic search function
  async searchNodes(query: string): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    
    // Filter entities
    const filteredEntities = graph.entities.filter(e => 
      e.name.toLowerCase().includes(query.toLowerCase()) ||
      e.entityType.toLowerCase().includes(query.toLowerCase()) ||
      e.observations.some(o => o.toLowerCase().includes(query.toLowerCase()))
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

  async getNeighbors(entityName: string, depth: number = 1): Promise<KnowledgeGraph> {
    const graph = await this.loadGraph();
    const visited = new Set<string>();
    const resultEntities = new Map<string, Entity>();
    const resultRelations: Relation[] = [];
    
    const traverse = (currentName: string, currentDepth: number) => {
      if (currentDepth > depth || visited.has(currentName)) return;
      visited.add(currentName);
      
      const entity = graph.entities.find(e => e.name === currentName);
      if (entity) {
        resultEntities.set(currentName, entity);
      }
      
      // Find all relations involving this entity
      const connectedRelations = graph.relations.filter(r => 
        r.from === currentName || r.to === currentName
      );
      
      resultRelations.push(...connectedRelations);
      
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
      relations: resultRelations
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

  async validateGraph(): Promise<string[]> {
    const graph = await this.loadGraph();
    const entityNames = new Set(graph.entities.map(e => e.name));
    const missingEntities = new Set<string>();
    
    graph.relations.forEach(r => {
      if (!entityNames.has(r.from)) {
        missingEntities.add(r.from);
      }
      if (!entityNames.has(r.to)) {
        missingEntities.add(r.to);
      }
    });
    
    return Array.from(missingEntities);
  }

  // BCL (Binary Combinatory Logic) evaluator
  async evaluateBCL(program: string, maxSteps: number): Promise<{ result: string; info: string; halted: boolean }> {
    let current = program;
    let stepCount = 0;
    
    // Find and apply one reduction rule to the leftmost applicable position
    const applyOneRule = (term: string): { newTerm: string; applied: boolean; rule: string } => {
      // Scan left-to-right for rule applications
      for (let pos = 0; pos <= term.length - 6; pos++) { // minimum pattern "1100xy" needs at least 6 chars
        
        // Rule 1: 1100xy → x
        if (pos <= term.length - 6 && term.substring(pos, pos + 4) === '1100') {
          let parsePos = pos + 4;
          
          // Parse x
          const xResult = this.parseBCLTerm(term, parsePos);
          if (xResult.success) {
            parsePos = xResult.nextPos;
            
            // Parse y
            const yResult = this.parseBCLTerm(term, parsePos);
            if (yResult.success) {
              // Apply Rule 1: 1100xy → x
              const before = term.substring(0, pos);
              const after = term.substring(yResult.nextPos);
              const newTerm = before + xResult.term + after;
              return {
                newTerm,
                applied: true,
                rule: `Rule 1: 1100${xResult.term}${yResult.term} → ${xResult.term}`
              };
            }
          }
        }
        
        // Rule 2: 11101xyz → 11xz1yz (need minimum 8 chars)
        if (pos <= term.length - 8 && term.substring(pos, pos + 5) === '11101') {
          let parsePos = pos + 5;
          
          // Parse x
          const xResult = this.parseBCLTerm(term, parsePos);
          if (xResult.success) {
            parsePos = xResult.nextPos;
            
            // Parse y
            const yResult = this.parseBCLTerm(term, parsePos);
            if (yResult.success) {
              parsePos = yResult.nextPos;
              
              // Parse z
              const zResult = this.parseBCLTerm(term, parsePos);
              if (zResult.success) {
                // Apply Rule 2: 11101xyz → 11xz1yz
                const x = xResult.term;
                const y = yResult.term;
                const z = zResult.term;
                const before = term.substring(0, pos);
                const after = term.substring(zResult.nextPos);
                const replacement = `11${x}${z}1${y}${z}`;
                const newTerm = before + replacement + after;
                return {
                  newTerm,
                  applied: true,
                  rule: `Rule 2: 11101${x}${y}${z} → 11${x}${z}1${y}${z}`
                };
              }
            }
          }
        }
      }
      
      return { newTerm: term, applied: false, rule: '' };
    };
    
    // Validate input is a well-formed BCL term
    const validation = this.parseBCLTerm(current, 0);
    if (!validation.success || validation.nextPos !== current.length) {
      return {
        result: current,
        info: `Error: Invalid BCL term - parse failed at position ${validation.nextPos}`,
        halted: false
      };
    }
    
    let max_size = current.length;
    while (stepCount < maxSteps) {
      const result = applyOneRule(current);
      
      if (!result.applied) {
        // Normal form reached - no more reductions possible
        break;
      }
      
      current = result.newTerm;
      if (current.length > max_size) {
        max_size = current.length;
      }
      stepCount++;
    }
    
    const halted = stepCount < maxSteps;
    return {
      result: current,
      info: `${stepCount} steps, max size ${max_size}`,
      halted
    };
  }

  // Helper function to parse a single BCL term from a given position
  private parseBCLTerm(input: string, startPos: number): { success: boolean; term: string; nextPos: number } {
    if (startPos >= input.length) {
      return { success: false, term: '', nextPos: startPos };
    }
    
    // Check for 00 (K combinator)
    if (startPos + 1 < input.length && input.substring(startPos, startPos + 2) === '00') {
      return { success: true, term: '00', nextPos: startPos + 2 };
    }
    
    // Check for 01 (S combinator) 
    if (startPos + 1 < input.length && input.substring(startPos, startPos + 2) === '01') {
      return { success: true, term: '01', nextPos: startPos + 2 };
    }
    
    // Check for 1 (application)
    if (input[startPos] === '1') {
      // Parse first subterm
      const leftResult = this.parseBCLTerm(input, startPos + 1);
      if (!leftResult.success) {
        return leftResult;
      }
      
      // Parse second subterm
      const rightResult = this.parseBCLTerm(input, leftResult.nextPos);
      if (!rightResult.success) {
        return rightResult;
      }
      
      const term = '1' + leftResult.term + rightResult.term;
      return { success: true, term, nextPos: rightResult.nextPos };
    }
    
    return { success: false, term: '', nextPos: startPos };
  }
}

const knowledgeGraphManager = new KnowledgeGraphManager();


// The server instance and tools exposed to Claude
const server = new Server({
  name: "memory-server",
  version: "0.0.0",
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
                    items: { type: "string" },
                    description: "An array of observation contents associated with the entity"
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
        description: "Add new observations to existing entities in the knowledge graph",
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
                    items: { type: "string" },
                    description: "An array of observation contents to add"
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
        name: "read_graph",
        description: "Read the entire knowledge graph",
        inputSchema: {
          type: "object",
          properties: {},
        },
      },
      {
        name: "search_nodes",
        description: "Search for nodes in the knowledge graph based on a query",
        inputSchema: {
          type: "object",
          properties: {
            query: { type: "string", description: "The search query to match against entity names, types, and observation content" },
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
            depth: { type: "number", description: "Maximum depth to traverse (default: 1)", default: 1 },
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
        description: "Validate the knowledge graph and return a list of missing entities referenced in relations",
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
    case "read_graph":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.readGraph(), null, 2) }] };
    case "search_nodes":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.searchNodes(args.query as string), null, 2) }] };
    case "open_nodes_filtered":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.openNodesFiltered(args.names as string[]), null, 2) }] };
    case "open_nodes":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.openNodes(args.names as string[]), null, 2) }] };
    case "get_neighbors":
      return { content: [{ type: "text", text: JSON.stringify(await knowledgeGraphManager.getNeighbors(args.entityName as string, args.depth as number), null, 2) }] };
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
