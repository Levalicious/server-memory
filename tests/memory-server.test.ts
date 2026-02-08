import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, Entity, Relation, KnowledgeGraph, Neighbor } from '../server.js';
import { createTestClient, callTool, PaginatedGraph, PaginatedResult } from './test-utils.js';

describe('MCP Memory Server E2E Tests', () => {
  let testDir: string;
  let memoryFile: string;
  let client: Awaited<ReturnType<typeof createTestClient>>['client'];
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    // Create a unique temp directory for each test
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mcp-memory-test-'));
    memoryFile = path.join(testDir, 'test-memory.json');
    
    const server = createServer(memoryFile);
    const result = await createTestClient(server);
    client = result.client;
    cleanup = result.cleanup;
  });

  afterEach(async () => {
    await cleanup();
    // Clean up test directory
    try {
      await fs.rm(testDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe('Entity Operations', () => {
    it('should create entities successfully', async () => {
      const result = await callTool(client, 'create_entities', {
        entities: [
          { name: 'Alice', entityType: 'Person', observations: ['Likes coding'] },
          { name: 'Bob', entityType: 'Person', observations: ['Likes music'] }
        ]
      }) as Entity[];

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Alice');
      expect(result[1].name).toBe('Bob');
    });

    it('should not duplicate existing entities', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Alice', entityType: 'Person', observations: ['First'] }]
      });

      // Exact same entity should be silently skipped
      const result = await callTool(client, 'create_entities', {
        entities: [
          { name: 'Alice', entityType: 'Person', observations: ['First'] },
          { name: 'Bob', entityType: 'Person', observations: ['New'] }
        ]
      }) as Entity[];

      // Only Bob should be returned as new
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Bob');
    });

    it('should error on duplicate name with different data', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Alice', entityType: 'Person', observations: ['First'] }]
      });

      // Same name, different type — should error
      await expect(
        callTool(client, 'create_entities', {
          entities: [{ name: 'Alice', entityType: 'Organization', observations: ['First'] }]
        })
      ).rejects.toThrow(/already exists/);

      // Same name, different observations — should error
      await expect(
        callTool(client, 'create_entities', {
          entities: [{ name: 'Alice', entityType: 'Person', observations: ['Different'] }]
        })
      ).rejects.toThrow(/already exists/);
    });

    it('should reject entities with more than 2 observations', async () => {
      await expect(
        callTool(client, 'create_entities', {
          entities: [{
            name: 'TooMany',
            entityType: 'Test',
            observations: ['One', 'Two', 'Three']
          }]
        })
      ).rejects.toThrow(/Maximum allowed is 2/);
    });

    it('should reject observations longer than 140 characters', async () => {
      const longObservation = 'x'.repeat(141);
      await expect(
        callTool(client, 'create_entities', {
          entities: [{
            name: 'LongObs',
            entityType: 'Test',
            observations: [longObservation]
          }]
        })
      ).rejects.toThrow(/exceeds 140 characters/);
    });

    it('should delete entities and their relations', async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'A', entityType: 'Node', observations: [] },
          { name: 'B', entityType: 'Node', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [{ from: 'A', to: 'B', relationType: 'connects' }]
      });

      await callTool(client, 'delete_entities', { entityNames: ['A'] });

      const stats = await callTool(client, 'get_stats', {}) as { entityCount: number; relationCount: number };
      expect(stats.entityCount).toBe(1);
      expect(stats.relationCount).toBe(0); // Relation should be deleted too
    });
  });

  describe('Observation Operations', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'TestEntity', entityType: 'Test', observations: [] }]
      });
    });

    it('should add observations to entities', async () => {
      const result = await callTool(client, 'add_observations', {
        observations: [{
          entityName: 'TestEntity',
          contents: ['New observation']
        }]
      }) as Array<{ entityName: string; addedObservations: string[] }>;

      expect(result[0].addedObservations).toContain('New observation');
    });

    it('should not duplicate existing observations', async () => {
      await callTool(client, 'add_observations', {
        observations: [{ entityName: 'TestEntity', contents: ['Existing'] }]
      });

      const result = await callTool(client, 'add_observations', {
        observations: [{ entityName: 'TestEntity', contents: ['Existing', 'New'] }]
      }) as Array<{ entityName: string; addedObservations: string[] }>;

      expect(result[0].addedObservations).toEqual(['New']);
    });

    it('should reject adding observations that would exceed limit', async () => {
      await callTool(client, 'add_observations', {
        observations: [{ entityName: 'TestEntity', contents: ['One', 'Two'] }]
      });

      await expect(
        callTool(client, 'add_observations', {
          observations: [{ entityName: 'TestEntity', contents: ['Three'] }]
        })
      ).rejects.toThrow(/would exceed limit of 2/);
    });

    it('should delete specific observations', async () => {
      await callTool(client, 'add_observations', {
        observations: [{ entityName: 'TestEntity', contents: ['Keep', 'Delete'] }]
      });

      await callTool(client, 'delete_observations', {
        deletions: [{ entityName: 'TestEntity', observations: ['Delete'] }]
      });

      const result = await callTool(client, 'open_nodes', { names: ['TestEntity'] }) as PaginatedGraph;
      expect(result.entities.items[0].observations).toEqual(['Keep']);
    });
  });

  describe('Relation Operations', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'A', entityType: 'Node', observations: [] },
          { name: 'B', entityType: 'Node', observations: [] },
          { name: 'C', entityType: 'Node', observations: [] }
        ]
      });
    });

    it('should create relations', async () => {
      const result = await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'B', relationType: 'connects' },
          { from: 'B', to: 'C', relationType: 'connects' }
        ]
      }) as Relation[];

      expect(result).toHaveLength(2);
    });

    it('should not duplicate relations', async () => {
      await callTool(client, 'create_relations', {
        relations: [{ from: 'A', to: 'B', relationType: 'connects' }]
      });

      const result = await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'B', relationType: 'connects' },
          { from: 'A', to: 'C', relationType: 'connects' }
        ]
      }) as Relation[];

      expect(result).toHaveLength(1);
      expect(result[0].to).toBe('C');
    });

    it('should delete relations', async () => {
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'B', relationType: 'connects' },
          { from: 'B', to: 'C', relationType: 'connects' }
        ]
      });

      await callTool(client, 'delete_relations', {
        relations: [{ from: 'A', to: 'B', relationType: 'connects' }]
      });

      const stats = await callTool(client, 'get_stats', {}) as { relationCount: number };
      expect(stats.relationCount).toBe(1);
    });
  });

  describe('Search Operations', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'JavaScript', entityType: 'Language', observations: ['Dynamic typing'] },
          { name: 'TypeScript', entityType: 'Language', observations: ['Static typing'] },
          { name: 'Python', entityType: 'Language', observations: ['Dynamic typing'] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [{ from: 'TypeScript', to: 'JavaScript', relationType: 'extends' }]
      });
    });

    it('should search by regex pattern', async () => {
      // Accumulate all entities across pagination
      const allEntities: Entity[] = [];
      let entityCursor: number | null = 0;

      while (entityCursor !== null) {
        const result = await callTool(client, 'search_nodes', {
          query: 'Script',
          entityCursor
        }) as PaginatedGraph;

        allEntities.push(...result.entities.items);
        entityCursor = result.entities.nextCursor;
      }

      expect(allEntities).toHaveLength(2);
      expect(allEntities.map(e => e.name)).toContain('JavaScript');
      expect(allEntities.map(e => e.name)).toContain('TypeScript');
    });

    it('should search with alternation', async () => {
      // Accumulate all entities across pagination
      const allEntities: Entity[] = [];
      let entityCursor: number | null = 0;

      while (entityCursor !== null) {
        const result = await callTool(client, 'search_nodes', {
          query: 'JavaScript|Python',
          entityCursor
        }) as PaginatedGraph;

        allEntities.push(...result.entities.items);
        entityCursor = result.entities.nextCursor;
      }

      expect(allEntities).toHaveLength(2);
    });

    it('should search in observations', async () => {
      const result = await callTool(client, 'search_nodes', {
        query: 'Static'
      }) as PaginatedGraph;

      expect(result.entities.items).toHaveLength(1);
      expect(result.entities.items[0].name).toBe('TypeScript');
    });

    it('should reject invalid regex', async () => {
      await expect(
        callTool(client, 'search_nodes', { query: '[invalid' })
      ).rejects.toThrow(/Invalid regex pattern/);
    });
  });

  describe('Node Retrieval', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'A', entityType: 'Node', observations: ['Root'] },
          { name: 'B', entityType: 'Node', observations: [] },
          { name: 'C', entityType: 'Node', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'B', relationType: 'parent_of' },
          { from: 'A', to: 'C', relationType: 'parent_of' }
        ]
      });
    });

    it('should open nodes by name', async () => {
      const result = await callTool(client, 'open_nodes', {
        names: ['A', 'B']
      }) as PaginatedGraph;

      expect(result.entities.items).toHaveLength(2);
      // open_nodes returns all relations where 'from' is in the requested set
      // A->B and A->C both have from='A' which is in the set
      expect(result.relations.items).toHaveLength(2);
    });
  });

  describe('Graph Traversal', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Root', entityType: 'Node', observations: [] },
          { name: 'Child1', entityType: 'Node', observations: [] },
          { name: 'Child2', entityType: 'Node', observations: [] },
          { name: 'Grandchild', entityType: 'Node', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'Root', to: 'Child1', relationType: 'parent_of' },
          { from: 'Root', to: 'Child2', relationType: 'parent_of' },
          { from: 'Child1', to: 'Grandchild', relationType: 'parent_of' }
        ]
      });
    });

    it('should get immediate neighbors at depth 0', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 0
      }) as PaginatedResult<Neighbor>;

      // depth 0 returns immediate neighbors only
      expect(result.items).toHaveLength(2);
      const names = result.items.map(n => n.name);
      expect(names).toContain('Child1');
      expect(names).toContain('Child2');
    });

    it('should get neighbors at depth 1 (includes neighbors of neighbors)', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 1
      }) as PaginatedResult<Neighbor>;

      // depth 1: Child1, Child2 (immediate) + Grandchild (neighbor of Child1)
      expect(result.items).toHaveLength(3);
      const names = result.items.map(n => n.name);
      expect(names).toContain('Child1');
      expect(names).toContain('Child2');
      expect(names).toContain('Grandchild');
    });

    it('should traverse to specified depth', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 2
      }) as PaginatedResult<Neighbor>;

      // At depth 2 from Root: same as depth 1 since graph is small
      // Child1, Child2, Grandchild (Root is excluded as starting point)
      expect(result.items).toHaveLength(3);
      const names = result.items.map(n => n.name);
      expect(names).toContain('Child1');
      expect(names).toContain('Child2');
      expect(names).toContain('Grandchild');
    });

    it('should deduplicate neighbors in traversal', async () => {
      // Add a bidirectional relation
      await callTool(client, 'create_relations', {
        relations: [{ from: 'Child2', to: 'Root', relationType: 'child_of' }]
      });

      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 1
      }) as PaginatedResult<Neighbor>;

      // Each neighbor should appear only once
      const names = result.items.map(n => n.name);
      const uniqueNames = [...new Set(names)];
      expect(names.length).toBe(uniqueNames.length);
    });

    it('should find path between entities', async () => {
      const result = await callTool(client, 'find_path', {
        fromEntity: 'Root',
        toEntity: 'Grandchild'
      }) as PaginatedResult<Relation>;

      expect(result.items).toHaveLength(2);
      expect(result.items[0].from).toBe('Root');
      expect(result.items[1].to).toBe('Grandchild');
    });

    it('should return empty path when no path exists', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Isolated', entityType: 'Node', observations: [] }]
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'Root',
        toEntity: 'Isolated'
      }) as PaginatedResult<Relation>;

      expect(result.items).toHaveLength(0);
    });
  });

  describe('Type Queries', () => {
    beforeEach(async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Alice', entityType: 'Person', observations: [] },
          { name: 'Bob', entityType: 'Person', observations: [] },
          { name: 'Acme', entityType: 'Company', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'Alice', to: 'Acme', relationType: 'works_at' },
          { from: 'Bob', to: 'Acme', relationType: 'works_at' }
        ]
      });
    });

    it('should get entities by type', async () => {
      const result = await callTool(client, 'get_entities_by_type', {
        entityType: 'Person'
      }) as PaginatedResult<Entity>;

      expect(result.items).toHaveLength(2);
      expect(result.items.every(e => e.entityType === 'Person')).toBe(true);
    });

    it('should get all entity types', async () => {
      const result = await callTool(client, 'get_entity_types', {}) as string[];

      expect(result).toContain('Person');
      expect(result).toContain('Company');
      expect(result).toHaveLength(2);
    });

    it('should get all relation types', async () => {
      const result = await callTool(client, 'get_relation_types', {}) as string[];

      expect(result).toContain('works_at');
      expect(result).toHaveLength(1);
    });
  });

  describe('Statistics and Validation', () => {
    it('should return correct stats', async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'A', entityType: 'Type1', observations: [] },
          { name: 'B', entityType: 'Type2', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [{ from: 'A', to: 'B', relationType: 'rel1' }]
      });

      const stats = await callTool(client, 'get_stats', {}) as {
        entityCount: number;
        relationCount: number;
        entityTypes: number;
        relationTypes: number;
      };

      expect(stats.entityCount).toBe(2);
      expect(stats.relationCount).toBe(1);
      expect(stats.entityTypes).toBe(2);
      expect(stats.relationTypes).toBe(1);
    });

    it('should find orphaned entities', async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Connected1', entityType: 'Node', observations: [] },
          { name: 'Connected2', entityType: 'Node', observations: [] },
          { name: 'Orphan', entityType: 'Node', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [{ from: 'Connected1', to: 'Connected2', relationType: 'links' }]
      });

      const result = await callTool(client, 'get_orphaned_entities', {}) as PaginatedResult<Entity>;

      expect(result.items).toHaveLength(1);
      expect(result.items[0].name).toBe('Orphan');
    });

    it('should find entities not connected to Self in strict mode', async () => {
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Self', entityType: 'Agent', observations: [] },
          { name: 'ConnectedToSelf', entityType: 'Node', observations: [] },
          { name: 'IndirectlyConnected', entityType: 'Node', observations: [] },
          { name: 'Island1', entityType: 'Node', observations: [] },
          { name: 'Island2', entityType: 'Node', observations: [] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'Self', to: 'ConnectedToSelf', relationType: 'knows' },
          { from: 'ConnectedToSelf', to: 'IndirectlyConnected', relationType: 'links' },
          { from: 'Island1', to: 'Island2', relationType: 'links' }  // Connected to each other but not to Self
        ]
      });

      // Non-strict: Island1 and Island2 are connected, so not orphaned
      const nonStrict = await callTool(client, 'get_orphaned_entities', {}) as PaginatedResult<Entity>;
      expect(nonStrict.items).toHaveLength(0);

      // Strict: Island1 and Island2 are not connected to Self
      const strict = await callTool(client, 'get_orphaned_entities', { strict: true }) as PaginatedResult<Entity>;
      expect(strict.items).toHaveLength(2);
      const names = strict.items.map(e => e.name).sort();
      expect(names).toEqual(['Island1', 'Island2']);
    });

    it('should validate graph and report violations', async () => {
      // Directly write invalid data to test validation
      const invalidData = [
        JSON.stringify({ type: 'entity', name: 'Valid', entityType: 'Test', observations: [] }),
        JSON.stringify({ type: 'relation', from: 'Valid', to: 'Missing', relationType: 'refs' })
      ].join('\n');
      await fs.writeFile(memoryFile, invalidData);

      const result = await callTool(client, 'validate_graph', {}) as {
        missingEntities: string[];
        observationViolations: Array<{ entity: string; count: number; oversizedObservations: number[] }>;
      };

      expect(result.missingEntities).toContain('Missing');
    });
  });

  describe('Sequential Thinking', () => {
    it('should create a thought and return ctxId', async () => {
      const result = await callTool(client, 'sequentialthinking', {
        observations: ['First thought observation']
      }) as { ctxId: string };

      expect(result.ctxId).toMatch(/^[0-9a-f]{24}$/);
    });

    it('should chain thoughts with relations', async () => {
      // Create first thought
      const first = await callTool(client, 'sequentialthinking', {
        observations: ['Starting point']
      }) as { ctxId: string };

      // Create second thought chained to first
      const second = await callTool(client, 'sequentialthinking', {
        previousCtxId: first.ctxId,
        observations: ['Following up']
      }) as { ctxId: string };

      // Verify the chain via neighbors
      const neighbors = await callTool(client, 'get_neighbors', {
        entityName: first.ctxId,
        depth: 1
      }) as PaginatedResult<Neighbor>;

      // Second thought should be a neighbor of first
      expect(neighbors.items.some(n => n.name === second.ctxId)).toBe(true);
    });

    it('should ignore invalid previousCtxId gracefully', async () => {
      const result = await callTool(client, 'sequentialthinking', {
        previousCtxId: 'nonexistent_thought',
        observations: ['Orphaned thought']
      }) as { ctxId: string };

      expect(result.ctxId).toMatch(/^[0-9a-f]{24}$/);

      // Verify no neighbors (no valid relations were created)
      const neighbors = await callTool(client, 'get_neighbors', {
        entityName: result.ctxId,
        depth: 1
      }) as PaginatedResult<Neighbor>;

      expect(neighbors.items).toHaveLength(0);
    });

    it('should enforce observation limits on thoughts', async () => {
      await expect(
        callTool(client, 'sequentialthinking', {
          observations: ['One', 'Two', 'Three']
        })
      ).rejects.toThrow(/Maximum allowed is 2/);
    });

    it('should set mtime and obsMtime on thought entities', async () => {
      const result = await callTool(client, 'sequentialthinking', {
        observations: ['Timed thought']
      }) as { ctxId: string };

      const graph = await callTool(client, 'open_nodes', {
        names: [result.ctxId]
      }) as PaginatedGraph;

      const thought = graph.entities.items[0];
      expect(thought.mtime).toBeDefined();
      expect(thought.obsMtime).toBeDefined();
    });
  });

  describe('Timestamp Decoding', () => {
    it('should decode a specific timestamp', async () => {
      const result = await callTool(client, 'decode_timestamp', {
        timestamp: 1735200000000  // Known timestamp
      }) as { timestamp: number; iso8601: string; formatted: string };

      expect(result.timestamp).toBe(1735200000000);
      expect(result.iso8601).toBe('2024-12-26T08:00:00.000Z');
      expect(result.formatted).toContain('2024');
    });

    it('should return current time when no timestamp provided', async () => {
      const before = Date.now();
      const result = await callTool(client, 'decode_timestamp', {}) as { timestamp: number };
      const after = Date.now();

      expect(result.timestamp).toBeGreaterThanOrEqual(before);
      expect(result.timestamp).toBeLessThanOrEqual(after);
    });

    it('should include relative time when requested', async () => {
      const oneHourAgo = Date.now() - 3600000;
      const result = await callTool(client, 'decode_timestamp', {
        timestamp: oneHourAgo,
        relative: true
      }) as { relative: string };

      expect(result.relative).toContain('hour');
      expect(result.relative).toContain('ago');
    });

    it('should handle future timestamps', async () => {
      const oneHourFromNow = Date.now() + 3600000;
      const result = await callTool(client, 'decode_timestamp', {
        timestamp: oneHourFromNow,
        relative: true
      }) as { relative: string };

      expect(result.relative).toContain('in');
    });
  });

  describe('Random Walk', () => {
    beforeEach(async () => {
      // Create a small graph for walking
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Center', entityType: 'Node', observations: ['Hub node'] },
          { name: 'North', entityType: 'Node', observations: ['North node'] },
          { name: 'South', entityType: 'Node', observations: ['South node'] },
          { name: 'East', entityType: 'Node', observations: ['East node'] },
          { name: 'Isolated', entityType: 'Node', observations: ['No connections'] },
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'Center', to: 'North', relationType: 'connects' },
          { from: 'Center', to: 'South', relationType: 'connects' },
          { from: 'Center', to: 'East', relationType: 'connects' },
          { from: 'North', to: 'South', relationType: 'connects' },
        ]
      });
    });

    it('should perform a walk and return path', async () => {
      const result = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 2
      }) as { entity: string; path: string[] };

      expect(result.path[0]).toBe('Center');
      expect(result.path.length).toBeGreaterThanOrEqual(1);
      expect(result.path.length).toBeLessThanOrEqual(3);
      expect(result.entity).toBe(result.path[result.path.length - 1]);
    });

    it('should terminate early at dead ends', async () => {
      const result = await callTool(client, 'random_walk', {
        start: 'Isolated',
        depth: 5
      }) as { entity: string; path: string[] };

      expect(result.path).toEqual(['Isolated']);
      expect(result.entity).toBe('Isolated');
    });

    it('should produce reproducible walks with same seed', async () => {
      const result1 = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 3,
        seed: 'test-seed-123'
      }) as { entity: string; path: string[] };

      const result2 = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 3,
        seed: 'test-seed-123'
      }) as { entity: string; path: string[] };

      expect(result1.path).toEqual(result2.path);
    });

    it('should throw on non-existent start entity', async () => {
      await expect(
        callTool(client, 'random_walk', { start: 'NonExistent', depth: 2 })
      ).rejects.toThrow(/not found/);
    });
  });

  describe('Sorting', () => {
    // Helper to create entities with controlled timestamps
    async function createEntitiesWithDelay(entities: Array<{ name: string; entityType: string; observations: string[] }>) {
      for (const entity of entities) {
        await callTool(client, 'create_entities', { entities: [entity] });
        // Small delay to ensure distinct mtime values
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }

    describe('search_nodes sorting', () => {
      beforeEach(async () => {
        // Create entities with distinct timestamps
        await createEntitiesWithDelay([
          { name: 'Alpha', entityType: 'Letter', observations: ['First letter'] },
          { name: 'Beta', entityType: 'Letter', observations: ['Second letter'] },
          { name: 'Gamma', entityType: 'Letter', observations: ['Third letter'] }
        ]);
      });

      it('should preserve insertion order when sortBy is omitted', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        expect(names).toEqual(['Alpha', 'Beta', 'Gamma']);
      });

      it('should sort by name ascending', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter',
          sortBy: 'name',
          sortDir: 'asc'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        expect(names).toEqual(['Alpha', 'Beta', 'Gamma']);
      });

      it('should sort by name descending', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter',
          sortBy: 'name',
          sortDir: 'desc'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        expect(names).toEqual(['Gamma', 'Beta', 'Alpha']);
      });

      it('should sort by mtime descending by default', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter',
          sortBy: 'mtime'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        // Gamma was created last, so should be first when sorted desc
        expect(names).toEqual(['Gamma', 'Beta', 'Alpha']);
      });

      it('should sort by mtime ascending when specified', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter',
          sortBy: 'mtime',
          sortDir: 'asc'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        // Alpha was created first, so should be first when sorted asc
        expect(names).toEqual(['Alpha', 'Beta', 'Gamma']);
      });

      it('should sort by obsMtime', async () => {
        // Update observation on Alpha to make it have most recent obsMtime
        await callTool(client, 'delete_observations', {
          deletions: [{ entityName: 'Alpha', observations: ['First letter'] }]
        });
        await callTool(client, 'add_observations', {
          observations: [{ entityName: 'Alpha', contents: ['Updated'] }]
        });

        const result = await callTool(client, 'search_nodes', {
          query: 'Letter|Updated',
          sortBy: 'obsMtime'
        }) as PaginatedGraph;

        const names = result.entities.items.map(e => e.name);
        // Alpha should be first because its obsMtime was just updated
        expect(names[0]).toBe('Alpha');
      });
    });

    describe('get_entities_by_type sorting', () => {
      beforeEach(async () => {
        await createEntitiesWithDelay([
          { name: 'Zebra', entityType: 'Animal', observations: ['Striped'] },
          { name: 'Aardvark', entityType: 'Animal', observations: ['Nocturnal'] },
          { name: 'Monkey', entityType: 'Animal', observations: ['Clever'] }
        ]);
      });

      it('should preserve insertion order when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Zebra', 'Aardvark', 'Monkey']);
      });

      it('should sort by name ascending (default for name)', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal',
          sortBy: 'name'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Aardvark', 'Monkey', 'Zebra']);
      });

      it('should sort by name descending', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal',
          sortBy: 'name',
          sortDir: 'desc'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Zebra', 'Monkey', 'Aardvark']);
      });

      it('should sort by mtime descending (default for mtime)', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal',
          sortBy: 'mtime'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        // Monkey was created last
        expect(names).toEqual(['Monkey', 'Aardvark', 'Zebra']);
      });

      it('should sort by mtime ascending', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal',
          sortBy: 'mtime',
          sortDir: 'asc'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        // Zebra was created first
        expect(names).toEqual(['Zebra', 'Aardvark', 'Monkey']);
      });
    });

    describe('get_orphaned_entities sorting', () => {
      beforeEach(async () => {
        // Create orphaned entities (no relations)
        await createEntitiesWithDelay([
          { name: 'Orphan_Z', entityType: 'Orphan', observations: ['Alone'] },
          { name: 'Orphan_A', entityType: 'Orphan', observations: ['Solo'] },
          { name: 'Orphan_M', entityType: 'Orphan', observations: ['Isolated'] }
        ]);
      });

      it('should preserve insertion order when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_orphaned_entities', {}) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Orphan_Z', 'Orphan_A', 'Orphan_M']);
      });

      it('should sort by name ascending', async () => {
        const result = await callTool(client, 'get_orphaned_entities', {
          sortBy: 'name'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Orphan_A', 'Orphan_M', 'Orphan_Z']);
      });

      it('should sort by name descending', async () => {
        const result = await callTool(client, 'get_orphaned_entities', {
          sortBy: 'name',
          sortDir: 'desc'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        expect(names).toEqual(['Orphan_Z', 'Orphan_M', 'Orphan_A']);
      });

      it('should sort by mtime descending (default)', async () => {
        const result = await callTool(client, 'get_orphaned_entities', {
          sortBy: 'mtime'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        // Orphan_M was created last
        expect(names).toEqual(['Orphan_M', 'Orphan_A', 'Orphan_Z']);
      });

      it('should work with strict mode and sorting', async () => {
        // Create Self and connect one orphan to it
        await callTool(client, 'create_entities', {
          entities: [{ name: 'Self', entityType: 'Agent', observations: [] }]
        });
        await callTool(client, 'create_relations', {
          relations: [{ from: 'Self', to: 'Orphan_A', relationType: 'knows' }]
        });

        const result = await callTool(client, 'get_orphaned_entities', {
          strict: true,
          sortBy: 'name'
        }) as PaginatedResult<Entity>;

        const names = result.items.map(e => e.name);
        // Orphan_A is now connected to Self, so only M and Z are orphaned
        expect(names).toEqual(['Orphan_M', 'Orphan_Z']);
      });
    });

    describe('get_neighbors sorting', () => {
      beforeEach(async () => {
        // Create a hub with neighbors created at different times
        await callTool(client, 'create_entities', {
          entities: [{ name: 'Hub', entityType: 'Center', observations: [] }]
        });
        
        await createEntitiesWithDelay([
          { name: 'Neighbor_Z', entityType: 'Node', observations: ['First'] },
          { name: 'Neighbor_A', entityType: 'Node', observations: ['Second'] },
          { name: 'Neighbor_M', entityType: 'Node', observations: ['Third'] }
        ]);

        // Connect all to Hub
        await callTool(client, 'create_relations', {
          relations: [
            { from: 'Hub', to: 'Neighbor_Z', relationType: 'connects' },
            { from: 'Hub', to: 'Neighbor_A', relationType: 'connects' },
            { from: 'Hub', to: 'Neighbor_M', relationType: 'connects' }
          ]
        });
      });

      it('should return unsorted neighbors when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub'
        }) as PaginatedResult<Neighbor>;

        expect(result.items).toHaveLength(3);
        // Just verify all neighbors are present
        const names = result.items.map(n => n.name);
        expect(names).toContain('Neighbor_Z');
        expect(names).toContain('Neighbor_A');
        expect(names).toContain('Neighbor_M');
      });

      it('should sort neighbors by name ascending', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'name'
        }) as PaginatedResult<Neighbor>;

        const names = result.items.map(n => n.name);
        expect(names).toEqual(['Neighbor_A', 'Neighbor_M', 'Neighbor_Z']);
      });

      it('should sort neighbors by name descending', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'name',
          sortDir: 'desc'
        }) as PaginatedResult<Neighbor>;

        const names = result.items.map(n => n.name);
        expect(names).toEqual(['Neighbor_Z', 'Neighbor_M', 'Neighbor_A']);
      });

      it('should sort neighbors by mtime descending (default)', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'mtime'
        }) as PaginatedResult<Neighbor>;

        const names = result.items.map(n => n.name);
        // Neighbor_M was created last
        expect(names).toEqual(['Neighbor_M', 'Neighbor_A', 'Neighbor_Z']);
      });

      it('should sort neighbors by mtime ascending', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'mtime',
          sortDir: 'asc'
        }) as PaginatedResult<Neighbor>;

        const names = result.items.map(n => n.name);
        // Neighbor_Z was created first
        expect(names).toEqual(['Neighbor_Z', 'Neighbor_A', 'Neighbor_M']);
      });

      it('should include mtime and obsMtime in neighbor objects', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'name'
        }) as PaginatedResult<Neighbor>;

        // Each neighbor should have timestamp fields
        for (const neighbor of result.items) {
          expect(neighbor.mtime).toBeDefined();
          expect(neighbor.obsMtime).toBeDefined();
          expect(typeof neighbor.mtime).toBe('number');
          expect(typeof neighbor.obsMtime).toBe('number');
        }
      });

      it('should sort by obsMtime after observation update', async () => {
        // Update observation on Neighbor_Z to make it have most recent obsMtime
        await callTool(client, 'delete_observations', {
          deletions: [{ entityName: 'Neighbor_Z', observations: ['First'] }]
        });
        await callTool(client, 'add_observations', {
          observations: [{ entityName: 'Neighbor_Z', contents: ['Updated recently'] }]
        });

        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub',
          sortBy: 'obsMtime'
        }) as PaginatedResult<Neighbor>;

        const names = result.items.map(n => n.name);
        // Neighbor_Z should be first because its obsMtime was just updated
        expect(names[0]).toBe('Neighbor_Z');
      });
    });

    describe('sorting with pagination', () => {
      it('should maintain sort order across paginated results', async () => {
        // Create many entities to force pagination
        const entities = [];
        for (let i = 0; i < 20; i++) {
          entities.push({
            name: `Entity_${String(i).padStart(2, '0')}`,
            entityType: 'Numbered',
            observations: [`Number ${i}`]
          });
        }
        await callTool(client, 'create_entities', { entities });

        // Fetch all pages sorted by name descending
        const allEntities: Entity[] = [];
        let entityCursor: number | null = 0;

        while (entityCursor !== null) {
          const result = await callTool(client, 'search_nodes', {
            query: 'Numbered',
            sortBy: 'name',
            sortDir: 'desc',
            entityCursor
          }) as PaginatedGraph;

          allEntities.push(...result.entities.items);
          entityCursor = result.entities.nextCursor;
        }

        // Verify all entities are in descending order
        const names = allEntities.map(e => e.name);
        const sortedNames = [...names].sort().reverse();
        expect(names).toEqual(sortedNames);
      });
    });
  });
});
