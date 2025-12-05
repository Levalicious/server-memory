import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, Entity, Relation, KnowledgeGraph } from '../server.js';
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

      const result = await callTool(client, 'create_entities', {
        entities: [
          { name: 'Alice', entityType: 'Person', observations: ['Second'] },
          { name: 'Bob', entityType: 'Person', observations: ['New'] }
        ]
      }) as Entity[];

      // Only Bob should be returned as new
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Bob');
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

    it('should open nodes filtered (only internal relations)', async () => {
      const result = await callTool(client, 'open_nodes_filtered', {
        names: ['B', 'C']
      }) as PaginatedGraph;

      expect(result.entities.items).toHaveLength(2);
      expect(result.relations.items).toHaveLength(0); // No relations between B and C
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

    it('should get neighbors at depth 0 (relations only by default)', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 0
      }) as PaginatedGraph;

      expect(result.entities.items).toHaveLength(0); // withEntities defaults to false
      expect(result.relations.items).toHaveLength(2); // Root's direct relations
    });

    it('should get neighbors with entities when requested', async () => {
      // Accumulate all entities across pagination
      const allEntities: Entity[] = [];
      let entityCursor: number | null = 0;

      while (entityCursor !== null) {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Root',
          depth: 1,
          withEntities: true,
          entityCursor
        }) as PaginatedGraph;

        allEntities.push(...result.entities.items);
        entityCursor = result.entities.nextCursor;
      }

      expect(allEntities).toHaveLength(3);
      expect(allEntities.map(e => e.name)).toContain('Root');
      expect(allEntities.map(e => e.name)).toContain('Child1');
      expect(allEntities.map(e => e.name)).toContain('Child2');
    });

    it('should traverse to specified depth', async () => {
      // Collect all entities and relations using pagination
      const allEntities: Entity[] = [];
      const allRelations: Relation[] = [];
      let entityCursor: number | null = 0;
      let relationCursor: number | null = 0;

      // Paginate through all results
      while (entityCursor !== null || relationCursor !== null) {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Root',
          depth: 2,
          withEntities: true,
          // Pass large cursor to skip when done, 0 to fetch
          entityCursor: entityCursor !== null ? entityCursor : 999999,
          relationCursor: relationCursor !== null ? relationCursor : 999999
        }) as PaginatedGraph;

        // Collect entities if we still need them
        if (entityCursor !== null) {
          allEntities.push(...result.entities.items);
          entityCursor = result.entities.nextCursor;
        }

        // Collect relations if we still need them
        if (relationCursor !== null) {
          allRelations.push(...result.relations.items);
          relationCursor = result.relations.nextCursor;
        }
      }

      expect(allEntities).toHaveLength(4); // All nodes
      expect(allRelations).toHaveLength(3); // All relations
    });

    it('should deduplicate relations in traversal', async () => {
      // Add a bidirectional relation
      await callTool(client, 'create_relations', {
        relations: [{ from: 'Child2', to: 'Root', relationType: 'child_of' }]
      });

      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 1
      }) as PaginatedGraph;

      // Each unique relation should appear only once
      const relationKeys = result.relations.items.map(r => `${r.from}|${r.relationType}|${r.to}`);
      const uniqueKeys = [...new Set(relationKeys)];
      expect(relationKeys.length).toBe(uniqueKeys.length);
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

  describe('BCL Evaluator', () => {
    it('should evaluate K combinator (identity for first arg)', async () => {
      // K = 00, evaluating K applied to two args should return first
      // This is a simplified test - BCL semantics are complex
      const result = await callTool(client, 'evaluate_bcl', {
        program: '00',
        maxSteps: 100
      }) as { result: string; halted: boolean };

      expect(result.halted).toBe(true);
    });

    it('should construct BCL terms incrementally', async () => {
      let result = await callTool(client, 'add_bcl_term', { term: 'App' });
      expect(result).toContain('more term');

      result = await callTool(client, 'add_bcl_term', { term: 'K' });
      expect(result).toContain('more term');

      result = await callTool(client, 'add_bcl_term', { term: 'S' });
      expect(result).toContain('Constructed Program');
    });

    it('should clear BCL constructor state', async () => {
      await callTool(client, 'add_bcl_term', { term: 'App' });
      await callTool(client, 'clear_bcl_term', {});

      // After clearing, we should need to start fresh
      const result = await callTool(client, 'add_bcl_term', { term: 'K' });
      expect(result).toContain('Constructed Program');
    });

    it('should reject invalid BCL terms', async () => {
      await expect(
        callTool(client, 'add_bcl_term', { term: 'invalid' })
      ).rejects.toThrow(/Invalid BCL term/);
    });
  });

  describe('Sequential Thinking', () => {
    it('should create a thought and return ctxId', async () => {
      const result = await callTool(client, 'sequentialthinking', {
        observations: ['First thought observation']
      }) as { ctxId: string };

      expect(result.ctxId).toMatch(/^thought_\d+_[a-z0-9]+$/);
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

      // Verify the chain via relations
      const neighbors = await callTool(client, 'get_neighbors', {
        entityName: first.ctxId,
        depth: 1
      }) as PaginatedGraph;

      // Should have 'follows' relation from first to second
      expect(neighbors.relations.items.some(r => 
        r.from === first.ctxId && r.to === second.ctxId && r.relationType === 'follows'
      )).toBe(true);
    });

    it('should ignore invalid previousCtxId gracefully', async () => {
      const result = await callTool(client, 'sequentialthinking', {
        previousCtxId: 'nonexistent_thought',
        observations: ['Orphaned thought']
      }) as { ctxId: string };

      expect(result.ctxId).toMatch(/^thought_\d+_[a-z0-9]+$/);

      // Verify no relations were created
      const neighbors = await callTool(client, 'get_neighbors', {
        entityName: result.ctxId,
        depth: 1
      }) as PaginatedGraph;

      expect(neighbors.relations.totalCount).toBe(0);
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
});
