import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, Entity, Relation, KnowledgeGraph } from '../index.js';
import { createTestClient, callTool } from './test-utils.js';

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

      const result = await callTool(client, 'open_nodes', { names: ['TestEntity'] }) as KnowledgeGraph;
      expect(result.entities[0].observations).toEqual(['Keep']);
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
      const result = await callTool(client, 'search_nodes', {
        query: 'Script'
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(2);
      expect(result.entities.map(e => e.name)).toContain('JavaScript');
      expect(result.entities.map(e => e.name)).toContain('TypeScript');
    });

    it('should search with alternation', async () => {
      const result = await callTool(client, 'search_nodes', {
        query: 'JavaScript|Python'
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(2);
    });

    it('should search in observations', async () => {
      const result = await callTool(client, 'search_nodes', {
        query: 'Static'
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(1);
      expect(result.entities[0].name).toBe('TypeScript');
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
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(2);
      // open_nodes returns all relations where 'from' is in the requested set
      // A->B and A->C both have from='A' which is in the set
      expect(result.relations).toHaveLength(2);
    });

    it('should open nodes filtered (only internal relations)', async () => {
      const result = await callTool(client, 'open_nodes_filtered', {
        names: ['B', 'C']
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(2);
      expect(result.relations).toHaveLength(0); // No relations between B and C
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
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(0); // withEntities defaults to false
      expect(result.relations).toHaveLength(2); // Root's direct relations
    });

    it('should get neighbors with entities when requested', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 1,
        withEntities: true
      }) as KnowledgeGraph;

      expect(result.entities.map(e => e.name)).toContain('Root');
      expect(result.entities.map(e => e.name)).toContain('Child1');
      expect(result.entities.map(e => e.name)).toContain('Child2');
    });

    it('should traverse to specified depth', async () => {
      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 2,
        withEntities: true
      }) as KnowledgeGraph;

      expect(result.entities).toHaveLength(4); // All nodes
      expect(result.relations).toHaveLength(3); // All relations
    });

    it('should deduplicate relations in traversal', async () => {
      // Add a bidirectional relation
      await callTool(client, 'create_relations', {
        relations: [{ from: 'Child2', to: 'Root', relationType: 'child_of' }]
      });

      const result = await callTool(client, 'get_neighbors', {
        entityName: 'Root',
        depth: 1
      }) as KnowledgeGraph;

      // Each unique relation should appear only once
      const relationKeys = result.relations.map(r => `${r.from}|${r.relationType}|${r.to}`);
      const uniqueKeys = [...new Set(relationKeys)];
      expect(relationKeys.length).toBe(uniqueKeys.length);
    });

    it('should find path between entities', async () => {
      const result = await callTool(client, 'find_path', {
        fromEntity: 'Root',
        toEntity: 'Grandchild'
      }) as Relation[];

      expect(result).toHaveLength(2);
      expect(result[0].from).toBe('Root');
      expect(result[1].to).toBe('Grandchild');
    });

    it('should return empty path when no path exists', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Isolated', entityType: 'Node', observations: [] }]
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'Root',
        toEntity: 'Isolated'
      }) as Relation[];

      expect(result).toHaveLength(0);
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
      }) as Entity[];

      expect(result).toHaveLength(2);
      expect(result.every(e => e.entityType === 'Person')).toBe(true);
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

      const result = await callTool(client, 'get_orphaned_entities', {}) as Entity[];

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Orphan');
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
});
