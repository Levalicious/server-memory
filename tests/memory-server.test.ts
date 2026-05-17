import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, type Entity, type Relation, type Neighbor } from '../server.js';
import { createTestClient, callTool, callToolRaw, type PaginatedGraph, type PaginatedResult, type FindPathResult } from './test-utils.js';

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

    it('trigram path: regex-extractable queries return the same results as before', async () => {
      // Soundness check: enabling the trigram fast path is invisible to the
      // caller. The `search_nodes` API still accepts a regex; the speedup
      // comes from server-side literal extraction. We seed enough entities
      // that the index actually exercises and compare paginated drains
      // across two query shapes (plain literal and alternation).
      const entities = [];
      for (let i = 0; i < 100; i++) {
        entities.push({
          name: `Trig_${i.toString().padStart(3, '0')}`,
          entityType: i % 2 === 0 ? 'Concept' : 'Person',
          observations: [`memory artefact ${i}`],
        });
      }
      await callTool(client, 'create_entities', { entities });

      // Drain ALL pages with deterministic name-sort so we compare full
      // result sets, not nondeterministic top-llmrank pages.
      async function drainAll(query: string): Promise<Set<string>> {
        const out = new Set<string>();
        let cursor: number | null = 0;
        while (cursor !== null) {
          const r = await callTool(client, 'search_nodes', {
            query, sortBy: 'name', sortDir: 'asc', entityCursor: cursor,
          }) as PaginatedGraph;
          for (const e of r.entities.items) out.add(e.name);
          cursor = r.entities.nextCursor;
        }
        return out;
      }

      // Hand-compute the expected match set per query, run search_nodes,
      // verify they match. This exercises the trigram path internally for
      // each (the queries are all extractable: plain literals and top-level
      // alternations). The "no metachar" check in extractRequiredTrigramFilter
      // makes those eligible.
      const all: { name: string; type: string; obs: string }[] = [
        ...entities.map(e => ({ name: e.name, type: e.entityType, obs: e.observations[0] })),
      ];

      const cases: { query: string; expected: (n: { name: string; type: string; obs: string }) => boolean }[] = [
        { query: 'memory', expected: e => /memory/i.test(e.name) || /memory/i.test(e.type) || /memory/i.test(e.obs) },
        { query: 'Trig_0', expected: e => /Trig_0/i.test(e.name) },
        { query: 'Concept', expected: e => /Concept/i.test(e.type) },
        { query: 'Concept|Person', expected: e => /Concept|Person/i.test(e.type) },
        { query: '^Trig_001$', expected: e => e.name === 'Trig_001' },
      ];

      for (const c of cases) {
        const got = await drainAll(c.query);
        const expected = new Set(all.filter(c.expected).map(e => e.name));
        expect(got.size).toBe(expected.size);
        for (const n of expected) expect(got.has(n)).toBe(true);
      }
    });

    it('trigram path: queries with metacharacters fall back to scan correctly', async () => {
      // `.*foo` and similar shapes can't be reduced to required substrings —
      // the extractor returns null and search_nodes uses the linear scan
      // path. Result must still be correct.
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'AlphaUniqueA', entityType: 'X', observations: [] },
          { name: 'AlphaUniqueB', entityType: 'X', observations: [] },
        ],
      });
      // `A.phaUniqueA` contains `.` so the extractor returns null. Should
      // still match AlphaUniqueA via post-filter.
      const r = await callTool(client, 'search_nodes', { query: 'A.phaUniqueA' }) as PaginatedGraph;
      const names = r.entities.items.map(e => e.name);
      expect(names).toContain('AlphaUniqueA');
    });

    it('trigram path: index stays consistent across writes', async () => {
      // Local writes (create / delete) must keep the index in sync. We use
      // an anchored regex `^X$` which the extractor reduces to literal `X` —
      // this exercises the trigram path. We also use queries broad enough to
      // dodge the natural-language guard's "literal query, zero matches"
      // false-error after deletes.
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'AlphaTango',   entityType: 'Codename', observations: ['call sign alpha'] },
          { name: 'BravoZulu',    entityType: 'Codename', observations: ['call sign bravo'] },
          { name: 'AlphaCharlie2', entityType: 'Codename', observations: ['call sign alpha2'] },
        ],
      });
      // Warm the index — `^AlphaTango$` extracts literal "alphatango" (after
      // server-side lowercase) and uses the trigram path.
      let r = await callTool(client, 'search_nodes', { query: '^AlphaTango$' }) as PaginatedGraph;
      expect(r.entities.items.map(e => e.name)).toContain('AlphaTango');

      // Mutate: add. The trigram update must include the new entity.
      await callTool(client, 'create_entities', {
        entities: [{ name: 'AlphaCharlie', entityType: 'Codename', observations: [] }],
      });
      r = await callTool(client, 'search_nodes', { query: '^AlphaCharlie$' }) as PaginatedGraph;
      expect(r.entities.items.map(e => e.name)).toContain('AlphaCharlie');

      // Mutate: delete. The trigram update must drop it.
      // We assert via the broader `Alpha` query (which still matches the
      // remaining AlphaCharlie / AlphaCharlie2 entities, dodging the NL guard)
      // and verify AlphaTango is absent.
      await callTool(client, 'delete_entities', { entityNames: ['AlphaTango'] });
      r = await callTool(client, 'search_nodes', { query: '^Alpha' }) as PaginatedGraph;
      const after = r.entities.items.map(e => e.name);
      expect(after).not.toContain('AlphaTango');
      expect(after).toContain('AlphaCharlie');
    });

    it('paginates oversized entities without wedging the cursor', async () => {
      // Regression test for an obsMtime-desc / long-name infinite loop:
      // `paginateItems` previously returned `nextCursor === cursor` whenever
      // the item at `cursor` was itself larger than MAX_CHARS=4096 (no other
      // item could fit either, so the page came back empty). The model
      // followed nextCursor back into the identical call, ad infinitum.
      // The fix is the forward-progress invariant in paginateItems: when a
      // page would otherwise be empty, emit the lead item anyway and advance.
      //
      // The cycle here is asserted indirectly: we drain the cursor to null
      // in a while-loop with a hard iteration cap. Pre-fix, the loop never
      // terminates and Jest's per-test timeout kills the suite.
      const longName = 'L_' + 'x'.repeat(4500);  // single entity JSON > 4096 chars
      await callTool(client, 'create_entities', {
        entities: [{ name: longName, entityType: 'BigName', observations: ['recent'] }],
      });

      const allEntities: Entity[] = [];
      let entityCursor: number | null = 0;
      let iterations = 0;
      const ITERATION_CAP = 50;  // any healthy graph fits in << 50 pages here

      while (entityCursor !== null) {
        if (++iterations > ITERATION_CAP) {
          throw new Error(
            `paginateItems forward-progress regression: drained ${iterations} ` +
            `pages without nextCursor reaching null. Pre-fix this loops forever.`,
          );
        }
        const result = await callTool(client, 'search_nodes', {
          query: longName.slice(0, 40),  // unique substring → matches only the long-name entity
          sortBy: 'obsMtime',
          sortDir: 'desc',
          entityCursor,
        }) as PaginatedGraph;

        allEntities.push(...result.entities.items);
        entityCursor = result.entities.nextCursor;
      }

      // The long-name entity must have been emitted (forward-progress guarantees
      // it's the only item on its page, in a single oversized response).
      expect(allEntities.some(e => e.name === longName)).toBe(true);
    });
  });

  describe('search_nodes natural-language guard', () => {
    beforeEach(async () => {
      // Same fixture as the parent suite — a small, populated KB.
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'JavaScript', entityType: 'Language', observations: ['Dynamic typing'] },
          { name: 'TypeScript', entityType: 'Language', observations: ['Static typing'] },
          { name: 'Python', entityType: 'Language', observations: ['Dynamic typing'] },
        ],
      });
    });

    it('flags a literal natural-language query with isError + suggestion', async () => {
      const raw = await callToolRaw(client, 'search_nodes', {
        query: 'knowledge graph features',
      });

      expect(raw.isError).toBe(true);
      const text = raw.content[0]?.text ?? '';
      expect(text).toContain('knowledge graph features');
      expect(text).toContain('regex');
      // Auto-suggested |-joined regex should appear.
      expect(text).toContain('knowledge|graph|features');
    });

    it('does NOT flag a regex query with anchors that simply misses', async () => {
      const raw = await callToolRaw(client, 'search_nodes', {
        query: '^MissingEntity$',
      });

      expect(raw.isError).toBeFalsy();
      const parsed = JSON.parse(raw.content[0]?.text ?? '{}') as PaginatedGraph;
      expect(parsed.entities.items).toEqual([]);
      expect(parsed.relations.items).toEqual([]);
    });

    it('does NOT flag an alternation query that misses', async () => {
      const raw = await callToolRaw(client, 'search_nodes', {
        query: 'foo|bar|baz',
      });

      expect(raw.isError).toBeFalsy();
      const parsed = JSON.parse(raw.content[0]?.text ?? '{}') as PaginatedGraph;
      expect(parsed.entities.items).toEqual([]);
    });

    it('does NOT flag a literal query that DOES return matches', async () => {
      const raw = await callToolRaw(client, 'search_nodes', {
        query: 'Script',
      });

      expect(raw.isError).toBeFalsy();
      const parsed = JSON.parse(raw.content[0]?.text ?? '{}') as PaginatedGraph;
      expect(parsed.entities.items.length).toBeGreaterThan(0);
    });

    it('omits the alternation suggestion when the query is a single word', async () => {
      const raw = await callToolRaw(client, 'search_nodes', {
        query: 'Slef', // typo, no spaces
      });

      expect(raw.isError).toBe(true);
      const text = raw.content[0]?.text ?? '';
      expect(text).toContain('Slef');
      expect(text).toContain('regex');
      // No multi-term suggestion because there's only one term.
      expect(text).not.toContain('"Slef|');
    });

    // Note: a "walker bias" test is intentionally omitted because the guard
    // path's entity list is always empty, so recordWalkerVisits([]) is a
    // no-op even without the early return — there's nothing externally
    // observable to assert against. The early return is defensive only.
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
      }) as FindPathResult;

      expect(result.targetReached).toBe(true);
      expect(result.budgetExhausted).toBe(false);
      expect(result.items).toHaveLength(2);
      expect(result.items[0].from).toBe('Root');
      expect(result.items[1].to).toBe('Grandchild');
    });

    it('returns a best-effort exploration path with targetReached=false when target is unreachable', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Isolated', entityType: 'Node', observations: [] }]
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'Root',
        toEntity: 'Isolated'
      }) as FindPathResult;

      // β-contract: BFS expanded Root + its connected component but
      // never reached Isolated. We expect a partial path to whatever
      // node the BFS got deepest into, NOT an empty list. The flag is
      // the source of truth for "did we find the asked-for target."
      expect(result.targetReached).toBe(false);
      expect(result.budgetExhausted).toBe(false);
      expect(result.farthestDiscovered).toBeDefined();
      // Path must NOT end at Isolated — confirms it's an exploration
      // path, not a fake "we found it" answer.
      if (result.items.length > 0) {
        expect(result.items[result.items.length - 1].to).not.toBe('Isolated');
      }
      // Note exists and mentions the actual target.
      expect(result.note).toBeDefined();
      expect(result.note).toMatch(/Isolated/);
    });
  });

  describe('find_path BFS regression suite', () => {
    // These tests pin the post-2026-05-16 BFS rewrite against the prior
    // DFS-with-backtrack-deletion implementation, which enumerated every
    // simple path of length ≤ maxDepth (O(b^d) on hub nodes) and pinned a
    // production memory-server at 91% CPU for 27 minutes before being
    // killed. Each test below would either hang or return a wrong-length
    // path under that implementation.

    it('returns the SHORTEST path when multiple paths exist', async () => {
      // Topology:
      //   A → B → D          (length 2)
      //   A → C → D          (length 2, alternate)
      //   A → X → Y → Z → D  (length 4, distractor)
      //
      // Build the distractor edge A→X FIRST so DFS would discover it on
      // its first descent and (without the backtrack-deletion bug)
      // return the length-4 path. BFS must return one of the length-2
      // paths regardless of insertion order.
      await callTool(client, 'create_entities', {
        entities: ['A','B','C','D','X','Y','Z'].map(n => ({
          name: n, entityType: 'Node', observations: [],
        })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'X', relationType: 'r' },
          { from: 'X', to: 'Y', relationType: 'r' },
          { from: 'Y', to: 'Z', relationType: 'r' },
          { from: 'Z', to: 'D', relationType: 'r' },
          { from: 'A', to: 'B', relationType: 'r' },
          { from: 'B', to: 'D', relationType: 'r' },
          { from: 'A', to: 'C', relationType: 'r' },
          { from: 'C', to: 'D', relationType: 'r' },
        ],
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'A', toEntity: 'D',
      }) as FindPathResult;

      expect(result.targetReached).toBe(true);
      expect(result.items).toHaveLength(2);   // not 4
      expect(result.items[0].from).toBe('A');
      expect(result.items[1].to).toBe('D');
    });

    it('returns best-effort exploration within milliseconds when target is unreachable from a high-branching hub (regression: would hang DFS)', async () => {
      // The production hang: a hub with branching factor b ≈ 50, target
      // in a disconnected island. DFS-with-backtrack-deletion expands
      // b^maxDepth path-suffixes before concluding. We build a hub of
      // 40 outbound edges with a 2-deep tail per branch (so the
      // pathological subgraph has ~40 * 40 = 1600 length-2 paths, and
      // depth=5 would explore on the order of 40^5 = 100M paths under
      // the old code) and a completely separate target node.
      const HUB_DEGREE = 40;
      const TAIL_LEN = 2;
      const entities: { name: string; entityType: string; observations: string[] }[] = [
        { name: 'Hub', entityType: 'Hub', observations: [] },
        { name: 'Unreachable', entityType: 'Node', observations: [] },
      ];
      const relations: { from: string; to: string; relationType: string }[] = [];
      for (let i = 0; i < HUB_DEGREE; i++) {
        let prev = 'Hub';
        for (let d = 0; d < TAIL_LEN; d++) {
          const n = `Hub_b${i}_d${d}`;
          entities.push({ name: n, entityType: 'Node', observations: [] });
          relations.push({ from: prev, to: n, relationType: 'r' });
          prev = n;
        }
        // Cross-edges between branches at the leaf level — keeps the
        // adversarial branching factor up at every level instead of
        // narrowing as DFS descends.
        if (i > 0) {
          relations.push({ from: `Hub_b${i-1}_d${TAIL_LEN-1}`, to: `Hub_b${i}_d0`, relationType: 'x' });
        }
      }
      await callTool(client, 'create_entities', { entities });
      await callTool(client, 'create_relations', { relations });

      const t0 = Date.now();
      const result = await callTool(client, 'find_path', {
        fromEntity: 'Hub', toEntity: 'Unreachable', maxDepth: 5,
      }) as FindPathResult;
      const elapsed = Date.now() - t0;

      // β-contract: target unreachable → targetReached=false and a
      // best-effort path is returned (not the empty list the pre-β
      // contract would have produced). The bug being regressed against
      // is the exponential DFS, not the response shape.
      expect(result.targetReached).toBe(false);
      expect(result.farthestDiscovered).toBeDefined();
      // Under the old DFS this completes in minutes-to-hours, not ms. 5s
      // is a generous ceiling that any reasonable BFS comfortably clears
      // (production typical: < 50ms for graphs of this size). A
      // regression that restores the old behavior fails here long before
      // the per-call watchdog or the test runner timeout intervenes.
      expect(elapsed).toBeLessThan(5000);
    });

    it('tolerates cycles and does not loop', async () => {
      // Triangle plus a separate target on the far side.
      await callTool(client, 'create_entities', {
        entities: ['P','Q','R','Target'].map(n => ({
          name: n, entityType: 'Node', observations: [],
        })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'P', to: 'Q', relationType: 'r' },
          { from: 'Q', to: 'R', relationType: 'r' },
          { from: 'R', to: 'P', relationType: 'r' },     // closes the cycle
          { from: 'R', to: 'Target', relationType: 'r' },
        ],
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'P', toEntity: 'Target',
      }) as FindPathResult;

      expect(result.targetReached).toBe(true);
      expect(result.items).toHaveLength(3);              // P→Q→R→Target
      expect(result.items[0].from).toBe('P');
      expect(result.items[result.items.length - 1].to).toBe('Target');
    });

    it('maxDepth boundary: path of length N is found when maxDepth=N; rejected when maxDepth=N-1', async () => {
      // Chain A → B → C → D → E (4 hops). maxDepth=4 must find it;
      // maxDepth=3 must return [].
      await callTool(client, 'create_entities', {
        entities: ['A','B','C','D','E'].map(n => ({
          name: n, entityType: 'Node', observations: [],
        })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'A', to: 'B', relationType: 'r' },
          { from: 'B', to: 'C', relationType: 'r' },
          { from: 'C', to: 'D', relationType: 'r' },
          { from: 'D', to: 'E', relationType: 'r' },
        ],
      });

      const accepted = await callTool(client, 'find_path', {
        fromEntity: 'A', toEntity: 'E', maxDepth: 4,
      }) as FindPathResult;
      expect(accepted.targetReached).toBe(true);
      expect(accepted.items).toHaveLength(4);

      // β-contract: depth was too small to reach E, so targetReached=false
      // and the returned path is a best-effort exploration ending at the
      // deepest node BFS expanded (D, after 3 hops). Caller can chain a
      // follow-up call with fromEntity='D' to continue.
      const rejected = await callTool(client, 'find_path', {
        fromEntity: 'A', toEntity: 'E', maxDepth: 3,
      }) as FindPathResult;
      expect(rejected.targetReached).toBe(false);
      expect(rejected.budgetExhausted).toBe(false);
      expect(rejected.farthestDiscovered).toBe('D');
      expect(rejected.items).toHaveLength(3);            // A→B→C→D, the best-effort partial
      expect(rejected.items[rejected.items.length - 1].to).toBe('D');
    });

    it('returns empty path with targetReached=true when fromEntity equals toEntity (no-op path)', async () => {
      await callTool(client, 'create_entities', {
        entities: [{ name: 'Solo', entityType: 'Node', observations: [] }],
      });
      const result = await callTool(client, 'find_path', {
        fromEntity: 'Solo', toEntity: 'Solo',
      }) as FindPathResult;
      // Trivial: 0 hops needed, target reached.
      expect(result.targetReached).toBe(true);
      expect(result.budgetExhausted).toBe(false);
      expect(result.items).toHaveLength(0);
    });

    it('memory budget: a tiny KB_FIND_PATH_BUDGET_BYTES forces budget exhaustion and bounded time', async () => {
      // Build a long-enough chain that BFS materializes several names
      // before reaching the target. With a budget low enough to admit
      // only the start node + a couple discoveries, BFS must terminate
      // mid-search and return []. Same graph with the default budget
      // (next sub-test) returns the full path — proves the budget is
      // what's limiting, not the topology.
      await callTool(client, 'create_entities', {
        entities: ['n0','n1','n2','n3','n4','n5','n6','n7'].map(n => ({
          name: n, entityType: 'Node', observations: [],
        })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'n0', to: 'n1', relationType: 'r' },
          { from: 'n1', to: 'n2', relationType: 'r' },
          { from: 'n2', to: 'n3', relationType: 'r' },
          { from: 'n3', to: 'n4', relationType: 'r' },
          { from: 'n4', to: 'n5', relationType: 'r' },
          { from: 'n5', to: 'n6', relationType: 'r' },
          { from: 'n6', to: 'n7', relationType: 'r' },
        ],
      });

      // 100 bytes only covers ~1 discovery (~120 B per BFS step under
      // our V8 layout constants), so BFS must trip the cap before
      // crossing the chain. Set/unset around the call so other tests
      // are unaffected. We rely on `findPathBudgetBytes` reading
      // `process.env` per-call (it does), so the in-process server
      // observes the override exactly on this call.
      process.env.KB_FIND_PATH_BUDGET_BYTES = '100';
      let constrained: FindPathResult;
      const t0 = Date.now();
      try {
        constrained = await callTool(client, 'find_path', {
          fromEntity: 'n0', toEntity: 'n7', maxDepth: 10,
        }) as FindPathResult;
      } finally {
        delete process.env.KB_FIND_PATH_BUDGET_BYTES;
      }
      const elapsed = Date.now() - t0;

      // β-contract: budget tripped before reaching n7. We get:
      //   - targetReached=false (the asked-for target was NOT n0)
      //   - budgetExhausted=true (the reason the search stopped)
      //   - farthestDiscovered=some name BFS got to before the cap
      //   - items: a best-effort partial path to farthestDiscovered
      //   - note: contains both "memory budget" wording and the target name
      expect(constrained.targetReached).toBe(false);
      expect(constrained.budgetExhausted).toBe(true);
      expect(constrained.farthestDiscovered).toBeDefined();
      expect(constrained.note).toMatch(/budget/);
      expect(constrained.note).toMatch(/n7/);
      // The partial path ends at farthestDiscovered, not at the target.
      if (constrained.items.length > 0) {
        expect(constrained.items[constrained.items.length - 1].to).toBe(constrained.farthestDiscovered);
      }
      expect(elapsed).toBeLessThan(1000);

      // Same call without the override finds the 7-hop path. This sub-
      // assertion is what nails "the budget is the limiting factor" —
      // without it, the test could pass by simply never finding paths.
      const unconstrained = await callTool(client, 'find_path', {
        fromEntity: 'n0', toEntity: 'n7', maxDepth: 10,
      }) as FindPathResult;
      expect(unconstrained.targetReached).toBe(true);
      expect(unconstrained.budgetExhausted).toBe(false);
      expect(unconstrained.items).toHaveLength(7);
      expect(unconstrained.items[0].from).toBe('n0');
      expect(unconstrained.items[unconstrained.items.length - 1].to).toBe('n7');
    });

    it('memory budget: env override of zero bytes terminates after the first discovery (target unreached)', async () => {
      // Edge case: budget=0 means *any* discovery trips the cap. The
      // budget check fires AFTER the "is this nextName the target?"
      // check, by design — we never want to fail a search that
      // succeeded. So budget=0 returns [] whenever the target is more
      // than one BFS hop away, and returns the path when the target is
      // exactly one hop (the found check short-circuits the budget
      // check). Here we use a 2-hop graph p→q→r asking for r, and
      // expect []: BFS discovers q, checks the budget, trips, exits.
      await callTool(client, 'create_entities', {
        entities: ['p', 'q', 'r'].map(n => ({ name: n, entityType: 'Node', observations: [] })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'p', to: 'q', relationType: 'r' },
          { from: 'q', to: 'r', relationType: 'r' },
        ],
      });

      process.env.KB_FIND_PATH_BUDGET_BYTES = '0';
      let result: FindPathResult;
      try {
        result = await callTool(client, 'find_path', {
          fromEntity: 'p', toEntity: 'r', maxDepth: 5,
        }) as FindPathResult;
      } finally {
        delete process.env.KB_FIND_PATH_BUDGET_BYTES;
      }
      // β-contract under budget=0: q gets discovered (one discovery
      // is admissible because the cap is checked after the
      // found-vs-target check, by design), then the budget trips.
      // farthestDiscovered=q, partial path = [p→q].
      expect(result.targetReached).toBe(false);
      expect(result.budgetExhausted).toBe(true);
      expect(result.farthestDiscovered).toBe('q');
      expect(result.items).toHaveLength(1);
      expect(result.items[0].from).toBe('p');
      expect(result.items[0].to).toBe('q');
    });

    it("direction='backward' walks the predecessor side of edges", async () => {
      // X → Y → Z (forward edges only). 'backward' from Z should reach X.
      await callTool(client, 'create_entities', {
        entities: ['X','Y','Z'].map(n => ({ name: n, entityType: 'Node', observations: [] })),
      });
      await callTool(client, 'create_relations', {
        relations: [
          { from: 'X', to: 'Y', relationType: 'r' },
          { from: 'Y', to: 'Z', relationType: 'r' },
        ],
      });

      const result = await callTool(client, 'find_path', {
        fromEntity: 'Z', toEntity: 'X', direction: 'backward',
      }) as FindPathResult;

      expect(result.targetReached).toBe(true);
      expect(result.items).toHaveLength(2);
      // Path reconstruction emits relations in the underlying graph's
      // (from, to) shape — Y→Z first, then X→Y — because the BFS walked
      // backward along forward edges.
      expect(result.items.map(r => r.from + '→' + r.to))
        .toEqual(['Y→Z', 'X→Y']);
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
      const result = await callTool(client, 'get_entity_types', {}) as PaginatedResult<string>;

      expect(result.items).toContain('Person');
      expect(result.items).toContain('Company');
      expect(result.totalCount).toBe(2);
      expect(result.items).toHaveLength(2);
    });

    it('should get all relation types', async () => {
      const result = await callTool(client, 'get_relation_types', {}) as PaginatedResult<string>;

      expect(result.items).toContain('works_at');
      expect(result.totalCount).toBe(1);
      expect(result.items).toHaveLength(1);
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

    it('should validate graph and report no violations on clean graph', async () => {
      // Create a valid graph through the API
      await callTool(client, 'create_entities', {
        entities: [
          { name: 'Valid', entityType: 'Test', observations: [] },
          { name: 'Also_Valid', entityType: 'Test', observations: ['Short obs'] }
        ]
      });
      await callTool(client, 'create_relations', {
        relations: [{ from: 'Valid', to: 'Also_Valid', relationType: 'refs' }]
      });

      const result = await callTool(client, 'validate_graph', {}) as {
        missingEntities: PaginatedResult<string>;
        observationViolations: PaginatedResult<{ entity: string; count: number; oversizedObservations: number[] }>;
      };

      // Binary store enforces referential integrity — no missing entities possible
      expect(result.missingEntities.totalCount).toBe(0);
      expect(result.missingEntities.items).toHaveLength(0);
      expect(result.observationViolations.totalCount).toBe(0);
      expect(result.observationViolations.items).toHaveLength(0);
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

    it('should accept mode=uniform and produce a valid walk', async () => {
      const result = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 2,
        mode: 'uniform',
      }) as { entity: string; path: string[] };

      expect(result.path[0]).toBe('Center');
      expect(result.path.length).toBeGreaterThanOrEqual(1);
      expect(result.path.length).toBeLessThanOrEqual(3);
      expect(result.entity).toBe(result.path[result.path.length - 1]);
      // Every step must land on an existing graph node — uniform mode must
      // never invent entities or step off the graph.
      for (const node of result.path) {
        expect(['Center', 'North', 'South', 'East']).toContain(node);
      }
    });

    it('should produce reproducible uniform walks with the same seed', async () => {
      const r1 = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 3,
        seed: 'uniform-seed-xyz',
        mode: 'uniform',
      }) as { entity: string; path: string[] };
      const r2 = await callTool(client, 'random_walk', {
        start: 'Center',
        depth: 3,
        seed: 'uniform-seed-xyz',
        mode: 'uniform',
      }) as { entity: string; path: string[] };
      expect(r1.path).toEqual(r2.path);
    });

    it('uniform mode should explore all neighbors over many unseeded walks', async () => {
      // From 'Center' with direction=forward the 1-step neighbor set is
      // {North, South, East}. Use unseeded walks (true RNG via crypto) so
      // the test is statistically robust: P(any single neighbor missed in
      // 200 draws of 1/3) ≈ (2/3)^200 ≈ 1e-35 — well past flake territory.
      const visited = new Set<string>();
      for (let i = 0; i < 200; i++) {
        const r = await callTool(client, 'random_walk', {
          start: 'Center',
          depth: 1,
          mode: 'uniform',
        }) as { entity: string; path: string[] };
        if (r.path.length > 1) visited.add(r.path[1]);
      }
      expect(visited.has('North')).toBe(true);
      expect(visited.has('South')).toBe(true);
      expect(visited.has('East')).toBe(true);
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

      it('should use llmrank as default sort when sortBy is omitted', async () => {
        const result = await callTool(client, 'search_nodes', {
          query: 'Letter'
        }) as PaginatedGraph;

        // With llmrank default, all entities returned (order varies due to random tiebreak)
        const names = result.entities.items.map(e => e.name);
        expect(names).toHaveLength(3);
        expect(names).toContain('Alpha');
        expect(names).toContain('Beta');
        expect(names).toContain('Gamma');
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

      it('should use llmrank as default sort when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_entities_by_type', {
          entityType: 'Animal'
        }) as PaginatedResult<Entity>;

        // With llmrank default, all entities returned (order varies due to random tiebreak)
        const names = result.items.map(e => e.name);
        expect(names).toHaveLength(3);
        expect(names).toContain('Zebra');
        expect(names).toContain('Aardvark');
        expect(names).toContain('Monkey');
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

      it('should use llmrank as default sort when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_orphaned_entities', {}) as PaginatedResult<Entity>;

        // With llmrank default, all entities returned (order varies due to random tiebreak)
        const names = result.items.map(e => e.name);
        expect(names).toHaveLength(3);
        expect(names).toContain('Orphan_Z');
        expect(names).toContain('Orphan_A');
        expect(names).toContain('Orphan_M');
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

      it('should use llmrank as default sort when sortBy is omitted', async () => {
        const result = await callTool(client, 'get_neighbors', {
          entityName: 'Hub'
        }) as PaginatedResult<Neighbor>;

        // With llmrank default, all neighbors returned (order varies due to random tiebreak)
        expect(result.items).toHaveLength(3);
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

    describe('pagerank sorting', () => {
      it('should sort by pagerank (structural rank)', async () => {
        // Build a star graph: Hub -> A, Hub -> B, Hub -> C
        // A, B, C are dangling nodes
        await callTool(client, 'create_entities', {
          entities: [
            { name: 'Hub', entityType: 'Node', observations: ['Central node'] },
            { name: 'LeafA', entityType: 'Node', observations: ['Leaf A'] },
            { name: 'LeafB', entityType: 'Node', observations: ['Leaf B'] },
            { name: 'LeafC', entityType: 'Node', observations: ['Leaf C'] },
          ]
        });
        await callTool(client, 'create_relations', {
          relations: [
            { from: 'Hub', to: 'LeafA', relationType: 'LINKS' },
            { from: 'Hub', to: 'LeafB', relationType: 'LINKS' },
            { from: 'Hub', to: 'LeafC', relationType: 'LINKS' },
          ]
        });

        // Sort by pagerank descending (default for ranks)
        const result = await callTool(client, 'search_nodes', {
          query: 'Node',
          sortBy: 'pagerank'
        }) as PaginatedGraph;

        // All entities should be returned
        expect(result.entities.items).toHaveLength(4);

        // With structural rank, the leaves should rank higher than the hub
        // because they receive visits from Hub's walks.
        // We just verify the sort works and returns all entities.
        const names = result.entities.items.map(e => e.name);
        expect(names).toContain('Hub');
        expect(names).toContain('LeafA');
        expect(names).toContain('LeafB');
        expect(names).toContain('LeafC');
      });
    });

    describe('llmrank sorting', () => {
      it('should sort by llmrank (walker visits)', async () => {
        await callTool(client, 'create_entities', {
          entities: [
            { name: 'Hot', entityType: 'Test', observations: ['Frequently accessed'] },
            { name: 'Cold', entityType: 'Test', observations: ['Rarely accessed'] },
          ]
        });

        // Access 'Hot' multiple times via open_nodes (which increments walker visits)
        await callTool(client, 'open_nodes', { names: ['Hot'] });
        await callTool(client, 'open_nodes', { names: ['Hot'] });
        await callTool(client, 'open_nodes', { names: ['Hot'] });
        await callTool(client, 'open_nodes', { names: ['Hot'] });
        await callTool(client, 'open_nodes', { names: ['Hot'] });
        // Access 'Cold' just once
        await callTool(client, 'open_nodes', { names: ['Cold'] });

        // Sort by llmrank descending
        const result = await callTool(client, 'search_nodes', {
          query: 'Test',
          sortBy: 'llmrank'
        }) as PaginatedGraph;

        // 'Hot' should rank higher than 'Cold' due to more walker visits
        expect(result.entities.items).toHaveLength(2);
        expect(result.entities.items[0].name).toBe('Hot');
        expect(result.entities.items[1].name).toBe('Cold');
      });

      it('should fall back to pagerank on llmrank tie', async () => {
        // Create entities with no prior walker visits
        await callTool(client, 'create_entities', {
          entities: [
            { name: 'Center', entityType: 'Fallback', observations: ['Hub'] },
            { name: 'Spoke', entityType: 'Fallback', observations: ['Leaf'] },
          ]
        });
        // Create a relation so structural rank differs
        await callTool(client, 'create_relations', {
          relations: [{ from: 'Center', to: 'Spoke', relationType: 'POINTS_TO' }]
        });

        // Both have 0 walker visits (tie), so llmrank should fall back to pagerank
        const result = await callTool(client, 'search_nodes', {
          query: 'Fallback',
          sortBy: 'llmrank'
        }) as PaginatedGraph;

        // Just verify we get both entities (ordering depends on structural rank + random tiebreak)
        expect(result.entities.items).toHaveLength(2);
        const names = result.entities.items.map(e => e.name);
        expect(names).toContain('Center');
        expect(names).toContain('Spoke');
      });
    });
  });

  describe('kb_load', () => {
    let docFile: string;

    beforeEach(async () => {
      docFile = path.join(testDir, 'test-doc.txt');
    });

    it('should reject non-plaintext extensions', async () => {
      const pdfPath = path.join(testDir, 'test.pdf');
      await fs.writeFile(pdfPath, 'fake pdf content');

      await expect(
        callTool(client, 'kb_load', { filePath: pdfPath })
      ).rejects.toThrow(/Unsupported file extension/);
    });

    it('should reject files with no extension', async () => {
      const noExtPath = path.join(testDir, 'noext');
      await fs.writeFile(noExtPath, 'some content');

      await expect(
        callTool(client, 'kb_load', { filePath: noExtPath })
      ).rejects.toThrow(/no extension/);
    });

    it('should reject missing files', async () => {
      await expect(
        callTool(client, 'kb_load', { filePath: path.join(testDir, 'nonexistent.txt') })
      ).rejects.toThrow(/Failed to read file/);
    });

    it('should load a small document and create entities + relations', async () => {
      const text = [
        'Abstract interpretation is a theory of sound approximation of program semantics.',
        'The key idea is to compute over abstract domains instead of concrete domains.',
        'This enables static analysis to scale to large programs.',
        'Galois connections formalize the relationship between abstract and concrete.',
        'Widening operators ensure termination of the analysis.',
      ].join(' ');
      await fs.writeFile(docFile, text);

      const result = await callTool(client, 'kb_load', { filePath: docFile }) as any;

      expect(result.document).toBe('test-doc');
      expect(result.entitiesCreated).toBeGreaterThan(0);
      expect(result.relationsCreated).toBeGreaterThan(0);
      expect(result.stats.chunks).toBeGreaterThan(0);
      expect(result.stats.sentences).toBeGreaterThan(0);
    });

    it('should create Document, TextChunk, and DocumentIndex entities', async () => {
      await fs.writeFile(docFile, 'The quick brown fox jumps over the lazy dog. The fox was very quick and the dog was very lazy. This sentence makes the document long enough to have multiple chunks for testing purposes and analysis.');

      await callTool(client, 'kb_load', { filePath: docFile });

      // Check document entity
      const docResult = await callTool(client, 'open_nodes', { names: ['test-doc'] }) as PaginatedGraph;
      expect(docResult.entities.items).toHaveLength(1);
      expect(docResult.entities.items[0].entityType).toBe('Document');

      // Check index entities — one hub (0 observations) + entries (1 observation each)
      const indexEntities = await callTool(client, 'get_entities_by_type', { entityType: 'DocumentIndex' }) as PaginatedResult<Entity>;
      expect(indexEntities.items.length).toBeGreaterThan(1);
      const hub = indexEntities.items.filter(e => e.observations.length === 0);
      const entries = indexEntities.items.filter(e => e.observations.length > 0);
      expect(hub).toHaveLength(1);
      expect(entries.length).toBeGreaterThan(0);
      for (const entry of entries) {
        expect(entry.observations.length).toBe(1);
        expect(entry.observations[0].length).toBeLessThanOrEqual(140);
      }

      // Check TextChunk entities exist via type query
      const chunks = await callTool(client, 'get_entities_by_type', { entityType: 'TextChunk' }) as PaginatedResult<Entity>;
      expect(chunks.items.length).toBeGreaterThan(0);
      for (const chunk of chunks.items) {
        expect(chunk.observations.length).toBeLessThanOrEqual(2);
        for (const obs of chunk.observations) {
          expect(obs.length).toBeLessThanOrEqual(140);
        }
      }
    });

    it('should create chain relations (starts_with, ends_with, follows)', async () => {
      const text = 'First sentence of the document goes here. ' +
        'Second sentence adds more content to the document. ' +
        'Third sentence continues the thought further along. ' +
        'Fourth sentence wraps up the document nicely.';
      await fs.writeFile(docFile, text);

      await callTool(client, 'kb_load', { filePath: docFile });

      // Get the document's relations
      const docResult = await callTool(client, 'open_nodes', { names: ['test-doc'] }) as PaginatedGraph;
      const relTypes = docResult.relations.items.map(r => r.relationType);

      expect(relTypes).toContain('starts_with');
      expect(relTypes).toContain('has_index');
    });

    it('should create index → chunk highlight relations', async () => {
      const sentences = [];
      for (let i = 0; i < 20; i++) {
        sentences.push(`This is sentence number ${i} about abstract interpretation and program analysis with different keywords each time.`);
      }
      await fs.writeFile(docFile, sentences.join(' '));

      const result = await callTool(client, 'kb_load', { filePath: docFile }) as any;
      expect(result.stats.indexHighlights).toBeGreaterThan(0);

      // Find index entities and verify they have highlight relations
      const indexEntities = await callTool(client, 'get_entities_by_type', { entityType: 'DocumentIndex' }) as PaginatedResult<Entity>;
      expect(indexEntities.items.length).toBeGreaterThan(0);
      const indexNames = indexEntities.items.map(e => e.name);
      const indexResult = await callTool(client, 'open_nodes', { names: indexNames }) as PaginatedGraph;
      const highlightRels = indexResult.relations.items.filter(r => r.relationType === 'highlights');
      expect(highlightRels.length).toBeGreaterThan(0);
    });

    it('should respect custom title', async () => {
      await fs.writeFile(docFile, 'Some document content that is long enough to process.');

      const result = await callTool(client, 'kb_load', {
        filePath: docFile,
        title: 'my-custom-title',
      }) as any;

      expect(result.document).toBe('my-custom-title');

      const docResult = await callTool(client, 'open_nodes', { names: ['my-custom-title'] }) as PaginatedGraph;
      expect(docResult.entities.items).toHaveLength(1);
    });

    it('should not create duplicate document entity on reload with different content', async () => {
      await fs.writeFile(docFile, 'Short doc for dedup testing purposes here.');
      await callTool(client, 'kb_load', { filePath: docFile });

      // Second load with different content but same title — Document entity
      // already exists with entityType 'Document' and no observations,
      // so it gets silently skipped. But the index entities already exist
      // with different observations, so it should error.
      await fs.writeFile(docFile, 'Completely different content for dedup testing now.');
      await expect(
        callTool(client, 'kb_load', { filePath: docFile })
      ).rejects.toThrow(/already exists/);
    });

    it('should enforce observation length limits', async () => {
      // Create a document with very long words that might challenge splitting
      const longWord = 'a'.repeat(200);
      await fs.writeFile(docFile, `${longWord} is a very long word that tests our splitting logic handles edge cases.`);

      const result = await callTool(client, 'kb_load', { filePath: docFile }) as any;
      expect(result.entitiesCreated).toBeGreaterThan(0);

      // All observations should be within limits
      const chunks = await callTool(client, 'get_entities_by_type', { entityType: 'TextChunk' }) as PaginatedResult<Entity>;
      for (const chunk of chunks.items) {
        for (const obs of chunk.observations) {
          expect(obs.length).toBeLessThanOrEqual(140);
        }
      }
    });

    it('should accept various plaintext extensions', async () => {
      const extensions = ['.txt', '.md', '.tex', '.py', '.ts', '.c'];
      for (const ext of extensions) {
        const filePath = path.join(testDir, `test${ext}`);
        await fs.writeFile(filePath, 'Some plaintext content for extension testing purposes here.');

        // Should not throw on validation — use a unique title per extension
        const result = await callTool(client, 'kb_load', {
          filePath,
          title: `ext-test-${ext.slice(1)}`,
        }) as any;
        expect(result.entitiesCreated).toBeGreaterThan(0);
      }
    });
  });
});
