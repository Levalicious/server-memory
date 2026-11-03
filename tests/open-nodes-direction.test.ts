import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, type Relation } from '../server.js';
import { createTestClient, callTool, type PaginatedGraph } from './test-utils.js';

// Regression: open_nodes direction 'any' kept an in-edge only when its source
// was ALSO being opened — the inverse of the right filter. Result: 'any'
// returned out-edges only (in-edges from outside the opened set vanished), and
// an edge between two opened nodes was listed twice (once as the source's
// out-edge, once as the target's in-edge).
describe("open_nodes direction semantics", () => {
  let testDir: string;
  let client: Awaited<ReturnType<typeof createTestClient>>['client'];
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mcp-memory-opendir-'));
    const server = createServer(path.join(testDir, 'test-memory.json'));
    const result = await createTestClient(server);
    client = result.client;
    cleanup = result.cleanup;

    // A -> B, C -> A, B -> C, D -> B, A -> A (self-loop)
    await callTool(client, 'create_entities', {
      entities: ['A', 'B', 'C', 'D'].map(name => ({ name, entityType: 'Test', observations: [] })),
    });
    await callTool(client, 'create_relations', {
      relations: [
        { from: 'A', to: 'B', relationType: 'R' },
        { from: 'C', to: 'A', relationType: 'R' },
        { from: 'B', to: 'C', relationType: 'R' },
        { from: 'D', to: 'B', relationType: 'R' },
        { from: 'A', to: 'A', relationType: 'SELF' },
      ],
    });
  });

  afterEach(async () => {
    await cleanup();
    try {
      await fs.rm(testDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  async function open(names: string[], direction: string): Promise<string[]> {
    const page = await callTool(client, 'open_nodes', { names, direction }) as PaginatedGraph;
    expect(page.relations.nextCursor).toBeNull();
    return page.relations.items.map((r: Relation) => `${r.from}-${r.relationType}->${r.to}`).sort();
  }

  it('single node: any = forward ∪ backward, self-loop once', async () => {
    expect(await open(['A'], 'forward')).toEqual(['A-R->B', 'A-SELF->A']);
    expect(await open(['A'], 'backward')).toEqual(['A-SELF->A', 'C-R->A']);
    expect(await open(['A'], 'any')).toEqual(['A-R->B', 'A-SELF->A', 'C-R->A']);
  });

  it('includes in-edges from nodes outside the opened set', async () => {
    // D -> B: D is not opened. Before the fix 'any' dropped it.
    expect(await open(['B'], 'any')).toEqual(['A-R->B', 'B-R->C', 'D-R->B']);
  });

  it('lists an edge between two opened nodes exactly once', async () => {
    // A -> B has both endpoints opened. Before the fix it appeared twice.
    expect(await open(['A', 'B'], 'any')).toEqual([
      'A-R->B', 'A-SELF->A', 'B-R->C', 'C-R->A', 'D-R->B',
    ]);
  });
});
