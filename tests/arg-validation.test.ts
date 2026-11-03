import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { createServer, type Relation } from '../server.js';
import { createTestClient, callTool, callToolRaw, parseToolError, type PaginatedGraph } from './test-utils.js';

interface ArgViolation {
  argument: string;
  code: string;
  message: string;
  allowed: unknown[];
}

// Policy R4 applied to arguments: undeclared keys and off-enum values are
// rejected, never dropped. Regression: open_nodes called with `cursor` (the
// get_neighbors key) instead of `relationCursor` silently re-served page 1
// with an identical nextCursor, making every edge past page 1 unreachable.
describe('Tool argument validation (R4: no silently dropped arguments)', () => {
  let testDir: string;
  let client: Awaited<ReturnType<typeof createTestClient>>['client'];
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mcp-memory-argval-'));
    const server = createServer(path.join(testDir, 'test-memory.json'));
    const result = await createTestClient(server);
    client = result.client;
    cleanup = result.cleanup;
  });

  afterEach(async () => {
    await cleanup();
    try {
      await fs.rm(testDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  async function seedHub(): Promise<void> {
    await callTool(client, 'create_entities', {
      entities: [{ name: 'Hub', entityType: 'Test', observations: [] }],
    });
  }

  it('rejects open_nodes `cursor` and names the cursor keys it does take', async () => {
    await seedHub();
    const raw = await callToolRaw(client, 'open_nodes', {
      names: ['Hub'], direction: 'backward', cursor: '31:950827ed',
    });
    expect(raw.isError).toBe(true);
    const err = parseToolError(raw);
    expect(err.code).toBe('VALIDATION_FAILED');
    const details = err.details as ArgViolation[];
    expect(details).toHaveLength(1);
    expect(details[0].argument).toBe('cursor');
    expect(details[0].message).toContain('entityCursor');
    expect(details[0].message).toContain('relationCursor');
  });

  it('rejects get_neighbors `relationCursor` and names `cursor`', async () => {
    await seedHub();
    const raw = await callToolRaw(client, 'get_neighbors', { entityName: 'Hub', relationCursor: '0:00000000' });
    const err = parseToolError(raw);
    expect(err.code).toBe('VALIDATION_FAILED');
    const details = err.details as ArgViolation[];
    expect(details[0].argument).toBe('relationCursor');
    expect(details[0].message).toMatch(/cursor parameter\(s\): cursor\b/);
  });

  it('rejects an off-enum direction instead of running it as something else', async () => {
    await seedHub();
    const raw = await callToolRaw(client, 'open_nodes', { names: ['Hub'], direction: 'both' });
    const err = parseToolError(raw);
    expect(err.code).toBe('VALIDATION_FAILED');
    const details = err.details as ArgViolation[];
    expect(details).toHaveLength(1);
    expect(details[0].argument).toBe('direction');
    expect(details[0].allowed).toEqual(['forward', 'backward', 'any']);
  });

  it('reports every offending argument in one error (validate-first)', async () => {
    await seedHub();
    const raw = await callToolRaw(client, 'open_nodes', {
      names: ['Hub'], direction: 'both', cursor: '31:950827ed',
    });
    const details = parseToolError(raw).details as ArgViolation[];
    expect(details.map(d => d.argument).sort()).toEqual(['cursor', 'direction']);
  });

  it('a write with an undeclared argument changes nothing', async () => {
    const raw = await callToolRaw(client, 'create_entities', {
      entities: [{ name: 'NeverWritten', entityType: 'Test', observations: [] }],
      dryRun: true,
    });
    expect(parseToolError(raw).code).toBe('VALIDATION_FAILED');
    const open = await callToolRaw(client, 'open_nodes', { names: ['NeverWritten'] });
    expect(parseToolError(open).code).toBe('ENTITY_NOT_FOUND');
  });

  it('drains a multi-page backward relation list via relationCursor', async () => {
    await seedHub();
    const sources = Array.from({ length: 120 }, (_, i) => `Src${String(i).padStart(3, '0')}`);
    await callTool(client, 'create_entities', {
      entities: sources.map(name => ({ name, entityType: 'Test', observations: [] })),
    });
    await callTool(client, 'create_relations', {
      relations: sources.map(from => ({ from, to: 'Hub', relationType: 'POINTS_AT' })),
    });

    const seen: Relation[] = [];
    let relationCursor: string | number | null = 0;
    let pages = 0;
    while (relationCursor !== null && pages < 50) {
      const page = await callTool(client, 'open_nodes', {
        names: ['Hub'], direction: 'backward', relationCursor,
      }) as PaginatedGraph;
      seen.push(...page.relations.items);
      relationCursor = page.relations.nextCursor;
      pages++;
    }

    expect(pages).toBeGreaterThan(1);
    expect(new Set(seen.map(r => r.from)).size).toBe(120);
    expect(seen).toHaveLength(120);
  });
});
