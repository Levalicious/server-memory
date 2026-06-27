import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { mkdtempSync, rmSync, existsSync, copyFileSync } from 'fs';
import { tmpdir } from 'os';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { Store } from '../src/store.js';
import { migrate, detectGraphFormat } from '../src/migrate.js';
import { KnowledgeGraphManager } from '../server.js';

// Committed v2 fixture (generated once via the old addon). It is read by the
// pure-TS reader at runtime, so these tests need no native old addon — which is
// the whole point: CI/deploy only build graphstore.node.
//   Alice(Person, obs ['likes tea'], walkerVisits=2, psi=0.42)
//     -KNOWS-> Bob(Person),  -WORKS_AT-> Acme(Org);  Bob structuralVisits=1.
const FIXTURE_DIR = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');
function placeOldV2(graphPath: string, strPath: string): void {
  copyFileSync(join(FIXTURE_DIR, 'old-v2.graph'), graphPath);
  copyFileSync(join(FIXTURE_DIR, 'old-v2.strings'), strPath);
}

describe('v3 migrator (v1/v2 -> v3 logical rebuild)', () => {
  let dir: string;
  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'mig-')); });
  afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

  it('rebuilds an old v2 KB into v3, preserving fields exactly', () => {
    placeOldV2(join(dir, 'old.graph'), join(dir, 'old.strings'));

    const report = migrate(join(dir, 'old.json'), join(dir, 'new.json'));
    expect(report.mismatches).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.entities).toBe(3);
    expect(report.relations).toBe(2);

    const s = new Store(join(dir, 'new.graph'), join(dir, 'new.strings'));
    const a = s.readEntity(s.lookup('Alice'));
    expect(a.type).toBe('Person');
    expect(a.observations).toEqual(['likes tea']);
    expect(a.walkerVisits).toBe(2n);
    expect(a.psi).toBeCloseTo(0.42, 10);
    const b = s.readEntity(s.lookup('Bob'));
    expect(b.obsMtime).toBe(0n);             // no-obs entity keeps obsMtime 0
    expect(b.structuralVisits).toBe(1n);
    expect(s.walkerTotal()).toBe(2n);
    s.close();
  });

  it('KnowledgeGraphManager auto-migrates an old KB on open (in place, backup kept)', async () => {
    placeOldV2(join(dir, 'mem.graph'), join(dir, 'mem.strings'));
    expect(detectGraphFormat(join(dir, 'mem.graph'))).toBe('old');

    // constructing the manager triggers the in-place auto-migration
    const mgr = new KnowledgeGraphManager(join(dir, 'mem.json'));
    const graph = await mgr.openNodes(['Alice'], 'forward');
    expect(graph.entities[0]?.name).toBe('Alice');
    expect(graph.entities[0]?.observations).toEqual(['likes tea']);
    expect(graph.relations.some(r => r.from === 'Alice' && r.to === 'Bob' && r.relationType === 'KNOWS')).toBe(true);
    mgr.close();

    expect(detectGraphFormat(join(dir, 'mem.graph'))).toBe('v3');      // converted in place
    expect(existsSync(join(dir, 'mem.graph.premigrate'))).toBe(true);  // backup retained
  });
});
