import { mkdtempSync, rmSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { GraphFile, DIR_FORWARD, DIR_BACKWARD } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { Store } from '../src/store.js';
import { migrate, detectGraphFormat } from '../src/migrate.js';
import { KnowledgeGraphManager } from '../server.js';

describe('v3 migrator (v1/v2 -> v3 logical rebuild)', () => {
  let dir: string;
  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'mig-')); });
  afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

  it('rebuilds an old v2 KB into v3, preserving fields exactly', () => {
    const now = 1700000000000n;

    // --- synthesize an old-format KB (a copy / scratch, never the live one) ---
    const st = new StringTable(join(dir, 'old.strings'));
    const gf = new GraphFile(join(dir, 'old.graph'), st);
    gf.lockExclusive();
    const alice = gf.createEntity('Alice', 'Person', now, now);
    gf.addObservation(alice.offset, 'likes tea', now + 5n);
    const bob = gf.createEntity('Bob', 'Person', now + 1n, 0n);    // no observations
    const acme = gf.createEntity('Acme', 'Org', now + 2n, 0n);

    const addRel = (from: bigint, to: bigint, rt: string, mtime: bigint) => {
      const idF = Number(st.intern(rt));
      gf.addEdge(from, { targetOffset: to, direction: DIR_FORWARD, relTypeId: idF, mtime });
      const idB = Number(st.intern(rt));
      gf.addEdge(to, { targetOffset: from, direction: DIR_BACKWARD, relTypeId: idB, mtime });
    };
    addRel(alice.offset, bob.offset, 'KNOWS', now + 10n);
    addRel(alice.offset, acme.offset, 'WORKS_AT', now + 11n);

    gf.incrementWalkerVisit(alice.offset);
    gf.incrementWalkerVisit(alice.offset);
    gf.incrementStructuralVisit(bob.offset);
    gf.setPsi(alice.offset, 0.42);
    gf.unlock();
    gf.sync(); st.sync();
    gf.close(); st.close();

    // --- migrate ---
    const report = migrate(join(dir, 'old.json'), join(dir, 'new.json'));
    expect(report.mismatches).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.entities).toBe(3);
    expect(report.relations).toBe(2);

    // --- independent spot-check of the v3 result ---
    const s = new Store(join(dir, 'new.graph'), join(dir, 'new.strings'));
    const a = s.readEntity(s.lookup('Alice'));
    expect(a.type).toBe('Person');
    expect(a.observations).toEqual(['likes tea']);
    expect(a.walkerVisits).toBe(2n);
    expect(a.psi).toBeCloseTo(0.42, 10);
    const b = s.readEntity(s.lookup('Bob'));
    expect(b.obsMtime).toBe(0n);          // no-obs entity keeps obsMtime 0
    expect(b.structuralVisits).toBe(1n);
    expect(s.walkerTotal()).toBe(2n);
    s.close();
  });

  it('KnowledgeGraphManager auto-migrates an old KB on open (in place, backup kept)', async () => {
    const now = 1700000000000n;

    // old-format KB at the manager's derived paths
    const st = new StringTable(join(dir, 'mem.strings'));
    const gf = new GraphFile(join(dir, 'mem.graph'), st);
    gf.lockExclusive();
    const a = gf.createEntity('Alice', 'Person', now, now);
    gf.addObservation(a.offset, 'likes tea', now + 5n);
    const bob = gf.createEntity('Bob', 'Person', now + 1n, 0n);
    const idF = Number(st.intern('KNOWS'));
    gf.addEdge(a.offset, { targetOffset: bob.offset, direction: DIR_FORWARD, relTypeId: idF, mtime: now + 10n });
    const idB = Number(st.intern('KNOWS'));
    gf.addEdge(bob.offset, { targetOffset: a.offset, direction: DIR_BACKWARD, relTypeId: idB, mtime: now + 10n });
    gf.unlock(); gf.sync(); st.sync(); gf.close(); st.close();

    expect(detectGraphFormat(join(dir, 'mem.graph'))).toBe('old');

    // constructing the manager triggers the in-place auto-migration
    const mgr = new KnowledgeGraphManager(join(dir, 'mem.json'));
    const graph = await mgr.openNodes(['Alice'], 'forward');
    expect(graph.entities[0]?.name).toBe('Alice');
    expect(graph.entities[0]?.observations).toEqual(['likes tea']);
    expect(graph.relations.some(r => r.from === 'Alice' && r.to === 'Bob' && r.relationType === 'KNOWS')).toBe(true);
    mgr.close();

    expect(detectGraphFormat(join(dir, 'mem.graph'))).toBe('v3');     // converted in place
    expect(existsSync(join(dir, 'mem.graph.premigrate'))).toBe(true); // backup retained
  });
});
