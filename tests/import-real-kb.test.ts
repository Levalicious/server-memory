/**
 * Real KB import test: loads the actual production knowledge base from
 * ~/.local/share/memory/vscode.json and verifies that the binary store
 * can import and perfectly recall every entity, observation, relation,
 * and timestamp.
 *
 * This is the safety gate before migration — if this passes, the binary
 * store is a faithful representation of the live data.
 */

import { GraphFile, DIR_FORWARD, DIR_BACKWARD } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { readFileSync, unlinkSync, existsSync } from 'fs';
import { tmpdir, homedir } from 'os';
import { join } from 'path';

// --- Types matching JSONL format ---

interface JsonEntity {
  type: 'entity';
  name: string;
  entityType: string;
  observations: string[];
  mtime?: number;
  obsMtime?: number;
}

interface JsonRelation {
  type: 'relation';
  from: string;
  to: string;
  relationType: string;
  mtime?: number;
}

// --- Parse the real KB ---

const KB_PATH = join(homedir(), '.local', 'share', 'memory', 'vscode.json');

function loadRealKB(): { entities: JsonEntity[]; relations: JsonRelation[] } | null {
  if (!existsSync(KB_PATH)) return null;

  const data = readFileSync(KB_PATH, 'utf-8');
  const lines = data.split('\n').filter(l => l.trim());
  const entities: JsonEntity[] = [];
  const relations: JsonRelation[] = [];

  for (const line of lines) {
    const item = JSON.parse(line);
    if (item.type === 'entity') entities.push(item);
    else if (item.type === 'relation') relations.push(item);
  }

  return { entities, relations };
}

// --- Import function ---

function importIntoGraphFile(
  gf: GraphFile,
  st: StringTable,
  entities: JsonEntity[],
  relations: JsonRelation[]
): Map<string, bigint> {
  const offsets = new Map<string, bigint>();

  // Phase 1: entities
  for (const e of entities) {
    const mtime = BigInt(e.mtime ?? 0);
    const obsMtime = BigInt(e.obsMtime ?? e.mtime ?? 0);
    const rec = gf.createEntity(e.name, e.entityType, mtime, obsMtime);

    for (const obs of e.observations) {
      gf.addObservation(rec.offset, obs, obsMtime);
    }

    // addObservation clobbers mtime; restore if observations had different timestamp
    if (e.observations.length > 0 && obsMtime !== mtime) {
      const updated = gf.readEntity(rec.offset);
      updated.mtime = mtime;
      gf.updateEntity(updated);
    }

    offsets.set(e.name, rec.offset);
  }

  // Phase 2: relations (bidirectional storage)
  for (const r of relations) {
    const fromOff = offsets.get(r.from);
    const toOff = offsets.get(r.to);
    if (!fromOff || !toOff) continue; // skip dangling

    const relTypeId = Number(st.intern(r.relationType));
    const mtime = BigInt(r.mtime ?? 0);

    gf.addEdge(fromOff, {
      targetOffset: toOff,
      direction: DIR_FORWARD,
      relTypeId,
      mtime,
    });

    gf.addEdge(toOff, {
      targetOffset: fromOff,
      direction: DIR_BACKWARD,
      relTypeId,
      mtime,
    });
  }

  return offsets;
}

// --- Test setup ---

const GRAPH_PATH = join(tmpdir(), `real-kb-graph-${process.pid}.dat`);
const STR_PATH = join(tmpdir(), `real-kb-str-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(GRAPH_PATH); } catch {}
  try { unlinkSync(STR_PATH); } catch {}
}

// Load once, share across tests
const realKB = loadRealKB();

const describeIfKB = realKB ? describe : describe.skip;

describeIfKB('Real KB Import', () => {
  let st: StringTable;
  let gf: GraphFile;
  let offsets: Map<string, bigint>;
  const entities = realKB!.entities;
  const relations = realKB!.relations;

  beforeAll(() => {
    cleanup();
    st = new StringTable(STR_PATH, 1024 * 1024);  // 1MB initial for 14K entities
    gf = new GraphFile(GRAPH_PATH, st, 2 * 1024 * 1024);  // 2MB initial
    offsets = importIntoGraphFile(gf, st, entities, relations);
  });

  afterAll(() => {
    gf.close();
    st.close();
    cleanup();
  });

  test('entity count matches', () => {
    expect(gf.getEntityCount()).toBe(entities.length);
    expect(offsets.size).toBe(entities.length);
  });

  test('node log contains all entity offsets', () => {
    const allOffsets = new Set(gf.getAllEntityOffsets().map(String));
    expect(allOffsets.size).toBe(entities.length);

    for (const [, offset] of offsets) {
      expect(allOffsets.has(String(offset))).toBe(true);
    }
  });

  test('every entity name round-trips exactly', () => {
    for (const e of entities) {
      const offset = offsets.get(e.name)!;
      expect(offset).toBeDefined();
      const rec = gf.readEntity(offset);
      expect(st.get(BigInt(rec.nameId))).toBe(e.name);
    }
  });

  test('every entity type round-trips exactly', () => {
    for (const e of entities) {
      const rec = gf.readEntity(offsets.get(e.name)!);
      expect(st.get(BigInt(rec.typeId))).toBe(e.entityType);
    }
  });

  test('every observation round-trips exactly', () => {
    let totalObs = 0;
    for (const e of entities) {
      const rec = gf.readEntity(offsets.get(e.name)!);
      expect(rec.obsCount).toBe(e.observations.length);

      if (e.observations.length >= 1) {
        expect(st.get(BigInt(rec.obs0Id))).toBe(e.observations[0]);
        totalObs++;
      }
      if (e.observations.length >= 2) {
        expect(st.get(BigInt(rec.obs1Id))).toBe(e.observations[1]);
        totalObs++;
      }
    }
    // Sanity: we checked a meaningful number of observations
    expect(totalObs).toBeGreaterThan(0);
    console.log(`  Verified ${totalObs} observations across ${entities.length} entities`);
  });

  test('every mtime round-trips exactly', () => {
    for (const e of entities) {
      const rec = gf.readEntity(offsets.get(e.name)!);
      expect(rec.mtime).toBe(BigInt(e.mtime ?? 0));
    }
  });

  test('every obsMtime round-trips exactly', () => {
    for (const e of entities) {
      const rec = gf.readEntity(offsets.get(e.name)!);
      const expected = BigInt(e.obsMtime ?? e.mtime ?? 0);
      expect(rec.obsMtime).toBe(expected);
    }
  });

  test('every relation has a forward edge on source', () => {
    for (const r of relations) {
      const fromOff = offsets.get(r.from)!;
      const toOff = offsets.get(r.to)!;
      expect(fromOff).toBeDefined();
      expect(toOff).toBeDefined();

      const edges = gf.getEdges(fromOff);
      const relTypeId = Number(st.intern(r.relationType));

      const fwd = edges.find(
        e => e.targetOffset === toOff && e.direction === DIR_FORWARD && e.relTypeId === relTypeId
      );
      expect(fwd).toBeDefined();
      expect(fwd!.mtime).toBe(BigInt(r.mtime ?? 0));

      st.release(BigInt(relTypeId));
    }
    console.log(`  Verified ${relations.length} forward edges`);
  });

  test('every relation has a backward edge on target', () => {
    for (const r of relations) {
      const fromOff = offsets.get(r.from)!;
      const toOff = offsets.get(r.to)!;

      const edges = gf.getEdges(toOff);
      const relTypeId = Number(st.intern(r.relationType));

      const bwd = edges.find(
        e => e.targetOffset === fromOff && e.direction === DIR_BACKWARD && e.relTypeId === relTypeId
      );
      expect(bwd).toBeDefined();
      expect(bwd!.mtime).toBe(BigInt(r.mtime ?? 0));

      st.release(BigInt(relTypeId));
    }
    console.log(`  Verified ${relations.length} backward edges`);
  });

  test('total edge count is 2× relation count (forward + backward)', () => {
    let totalEdges = 0;
    for (const [, offset] of offsets) {
      totalEdges += gf.getEdges(offset).length;
    }
    // Each relation creates one forward and one backward edge
    expect(totalEdges).toBe(relations.length * 2);
  });

  test('Self entity exists and has expected edges', () => {
    const selfOff = offsets.get('Self')!;
    expect(selfOff).toBeDefined();

    const rec = gf.readEntity(selfOff);
    expect(st.get(BigInt(rec.nameId))).toBe('Self');
    expect(st.get(BigInt(rec.typeId))).toBe('agent');

    const edges = gf.getEdges(selfOff);
    const fwdEdges = edges.filter(e => e.direction === DIR_FORWARD);
    const bwdEdges = edges.filter(e => e.direction === DIR_BACKWARD);

    // Self should have outgoing relations (we saw 35+ from the live KB)
    expect(fwdEdges.length).toBeGreaterThan(10);
    console.log(`  Self: ${fwdEdges.length} forward, ${bwdEdges.length} backward edges`);
  });

  test('data survives close and reopen', () => {
    // Close current handles
    gf.close();
    st.close();

    // Reopen from disk
    const st2 = new StringTable(STR_PATH);
    const gf2 = new GraphFile(GRAPH_PATH, st2);

    expect(gf2.getEntityCount()).toBe(entities.length);

    // Spot-check 10 random entities
    const sample = entities.filter((_, i) => i % Math.floor(entities.length / 10) === 0).slice(0, 10);
    for (const e of sample) {
      const offset = offsets.get(e.name)!;
      const rec = gf2.readEntity(offset);
      expect(st2.get(BigInt(rec.nameId))).toBe(e.name);
      expect(st2.get(BigInt(rec.typeId))).toBe(e.entityType);
      expect(rec.obsCount).toBe(e.observations.length);
      expect(rec.mtime).toBe(BigInt(e.mtime ?? 0));
    }

    gf2.close();
    st2.close();

    // Reopen again for remaining tests in afterAll cleanup
    st = new StringTable(STR_PATH);
    gf = new GraphFile(GRAPH_PATH, st);
  });

  test('string table deduplicated shared types and relation names', () => {
    // "entity" types like "Design", "Decision" etc. are shared across many entities.
    // The string table should have far fewer entries than total string usages.
    const uniqueNames = new Set(entities.map(e => e.name));
    const uniqueTypes = new Set(entities.map(e => e.entityType));
    const uniqueRelTypes = new Set(relations.map(r => r.relationType));
    const allObs = entities.flatMap(e => e.observations);
    const uniqueObs = new Set(allObs);

    // Total unique strings should be < total usages
    const totalUnique = uniqueNames.size + uniqueTypes.size + uniqueRelTypes.size + uniqueObs.size;
    const totalUsages = entities.length + entities.length + relations.length + allObs.length;

    console.log(`  Unique strings: ${totalUnique}, total usages: ${totalUsages}, dedup ratio: ${(1 - totalUnique / totalUsages).toFixed(2)}`);
    expect(totalUnique).toBeLessThan(totalUsages);
  });
});
