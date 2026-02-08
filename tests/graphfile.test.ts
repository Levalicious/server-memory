import { GraphFile, DIR_FORWARD, DIR_BACKWARD, readEntityRecord } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { unlinkSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

const GRAPH_PATH = join(tmpdir(), `graph-test-${process.pid}.dat`);
const STR_PATH = join(tmpdir(), `strtab-test-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(GRAPH_PATH); } catch {}
  try { unlinkSync(STR_PATH); } catch {}
}

describe('GraphFile', () => {
  afterEach(cleanup);

  function makeGF() {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    return { st, gf };
  }

  test('create and read entity', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const rec = gf.createEntity('TestNode', 'Person', now);
    expect(rec.offset).not.toBe(0n);

    const read = gf.readEntity(rec.offset);
    expect(st.get(BigInt(read.nameId))).toBe('TestNode');
    expect(st.get(BigInt(read.typeId))).toBe('Person');
    expect(read.mtime).toBe(now);
    expect(read.obsMtime).toBe(now);
    expect(read.obsCount).toBe(0);
    expect(read.adjOffset).toBe(0n);

    gf.close();
    st.close();
  });

  test('node log tracks entities', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const a = gf.createEntity('A', 'Type', now);
    const b = gf.createEntity('B', 'Type', now);
    const c = gf.createEntity('C', 'Type', now);

    expect(gf.getEntityCount()).toBe(3);
    const offsets = gf.getAllEntityOffsets();
    expect(offsets).toContain(a.offset);
    expect(offsets).toContain(b.offset);
    expect(offsets).toContain(c.offset);

    gf.close();
    st.close();
  });

  test('delete entity removes from log and frees strings', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const rec = gf.createEntity('Ephemeral', 'Temp', now);
    expect(gf.getEntityCount()).toBe(1);

    gf.deleteEntity(rec.offset);
    expect(gf.getEntityCount()).toBe(0);
    expect(gf.getAllEntityOffsets()).toEqual([]);

    // String refs should be released (refcount 0)
    expect(st.count).toBe(0);

    gf.close();
    st.close();
  });

  test('add and read observations', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const rec = gf.createEntity('Node', 'Type', now);
    gf.addObservation(rec.offset, 'First observation', now + 1n);
    gf.addObservation(rec.offset, 'Second observation', now + 2n);

    const read = gf.readEntity(rec.offset);
    expect(read.obsCount).toBe(2);
    expect(st.get(BigInt(read.obs0Id))).toBe('First observation');
    expect(st.get(BigInt(read.obs1Id))).toBe('Second observation');
    expect(read.obsMtime).toBe(now + 2n);

    gf.close();
    st.close();
  });

  test('remove observation', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const rec = gf.createEntity('Node', 'Type', now);
    gf.addObservation(rec.offset, 'Keep this', now + 1n);
    gf.addObservation(rec.offset, 'Remove this', now + 2n);

    const removed = gf.removeObservation(rec.offset, 'Remove this', now + 3n);
    expect(removed).toBe(true);

    const read = gf.readEntity(rec.offset);
    expect(read.obsCount).toBe(1);
    expect(st.get(BigInt(read.obs0Id))).toBe('Keep this');
    expect(read.obs1Id).toBe(0);

    gf.close();
    st.close();
  });

  test('max 2 observations enforced', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const rec = gf.createEntity('Node', 'Type', now);
    gf.addObservation(rec.offset, 'Obs 1', now);
    gf.addObservation(rec.offset, 'Obs 2', now);

    expect(() => gf.addObservation(rec.offset, 'Obs 3', now)).toThrow('max observations');

    gf.close();
    st.close();
  });

  test('add and read edges', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const a = gf.createEntity('Alice', 'Person', now);
    const b = gf.createEntity('Bob', 'Person', now);
    const relTypeId = Number(st.intern('KNOWS'));

    gf.addEdge(a.offset, {
      targetOffset: b.offset,
      direction: DIR_FORWARD,
      relTypeId,
      mtime: now,
    });

    // Also add backward link on B
    gf.addEdge(b.offset, {
      targetOffset: a.offset,
      direction: DIR_BACKWARD,
      relTypeId,
      mtime: now,
    });

    const aEdges = gf.getEdges(a.offset);
    expect(aEdges.length).toBe(1);
    expect(aEdges[0].targetOffset).toBe(b.offset);
    expect(aEdges[0].direction).toBe(DIR_FORWARD);
    expect(st.get(BigInt(aEdges[0].relTypeId))).toBe('KNOWS');

    const bEdges = gf.getEdges(b.offset);
    expect(bEdges.length).toBe(1);
    expect(bEdges[0].targetOffset).toBe(a.offset);
    expect(bEdges[0].direction).toBe(DIR_BACKWARD);

    gf.close();
    st.close();
  });

  test('adj block grows beyond initial capacity', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const hub = gf.createEntity('Hub', 'Node', now);
    const targets: bigint[] = [];

    // Create 10 neighbors — exceeds initial capacity of 4
    for (let i = 0; i < 10; i++) {
      const t = gf.createEntity(`Target_${i}`, 'Node', now);
      targets.push(t.offset);
      const relTypeId = Number(st.intern('EDGE'));
      gf.addEdge(hub.offset, {
        targetOffset: t.offset,
        direction: DIR_FORWARD,
        relTypeId,
        mtime: now,
      });
    }

    const edges = gf.getEdges(hub.offset);
    expect(edges.length).toBe(10);

    // All targets should be present
    const edgeTargets = edges.map(e => e.targetOffset);
    for (const t of targets) {
      expect(edgeTargets).toContain(t);
    }

    gf.close();
    st.close();
  });

  test('remove edge', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());

    const a = gf.createEntity('A', 'Node', now);
    const b = gf.createEntity('B', 'Node', now);
    const c = gf.createEntity('C', 'Node', now);
    const relTypeId = Number(st.intern('LINK'));

    gf.addEdge(a.offset, { targetOffset: b.offset, direction: DIR_FORWARD, relTypeId, mtime: now });
    gf.addEdge(a.offset, { targetOffset: c.offset, direction: DIR_FORWARD, relTypeId, mtime: now });

    expect(gf.getEdges(a.offset).length).toBe(2);

    const removed = gf.removeEdge(a.offset, b.offset, relTypeId, DIR_FORWARD);
    expect(removed).toBe(true);

    const remaining = gf.getEdges(a.offset);
    expect(remaining.length).toBe(1);
    expect(remaining[0].targetOffset).toBe(c.offset);

    gf.close();
    st.close();
  });

  test('entity with no edges returns empty array', () => {
    const { st, gf } = makeGF();
    const rec = gf.createEntity('Lonely', 'Node', BigInt(Date.now()));
    expect(gf.getEdges(rec.offset)).toEqual([]);

    gf.close();
    st.close();
  });

  test('persists across close and reopen', () => {
    let entityOffset: bigint;
    {
      const st = new StringTable(STR_PATH);
      const gf = new GraphFile(GRAPH_PATH, st);
      const now = BigInt(Date.now());
      const rec = gf.createEntity('Persistent', 'Data', now);
      gf.addObservation(rec.offset, 'Survives restart', now);
      entityOffset = rec.offset;
      gf.close();
      st.close();
    }

    {
      const st = new StringTable(STR_PATH);
      const gf = new GraphFile(GRAPH_PATH, st);
      const read = gf.readEntity(entityOffset);
      expect(st.get(BigInt(read.nameId))).toBe('Persistent');
      expect(st.get(BigInt(read.typeId))).toBe('Data');
      expect(read.obsCount).toBe(1);
      expect(st.get(BigInt(read.obs0Id))).toBe('Survives restart');
      expect(gf.getEntityCount()).toBe(1);
      gf.close();
      st.close();
    }
  });

  test('node log survives grow', () => {
    const { st, gf } = makeGF();
    const now = BigInt(Date.now());
    const offsets: bigint[] = [];

    // Initial log capacity is 256, insert 300 to trigger grow
    for (let i = 0; i < 300; i++) {
      const rec = gf.createEntity(`N_${i}`, 'Bulk', now);
      offsets.push(rec.offset);
    }

    expect(gf.getEntityCount()).toBe(300);
    const logged = gf.getAllEntityOffsets();
    for (const off of offsets) {
      expect(logged).toContain(off);
    }

    gf.close();
    st.close();
  });
});
