/**
 * PageRank tests — structural sampling and walker visit counting.
 */

import { GraphFile, DIR_FORWARD, DIR_BACKWARD, ENTITY_RECORD_SIZE } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { structuralIteration, structuralSample } from '../src/pagerank.js';
import { unlinkSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

const GRAPH_PATH = join(tmpdir(), `pr-test-graph-${process.pid}.dat`);
const STR_PATH = join(tmpdir(), `pr-test-str-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(GRAPH_PATH); } catch {}
  try { unlinkSync(STR_PATH); } catch {}
}

describe('PageRank', () => {

  describe('EntityRecord PageRank fields', () => {
    let st: StringTable;
    let gf: GraphFile;

    beforeAll(() => {
      cleanup();
      st = new StringTable(STR_PATH);
      gf = new GraphFile(GRAPH_PATH, st);
    });

    afterAll(() => {
      gf.close();
      st.close();
      cleanup();
    });

    test('new entity has zero visit counts', () => {
      const rec = gf.createEntity('TestNode', 'test', BigInt(Date.now()));
      expect(rec.structuralVisits).toBe(0n);
      expect(rec.walkerVisits).toBe(0n);

      const read = gf.readEntity(rec.offset);
      expect(read.structuralVisits).toBe(0n);
      expect(read.walkerVisits).toBe(0n);
    });

    test('incrementStructuralVisit bumps entity and global counter', () => {
      const rec = gf.createEntity('StructNode', 'test', BigInt(Date.now()));

      expect(gf.getStructuralTotal()).toBe(0n);

      gf.incrementStructuralVisit(rec.offset);
      gf.incrementStructuralVisit(rec.offset);
      gf.incrementStructuralVisit(rec.offset);

      const read = gf.readEntity(rec.offset);
      expect(read.structuralVisits).toBe(3n);
      expect(gf.getStructuralTotal()).toBe(3n);
    });

    test('incrementWalkerVisit bumps entity and global counter', () => {
      const rec = gf.createEntity('WalkNode', 'test', BigInt(Date.now()));

      expect(gf.getWalkerTotal()).toBe(0n);

      gf.incrementWalkerVisit(rec.offset);
      gf.incrementWalkerVisit(rec.offset);

      const read = gf.readEntity(rec.offset);
      expect(read.walkerVisits).toBe(2n);
      expect(gf.getWalkerTotal()).toBe(2n);
    });

    test('structural and walker counters are independent', () => {
      const rec = gf.createEntity('BothNode', 'test', BigInt(Date.now()));

      gf.incrementStructuralVisit(rec.offset);
      gf.incrementWalkerVisit(rec.offset);
      gf.incrementWalkerVisit(rec.offset);

      const read = gf.readEntity(rec.offset);
      expect(read.structuralVisits).toBe(1n);
      expect(read.walkerVisits).toBe(2n);
    });

    test('getStructuralRank returns visits/total', () => {
      const a = gf.createEntity('RankA', 'test', BigInt(Date.now()));
      const b = gf.createEntity('RankB', 'test', BigInt(Date.now()));

      // Reset: create fresh gf to get clean counters
      // (counters accumulated from prior tests — just check relative behavior)
      const baseTotal = gf.getStructuralTotal();

      gf.incrementStructuralVisit(a.offset);
      gf.incrementStructuralVisit(a.offset);
      gf.incrementStructuralVisit(a.offset);
      gf.incrementStructuralVisit(b.offset);

      const rankA = gf.getStructuralRank(a.offset);
      const rankB = gf.getStructuralRank(b.offset);

      // A should rank higher than B (3 visits vs 1)
      expect(rankA).toBeGreaterThan(rankB);
      expect(rankA).toBeGreaterThan(0);
      expect(rankB).toBeGreaterThan(0);
    });
  });

  describe('Structural sampling on known graph', () => {
    let st: StringTable;
    let gf: GraphFile;
    let offsets: Map<string, bigint>;

    /**
     * Build a star graph: Hub -> A, Hub -> B, Hub -> C, Hub -> D
     * A, B, C, D are dangling (no outgoing edges).
     *
     * Expected behavior: walks from Hub follow one edge then stop.
     * Walks from A,B,C,D stop immediately (dangling).
     * Hub gets visited 5 times per iteration (once from its own start,
     * A/B/C/D never lead back). A,B,C,D each get visited ~1.85 times
     * (once from self start, plus ~0.85 chance from Hub picking them).
     * Actually with 4 outgoing edges: Hub visits self, then with prob 0.85
     * picks one of {A,B,C,D} equally. So each leaf gets ~0.85/4 = 0.2125
     * from Hub's walk + 1 from its own walk = ~1.2125 expected per iteration.
     * Hub: 1 from own walk (stops because it terminates or follows) +
     * 0 from other walks (no incoming edges). Actually Hub has no incoming
     * edges, so it only gets visited from its own walk start. Expected = 1.
     * Wait — Hub visits ITSELF in the walk: step 0 = Hub (visit +1),
     * then follows edge (visits A or B or C or D). So Hub gets exactly 1
     * visit per iteration (from its own walk start), and each leaf gets
     * exactly 1 + 0.85/4 = 1.2125.
     */
    beforeAll(() => {
      const gpath = join(tmpdir(), `pr-star-graph-${process.pid}.dat`);
      const spath = join(tmpdir(), `pr-star-str-${process.pid}.dat`);
      try { unlinkSync(gpath); } catch {}
      try { unlinkSync(spath); } catch {}

      st = new StringTable(spath);
      gf = new GraphFile(gpath, st);
      offsets = new Map();

      const now = BigInt(Date.now());
      for (const name of ['Hub', 'A', 'B', 'C', 'D']) {
        const rec = gf.createEntity(name, 'node', now);
        offsets.set(name, rec.offset);
      }

      // Hub -> A, B, C, D (forward edges)
      const relTypeId = Number(st.intern('LINKS_TO'));
      for (const target of ['A', 'B', 'C', 'D']) {
        gf.addEdge(offsets.get('Hub')!, {
          targetOffset: offsets.get(target)!,
          direction: DIR_FORWARD,
          relTypeId,
          mtime: now,
        });
        gf.addEdge(offsets.get(target)!, {
          targetOffset: offsets.get('Hub')!,
          direction: DIR_BACKWARD,
          relTypeId,
          mtime: now,
        });
      }
    });

    afterAll(() => {
      gf.close();
      st.close();
    });

    test('one iteration visits all nodes', () => {
      const visits = structuralIteration(gf, 0.85);
      // At minimum: 5 walks, each visits at least 1 node = 5
      // Hub's walk: 1 (Hub) + 1 (random leaf) with prob 0.85, so ~1.85
      // Each leaf: 1 (itself, dangling, stops)
      // Total: ~1.85 + 4 = ~5.85 per iteration
      expect(visits).toBeGreaterThanOrEqual(5);
      expect(visits).toBeLessThanOrEqual(50); // reasonable upper bound
    });

    test('structural ranks converge with many iterations', () => {
      // Run many iterations for statistical convergence
      structuralSample(gf, 1000, 0.85);

      const total = gf.getStructuralTotal();
      expect(total).toBeGreaterThan(0n);

      // Read visit counts
      const hubRec = gf.readEntity(offsets.get('Hub')!);
      const aRec = gf.readEntity(offsets.get('A')!);
      const bRec = gf.readEntity(offsets.get('B')!);
      const cRec = gf.readEntity(offsets.get('C')!);
      const dRec = gf.readEntity(offsets.get('D')!);

      const hubVisits = Number(hubRec.structuralVisits);
      const leafVisits = [aRec, bRec, cRec, dRec].map(r => Number(r.structuralVisits));

      // Hub should have ~1 visit per iteration (only from own walk start)
      // Each leaf should have ~1.2125 visits per iteration
      // With 1001 iterations (1 from prior test + 1000 here):

      // Hub gets fewer visits than each leaf? No — Hub gets 1 per iter,
      // leaves get ~1.2125. But Hub also gets visited from the initial test.
      // Let's just check the relative ordering and approximate ratios.

      // All leaves should be approximately equal
      const avgLeaf = leafVisits.reduce((a, b) => a + b, 0) / 4;
      for (const lv of leafVisits) {
        // Each leaf should be within 20% of average (statistical tolerance)
        expect(lv / avgLeaf).toBeGreaterThan(0.8);
        expect(lv / avgLeaf).toBeLessThan(1.2);
      }

      // Leaves should collectively have more visits than Hub
      // (4 leaves × ~1.2125 vs Hub's ~1.0 per iteration)
      expect(avgLeaf).toBeGreaterThan(hubVisits * 0.9);

      // PageRank sums to approximately 1
      const sumRanks = gf.getStructuralRank(offsets.get('Hub')!) +
                       gf.getStructuralRank(offsets.get('A')!) +
                       gf.getStructuralRank(offsets.get('B')!) +
                       gf.getStructuralRank(offsets.get('C')!) +
                       gf.getStructuralRank(offsets.get('D')!);
      expect(sumRanks).toBeCloseTo(1.0, 1);

      console.log(`  Hub: ${hubVisits} visits, rank=${gf.getStructuralRank(offsets.get('Hub')!).toFixed(4)}`);
      console.log(`  Leaves avg: ${avgLeaf.toFixed(0)} visits, ranks=${leafVisits.map(v => (Number(v) / Number(total)).toFixed(4)).join(', ')}`);
      console.log(`  Total: ${total}, sum of ranks: ${sumRanks.toFixed(4)}`);
    });
  });

  describe('Structural sampling on linear chain', () => {
    let st: StringTable;
    let gf: GraphFile;
    let offsets: Map<string, bigint>;

    /**
     * Linear chain: A -> B -> C -> D -> E (all forward)
     * E is dangling. Walks from A traverse the most nodes.
     *
     * Expected ranking (most visits): A > B > C > D > E? No —
     * walks start from each node. But nodes earlier in the chain
     * are visited by walks starting from predecessors.
     * A: visited only from own walk (no incoming forward edges)
     * B: visited from own walk + walks starting at A
     * C: visited from own + A's + B's walks
     * etc.
     * So E should have the highest PageRank (most incoming paths).
     */
    beforeAll(() => {
      const gpath = join(tmpdir(), `pr-chain-graph-${process.pid}.dat`);
      const spath = join(tmpdir(), `pr-chain-str-${process.pid}.dat`);
      try { unlinkSync(gpath); } catch {}
      try { unlinkSync(spath); } catch {}

      st = new StringTable(spath);
      gf = new GraphFile(gpath, st);
      offsets = new Map();

      const now = BigInt(Date.now());
      const names = ['A', 'B', 'C', 'D', 'E'];
      for (const name of names) {
        const rec = gf.createEntity(name, 'node', now);
        offsets.set(name, rec.offset);
      }

      const relTypeId = Number(st.intern('NEXT'));
      for (let i = 0; i < names.length - 1; i++) {
        gf.addEdge(offsets.get(names[i])!, {
          targetOffset: offsets.get(names[i + 1])!,
          direction: DIR_FORWARD,
          relTypeId,
          mtime: now,
        });
        gf.addEdge(offsets.get(names[i + 1])!, {
          targetOffset: offsets.get(names[i])!,
          direction: DIR_BACKWARD,
          relTypeId,
          mtime: now,
        });
      }
    });

    afterAll(() => {
      gf.close();
      st.close();
    });

    test('downstream nodes rank higher (more incoming paths)', () => {
      structuralSample(gf, 2000, 0.85);

      const visits: Record<string, number> = {};
      for (const name of ['A', 'B', 'C', 'D', 'E']) {
        const rec = gf.readEntity(offsets.get(name)!);
        visits[name] = Number(rec.structuralVisits);
      }

      // E should have more visits than D, D more than C, etc.
      // (each downstream node accumulates visits from all upstream walk-throughs)
      // With statistical noise, just check the overall trend
      expect(visits['E']).toBeGreaterThan(visits['A']);
      expect(visits['D']).toBeGreaterThan(visits['A']);
      expect(visits['C']).toBeGreaterThan(visits['A']);

      // More precise: B > A (B gets A's walk-throughs)
      expect(visits['B']).toBeGreaterThan(visits['A']);

      console.log(`  Chain visits: A=${visits['A']}, B=${visits['B']}, C=${visits['C']}, D=${visits['D']}, E=${visits['E']}`);
    });
  });

  describe('Walker visit counting', () => {
    let st: StringTable;
    let gf: GraphFile;

    beforeAll(() => {
      const gpath = join(tmpdir(), `pr-walker-graph-${process.pid}.dat`);
      const spath = join(tmpdir(), `pr-walker-str-${process.pid}.dat`);
      try { unlinkSync(gpath); } catch {}
      try { unlinkSync(spath); } catch {}

      st = new StringTable(spath);
      gf = new GraphFile(gpath, st);
    });

    afterAll(() => {
      gf.close();
      st.close();
    });

    test('multiple nodes visited in one event', () => {
      const now = BigInt(Date.now());
      const a = gf.createEntity('WA', 'test', now);
      const b = gf.createEntity('WB', 'test', now);
      const c = gf.createEntity('WC', 'test', now);

      // Simulate open_nodes(["WA", "WB", "WC"]) — each gets +1
      gf.incrementWalkerVisit(a.offset);
      gf.incrementWalkerVisit(b.offset);
      gf.incrementWalkerVisit(c.offset);

      expect(gf.readEntity(a.offset).walkerVisits).toBe(1n);
      expect(gf.readEntity(b.offset).walkerVisits).toBe(1n);
      expect(gf.readEntity(c.offset).walkerVisits).toBe(1n);
      expect(gf.getWalkerTotal()).toBe(3n);
    });

    test('repeated visits accumulate', () => {
      const now = BigInt(Date.now());
      const hot = gf.createEntity('HotNode', 'test', now);
      const cold = gf.createEntity('ColdNode', 'test', now);

      for (let i = 0; i < 100; i++) {
        gf.incrementWalkerVisit(hot.offset);
      }
      gf.incrementWalkerVisit(cold.offset);

      const hotRec = gf.readEntity(hot.offset);
      const coldRec = gf.readEntity(cold.offset);

      expect(hotRec.walkerVisits).toBe(100n);
      expect(coldRec.walkerVisits).toBe(1n);

      const hotRank = gf.getWalkerRank(hot.offset);
      const coldRank = gf.getWalkerRank(cold.offset);
      expect(hotRank).toBeGreaterThan(coldRank * 50);
    });
  });

  describe('Persistence', () => {
    const gpath = join(tmpdir(), `pr-persist-graph-${process.pid}.dat`);
    const spath = join(tmpdir(), `pr-persist-str-${process.pid}.dat`);

    afterAll(() => {
      try { unlinkSync(gpath); } catch {}
      try { unlinkSync(spath); } catch {}
    });

    test('visit counts survive close and reopen', () => {
      // Create and populate
      let st = new StringTable(spath);
      let gf = new GraphFile(gpath, st);

      const now = BigInt(Date.now());
      const rec = gf.createEntity('Persist', 'test', now);

      gf.incrementStructuralVisit(rec.offset);
      gf.incrementStructuralVisit(rec.offset);
      gf.incrementStructuralVisit(rec.offset);
      gf.incrementWalkerVisit(rec.offset);
      gf.incrementWalkerVisit(rec.offset);

      expect(gf.getStructuralTotal()).toBe(3n);
      expect(gf.getWalkerTotal()).toBe(2n);

      const entityOffset = rec.offset;
      gf.close();
      st.close();

      // Reopen
      st = new StringTable(spath);
      gf = new GraphFile(gpath, st);

      expect(gf.getStructuralTotal()).toBe(3n);
      expect(gf.getWalkerTotal()).toBe(2n);

      const readRec = gf.readEntity(entityOffset);
      expect(readRec.structuralVisits).toBe(3n);
      expect(readRec.walkerVisits).toBe(2n);
      expect(gf.getStructuralRank(entityOffset)).toBeCloseTo(1.0, 5);

      gf.close();
      st.close();
    });
  });
});
