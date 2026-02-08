/**
 * Import/roundtrip test: validates that the binary store (GraphFile + StringTable)
 * can faithfully import a JSONL knowledge graph and recall every entity, observation,
 * relation, and timestamp with perfect fidelity.
 *
 * Uses a realistic fixture modeled after actual KB data: mixed entity types,
 * observations at the 140-char limit, varied relation types, timestamps, etc.
 */

import { GraphFile, DIR_FORWARD, DIR_BACKWARD, EntityRecord, AdjEntry } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { unlinkSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

// --- JSON types matching server.ts ---

interface Entity {
  type: 'entity';
  name: string;
  entityType: string;
  observations: string[];
  mtime?: number;
  obsMtime?: number;
}

interface Relation {
  type: 'relation';
  from: string;
  to: string;
  relationType: string;
  mtime?: number;
}

interface KnowledgeGraph {
  entities: Entity[];
  relations: Relation[];
}

// --- Realistic fixture ---

function buildFixture(): KnowledgeGraph {
  const now = 1770582806902;  // realistic millisecond timestamp

  const entities: Entity[] = [
    // Core project entity with max observations
    {
      type: 'entity',
      name: 'BinaryGraphStore',
      entityType: 'Project',
      observations: [
        'Custom binary graph store. No hash index. Self at fixed offset 0 is sole entry point.',
        'No SQL, no existing DBs. All edges bidirectional in storage; direction at query time.',
      ],
      mtime: now,
      obsMtime: now - 100000,
    },
    // Design entities
    {
      type: 'entity',
      name: 'BGS_Sharding',
      entityType: 'Design',
      observations: [
        'Directory of N shard files. Entity -> shard via hash(name) mod N.',
        'Cross-shard traversal = two file touches. Same-shard = one.',
      ],
      mtime: now - 50000,
      obsMtime: now - 50000,
    },
    {
      type: 'entity',
      name: 'BGS_Concurrency',
      entityType: 'Design',
      observations: [
        'POSIX flock per shard file: LOCK_SH for reads, LOCK_EX for writes.',
      ],
      mtime: now - 40000,
      obsMtime: now - 40000,
    },
    // Decision entity with no observations
    {
      type: 'entity',
      name: 'BGS_D_ErrorPolicy',
      entityType: 'Decision',
      observations: [],
      mtime: now - 30000,
    },
    // Entity with 140-char observation (at the limit)
    {
      type: 'entity',
      name: 'BGS_D_DirectionPolicy',
      entityType: 'Decision',
      observations: [
        'A'.repeat(140),
      ],
      mtime: now - 20000,
      obsMtime: now - 20000,
    },
    // Entity with unicode in name and observations
    {
      type: 'entity',
      name: 'Unicode_Test_日本語',
      entityType: 'TestCase',
      observations: [
        'Handles UTF-8: émojis 🚀, CJK 漢字, accents àéîõü',
      ],
      mtime: now - 10000,
      obsMtime: now - 10000,
    },
    // Thought entity (from sequentialthinking)
    {
      type: 'entity',
      name: 'a1b2c3d4e5f6a1b2c3d4e5f6',
      entityType: 'Thought',
      observations: [
        'First thought in chain',
        'Second observation for this thought',
      ],
      mtime: now - 5000,
      obsMtime: now - 5000,
    },
    // Entity with no mtime/obsMtime (legacy data)
    {
      type: 'entity',
      name: 'LegacyEntity',
      entityType: 'OldData',
      observations: ['Predates timestamp tracking'],
    },
    // Orphan entity (no relations)
    {
      type: 'entity',
      name: 'OrphanNode',
      entityType: 'Isolated',
      observations: [],
      mtime: now,
    },
    // Self entity (root of the graph)
    {
      type: 'entity',
      name: 'Self',
      entityType: 'Core',
      observations: ['The root entity that anchors the knowledge graph'],
      mtime: now,
      obsMtime: now,
    },
  ];

  const relations: Relation[] = [
    // Typed hierarchy
    { type: 'relation', from: 'BinaryGraphStore', to: 'BGS_Sharding', relationType: 'CONTAINS', mtime: now },
    { type: 'relation', from: 'BinaryGraphStore', to: 'BGS_Concurrency', relationType: 'CONTAINS', mtime: now },
    { type: 'relation', from: 'BinaryGraphStore', to: 'BGS_D_ErrorPolicy', relationType: 'HAS_DECISION', mtime: now },
    { type: 'relation', from: 'BinaryGraphStore', to: 'BGS_D_DirectionPolicy', relationType: 'HAS_DECISION', mtime: now },
    // Cross-type relations
    { type: 'relation', from: 'BGS_Sharding', to: 'BGS_Concurrency', relationType: 'ENABLES', mtime: now - 1000 },
    { type: 'relation', from: 'BGS_D_ErrorPolicy', to: 'BinaryGraphStore', relationType: 'RESOLVES', mtime: now - 2000 },
    // Thought chain
    { type: 'relation', from: 'a1b2c3d4e5f6a1b2c3d4e5f6', to: 'BinaryGraphStore', relationType: 'follows', mtime: now - 3000 },
    // Self connection
    { type: 'relation', from: 'Self', to: 'BinaryGraphStore', relationType: 'WORKS_ON', mtime: now },
    // Unicode entity relation
    { type: 'relation', from: 'Unicode_Test_日本語', to: 'BinaryGraphStore', relationType: 'TESTS', mtime: now },
    // Relation with no mtime (legacy)
    { type: 'relation', from: 'LegacyEntity', to: 'Self', relationType: 'PREDATES' },
    // Multiple relation types between same pair
    { type: 'relation', from: 'BGS_Sharding', to: 'BGS_Concurrency', relationType: 'DEPENDS_ON', mtime: now },
  ];

  return { entities, relations };
}

// --- Test paths ---

const GRAPH_PATH = join(tmpdir(), `import-test-graph-${process.pid}.dat`);
const STR_PATH = join(tmpdir(), `import-test-str-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(GRAPH_PATH); } catch {}
  try { unlinkSync(STR_PATH); } catch {}
}

describe('Import/Roundtrip', () => {
  afterEach(cleanup);

  // We need a proper import function that doesn't use dynamic import
  function doImport(gf: GraphFile, st: StringTable, graph: KnowledgeGraph): Map<string, bigint> {
    const entityOffsets = new Map<string, bigint>();

    // Phase 1: create all entities
    for (const e of graph.entities) {
      const mtime = BigInt(e.mtime ?? 0);
      const obsMtime = BigInt(e.obsMtime ?? e.mtime ?? 0);
      const rec = gf.createEntity(e.name, e.entityType, mtime, obsMtime);

      // Add observations (addObservation updates both mtime and obsMtime on the record)
      for (const obs of e.observations) {
        gf.addObservation(rec.offset, obs, obsMtime);
      }

      // addObservation clobbers mtime with the observation timestamp.
      // During import we need to restore the original mtime.
      if (e.observations.length > 0 && obsMtime !== mtime) {
        const updated = gf.readEntity(rec.offset);
        updated.mtime = mtime;
        gf.updateEntity(updated);
      }

      entityOffsets.set(e.name, rec.offset);
    }

    // Phase 2: create all relations (with bidirectional storage)
    for (const r of graph.relations) {
      const fromOffset = entityOffsets.get(r.from);
      const toOffset = entityOffsets.get(r.to);
      if (!fromOffset || !toOffset) continue;

      const relTypeId = Number(st.intern(r.relationType));
      const mtime = BigInt(r.mtime ?? 0);

      gf.addEdge(fromOffset, {
        targetOffset: toOffset,
        direction: DIR_FORWARD,
        relTypeId,
        mtime,
      });

      gf.addEdge(toOffset, {
        targetOffset: fromOffset,
        direction: DIR_BACKWARD,
        relTypeId,
        mtime,
      });
    }

    return entityOffsets;
  }

  test('all entities survive import with correct fields', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    expect(offsets.size).toBe(fixture.entities.length);
    expect(gf.getEntityCount()).toBe(fixture.entities.length);

    for (const e of fixture.entities) {
      const offset = offsets.get(e.name)!;
      expect(offset).toBeDefined();

      const rec = gf.readEntity(offset);

      // Name
      expect(st.get(BigInt(rec.nameId))).toBe(e.name);
      // Type
      expect(st.get(BigInt(rec.typeId))).toBe(e.entityType);
      // Observation count
      expect(rec.obsCount).toBe(e.observations.length);
      // Observation content
      if (e.observations.length >= 1) {
        expect(st.get(BigInt(rec.obs0Id))).toBe(e.observations[0]);
      }
      if (e.observations.length >= 2) {
        expect(st.get(BigInt(rec.obs1Id))).toBe(e.observations[1]);
      }
      // mtime
      expect(rec.mtime).toBe(BigInt(e.mtime ?? 0));
    }

    gf.close();
    st.close();
  });

  test('all relations survive import as bidirectional edges', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    for (const r of fixture.relations) {
      const fromOffset = offsets.get(r.from)!;
      const toOffset = offsets.get(r.to)!;
      if (!fromOffset || !toOffset) continue;

      const relTypeId = Number(st.intern(r.relationType));

      // Check forward edge on source
      const fromEdges = gf.getEdges(fromOffset);
      const fwdEdge = fromEdges.find(
        e => e.targetOffset === toOffset && e.direction === DIR_FORWARD && e.relTypeId === relTypeId
      );
      expect(fwdEdge).toBeDefined();
      expect(fwdEdge!.mtime).toBe(BigInt(r.mtime ?? 0));

      // Check backward edge on target
      const toEdges = gf.getEdges(toOffset);
      const bwdEdge = toEdges.find(
        e => e.targetOffset === fromOffset && e.direction === DIR_BACKWARD && e.relTypeId === relTypeId
      );
      expect(bwdEdge).toBeDefined();
      expect(bwdEdge!.mtime).toBe(BigInt(r.mtime ?? 0));
    }

    // Release the extra refs from intern() in the assertion loop
    // (intern bumps refcount; this is a test artifact)
    gf.close();
    st.close();
  });

  test('edge counts are correct per entity', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    // Count expected edges per entity (forward = outgoing, backward = incoming)
    const expectedForward = new Map<string, number>();
    const expectedBackward = new Map<string, number>();
    for (const e of fixture.entities) {
      expectedForward.set(e.name, 0);
      expectedBackward.set(e.name, 0);
    }
    for (const r of fixture.relations) {
      if (offsets.has(r.from) && offsets.has(r.to)) {
        expectedForward.set(r.from, (expectedForward.get(r.from) ?? 0) + 1);
        expectedBackward.set(r.to, (expectedBackward.get(r.to) ?? 0) + 1);
      }
    }

    for (const e of fixture.entities) {
      const offset = offsets.get(e.name)!;
      const edges = gf.getEdges(offset);

      const fwd = edges.filter(e => e.direction === DIR_FORWARD).length;
      const bwd = edges.filter(e => e.direction === DIR_BACKWARD).length;

      expect(fwd).toBe(expectedForward.get(e.name));
      expect(bwd).toBe(expectedBackward.get(e.name));
    }

    gf.close();
    st.close();
  });

  test('string table deduplicates shared strings', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    doImport(gf, st, fixture);

    // 'CONTAINS' is used 4 times as relationType in the fixture
    // 'HAS_DECISION' is used 2 times
    // 'Design' is used 2 times as entityType
    // Each should be interned once, with refcount = usage count
    // (But note: intern() in doImport already bumps refcount per use,
    //  plus entity name/type intern from createEntity.)

    // Just verify uniqueness: intern('CONTAINS') should return an existing ID
    const containsId = st.intern('CONTAINS');
    expect(st.refcount(containsId)).toBeGreaterThan(1);
    // Release the extra ref we just created
    st.release(containsId);

    gf.close();
    st.close();
  });

  test('140-char observation preserved exactly', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    const dirPolicyOffset = offsets.get('BGS_D_DirectionPolicy')!;
    const rec = gf.readEntity(dirPolicyOffset);
    const obs = st.get(BigInt(rec.obs0Id));
    expect(obs).toBe('A'.repeat(140));
    expect(obs.length).toBe(140);

    gf.close();
    st.close();
  });

  test('UTF-8 entity names and observations preserved', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    const unicodeOffset = offsets.get('Unicode_Test_日本語')!;
    const rec = gf.readEntity(unicodeOffset);
    expect(st.get(BigInt(rec.nameId))).toBe('Unicode_Test_日本語');
    expect(st.get(BigInt(rec.obs0Id))).toBe('Handles UTF-8: émojis 🚀, CJK 漢字, accents àéîõü');

    gf.close();
    st.close();
  });

  test('orphan entity (no relations) has no edges', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    const orphanOffset = offsets.get('OrphanNode')!;
    expect(gf.getEdges(orphanOffset)).toEqual([]);

    gf.close();
    st.close();
  });

  test('data survives close and reopen', () => {
    const fixture = buildFixture();
    let offsets: Map<string, bigint>;

    // Import
    {
      const st = new StringTable(STR_PATH);
      const gf = new GraphFile(GRAPH_PATH, st);
      offsets = doImport(gf, st, fixture);
      gf.close();
      st.close();
    }

    // Reopen and verify
    {
      const st = new StringTable(STR_PATH);
      const gf = new GraphFile(GRAPH_PATH, st);

      expect(gf.getEntityCount()).toBe(fixture.entities.length);

      // Spot-check a few entities
      const bgsOffset = offsets.get('BinaryGraphStore')!;
      const bgsRec = gf.readEntity(bgsOffset);
      expect(st.get(BigInt(bgsRec.nameId))).toBe('BinaryGraphStore');
      expect(st.get(BigInt(bgsRec.typeId))).toBe('Project');
      expect(bgsRec.obsCount).toBe(2);

      // Verify edges survived
      const bgsEdges = gf.getEdges(bgsOffset);
      const fwdEdges = bgsEdges.filter(e => e.direction === DIR_FORWARD);
      // BinaryGraphStore has 4 outgoing in fixture: CONTAINS×2 + HAS_DECISION×2
      expect(fwdEdges.length).toBe(4);

      // Verify Self
      const selfOffset = offsets.get('Self')!;
      const selfRec = gf.readEntity(selfOffset);
      expect(st.get(BigInt(selfRec.nameId))).toBe('Self');

      gf.close();
      st.close();
    }
  });

  test('multiple relations between same entity pair', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    // BGS_Sharding -> BGS_Concurrency has both ENABLES and DEPENDS_ON
    const shardOffset = offsets.get('BGS_Sharding')!;
    const concOffset = offsets.get('BGS_Concurrency')!;

    const shardEdges = gf.getEdges(shardOffset);
    const toConc = shardEdges.filter(
      e => e.targetOffset === concOffset && e.direction === DIR_FORWARD
    );
    expect(toConc.length).toBe(2);

    const relTypes = toConc.map(e => st.get(BigInt(e.relTypeId))).sort();
    expect(relTypes).toEqual(['DEPENDS_ON', 'ENABLES']);

    gf.close();
    st.close();
  });

  test('legacy entity with no mtime stores as 0', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    const legacyOffset = offsets.get('LegacyEntity')!;
    const rec = gf.readEntity(legacyOffset);
    expect(rec.mtime).toBe(0n);
    expect(st.get(BigInt(rec.nameId))).toBe('LegacyEntity');
    expect(st.get(BigInt(rec.obs0Id))).toBe('Predates timestamp tracking');

    gf.close();
    st.close();
  });

  test('full node log enumeration matches entity set', () => {
    const st = new StringTable(STR_PATH);
    const gf = new GraphFile(GRAPH_PATH, st);
    const fixture = buildFixture();

    const offsets = doImport(gf, st, fixture);

    const allOffsets = gf.getAllEntityOffsets();
    expect(allOffsets.length).toBe(fixture.entities.length);

    // Every imported offset should appear in the node log
    for (const [name, offset] of offsets) {
      expect(allOffsets).toContain(offset);
    }

    // Every offset in the log should be a valid entity we imported
    const importedOffsets = new Set([...offsets.values()].map(String));
    for (const off of allOffsets) {
      expect(importedOffsets.has(String(off))).toBe(true);
    }

    gf.close();
    st.close();
  });
});
