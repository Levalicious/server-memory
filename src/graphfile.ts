/**
 * Graph record types — binary layouts for entities, adjacency blocks, and node log.
 *
 * All records live in a MemoryFile (graph.mem). Variable-length strings are
 * stored in a separate StringTable (strings.mem) and referenced by u32 ID.
 *
 * Versioning:
 *   MEMFILE_VERSION 1  = original 64-byte entity records (no psi field)
 *   MEMFILE_VERSION 2  = 72-byte entity records with f64 psi for MERW
 *   On open, version 1 files are migrated to version 2 automatically.
 *
 * Graph file layout:
 *   [memfile header: 32 bytes]
 *   [graph header block: first allocation]
 *     u64 node_log_offset
 *     u64 structural_total     total structural walk visits (global counter)
 *     u64 walker_total         total walker visits (global counter)
 *   [entity records, adj blocks, node log ...]
 *
 * EntityRecord: 72 bytes fixed (v2)
 *   u32  name_id         string table ID
 *   u32  type_id         string table ID
 *   u64  adj_offset      offset to AdjBlock (0 = no edges)
 *   u64  mtime           general modification timestamp (ms)
 *   u64  obsMtime        observation modification timestamp (ms)
 *   u8   obs_count       0, 1, or 2
 *   u8   _pad[3]
 *   u32  obs0_id         string table ID (0 = empty)
 *   u32  obs1_id         string table ID (0 = empty)
 *   u64  structural_visits  structural PageRank visit count
 *   u64  walker_visits      walker PageRank visit count
 *   f64  psi             MERW dominant eigenvector component
 *
 * AdjBlock:
 *   u32  count
 *   u32  capacity
 *   AdjEntry[capacity]:
 *     u64  target_and_dir    62 bits target offset | 2 bits direction
 *     u32  relType_id        string table ID
 *     u32  _pad
 *     u64  mtime
 *
 * AdjEntry direction bits (in the low 2 bits of target_and_dir):
 *   0b00 = FORWARD   (this entity is 'from')
 *   0b01 = BACKWARD  (this entity is 'to', edge points at us)
 *   0b10 = BIDIR     (reserved)
 *
 * NodeLog:
 *   u32  count
 *   u32  capacity
 *   u64  offsets[capacity]
 */

import * as fs from 'fs';
import { MemoryFile } from './memoryfile.js';
import { type StringTable } from './stringtable.js';

// --- Constants ---

export const ENTITY_RECORD_SIZE = 72;
const OLD_ENTITY_RECORD_SIZE = 64;     // v1 layout without psi
export const ADJ_ENTRY_SIZE = 24;      // 8 + 4 + 4 + 8, naturally aligned
const ADJ_HEADER_SIZE = 8;             // count:u32 + capacity:u32
const NODE_LOG_HEADER_SIZE = 8;        // count:u32 + capacity:u32
const GRAPH_HEADER_SIZE = 24;          // node_log_offset:u64 + structural_total:u64 + walker_total:u64
const INITIAL_ADJ_CAPACITY = 4;
const INITIAL_LOG_CAPACITY = 256;

// Graph-layer version stored in the memfile header version field
const GRAPH_VERSION_V1 = 1;            // original 64-byte entity records
const GRAPH_VERSION_V2 = 2;            // 72-byte entity records with f64 psi
export const CURRENT_GRAPH_VERSION = GRAPH_VERSION_V2;

// Direction flags
export const DIR_FORWARD = 0n;
export const DIR_BACKWARD = 1n;
export const DIR_BIDIR = 2n;
const DIR_MASK = 3n;
const OFFSET_SHIFT = 2n;

// Entity record field offsets
const E_NAME_ID = 0;
const E_TYPE_ID = 4;
const E_ADJ_OFFSET = 8;
const E_MTIME = 16;
const E_OBS_MTIME = 24;
const E_OBS_COUNT = 32;
// 3 bytes pad at 33
const E_OBS0_ID = 36;
const E_OBS1_ID = 40;
// 4 bytes pad at 44
const E_STRUCTURAL_VISITS = 48;  // u64: 48..55, 8-aligned
const E_WALKER_VISITS = 56;      // u64: 56..63, 8-aligned
const E_PSI = 64;               // f64: 64..71, 8-aligned (MERW eigenvector component)
// total = 72

// AdjEntry field offsets (within each entry)
const AE_TARGET_DIR = 0;
const AE_RELTYPE_ID = 8;
// 4 bytes pad at 12
const AE_MTIME = 16;

// Graph header field offsets
const GH_NODE_LOG_OFFSET = 0;
const GH_STRUCTURAL_TOTAL = 8;
const GH_WALKER_TOTAL = 16;

// --- Encoding helpers ---

export function packTargetDir(targetOffset: bigint, direction: bigint): bigint {
  return (targetOffset << OFFSET_SHIFT) | (direction & DIR_MASK);
}

export function unpackTarget(packed: bigint): bigint {
  return packed >> OFFSET_SHIFT;
}

export function unpackDir(packed: bigint): bigint {
  return packed & DIR_MASK;
}

// --- Deserialized types ---

export interface EntityRecord {
  offset: bigint;         // where this record lives in graph.mem
  nameId: number;         // string table ID
  typeId: number;         // string table ID
  adjOffset: bigint;      // 0 = no adjacencies
  mtime: bigint;
  obsMtime: bigint;
  obsCount: number;
  obs0Id: number;         // 0 = empty
  obs1Id: number;         // 0 = empty
  structuralVisits: bigint;  // structural PageRank visit count
  walkerVisits: bigint;      // walker PageRank visit count
  psi: number;               // MERW dominant eigenvector component
}

export interface AdjEntry {
  targetOffset: bigint;   // offset of target entity record
  direction: bigint;      // DIR_FORWARD, DIR_BACKWARD, DIR_BIDIR
  relTypeId: number;      // string table ID
  mtime: bigint;
}

// --- Read/write functions ---

export function readEntityRecord(mf: MemoryFile, offset: bigint): EntityRecord {
  const buf = mf.read(offset, BigInt(ENTITY_RECORD_SIZE));
  return {
    offset,
    nameId: buf.readUInt32LE(E_NAME_ID),
    typeId: buf.readUInt32LE(E_TYPE_ID),
    adjOffset: buf.readBigUInt64LE(E_ADJ_OFFSET),
    mtime: buf.readBigUInt64LE(E_MTIME),
    obsMtime: buf.readBigUInt64LE(E_OBS_MTIME),
    obsCount: buf.readUInt8(E_OBS_COUNT),
    obs0Id: buf.readUInt32LE(E_OBS0_ID),
    obs1Id: buf.readUInt32LE(E_OBS1_ID),
    structuralVisits: buf.readBigUInt64LE(E_STRUCTURAL_VISITS),
    walkerVisits: buf.readBigUInt64LE(E_WALKER_VISITS),
    psi: buf.readDoubleLE(E_PSI),
  };
}

export function writeEntityRecord(mf: MemoryFile, rec: EntityRecord): void {
  const buf = Buffer.alloc(ENTITY_RECORD_SIZE);
  buf.writeUInt32LE(rec.nameId, E_NAME_ID);
  buf.writeUInt32LE(rec.typeId, E_TYPE_ID);
  buf.writeBigUInt64LE(rec.adjOffset, E_ADJ_OFFSET);
  buf.writeBigUInt64LE(rec.mtime, E_MTIME);
  buf.writeBigUInt64LE(rec.obsMtime, E_OBS_MTIME);
  buf.writeUInt8(rec.obsCount, E_OBS_COUNT);
  buf.writeUInt32LE(rec.obs0Id, E_OBS0_ID);
  buf.writeUInt32LE(rec.obs1Id, E_OBS1_ID);
  buf.writeBigUInt64LE(rec.structuralVisits, E_STRUCTURAL_VISITS);
  buf.writeBigUInt64LE(rec.walkerVisits, E_WALKER_VISITS);
  buf.writeDoubleLE(rec.psi, E_PSI);
  mf.write(rec.offset, buf);
}

export function readAdjBlock(mf: MemoryFile, adjOffset: bigint): { count: number; capacity: number; entries: AdjEntry[] } {
  const header = mf.read(adjOffset, BigInt(ADJ_HEADER_SIZE));
  const count = header.readUInt32LE(0);
  const capacity = header.readUInt32LE(4);

  const entries: AdjEntry[] = [];
  if (count > 0) {
    const dataSize = count * ADJ_ENTRY_SIZE;
    const data = mf.read(adjOffset + BigInt(ADJ_HEADER_SIZE), BigInt(dataSize));
    for (let i = 0; i < count; i++) {
      const base = i * ADJ_ENTRY_SIZE;
      const packed = data.readBigUInt64LE(base + AE_TARGET_DIR);
      entries.push({
        targetOffset: unpackTarget(packed),
        direction: unpackDir(packed),
        relTypeId: data.readUInt32LE(base + AE_RELTYPE_ID),
        mtime: data.readBigUInt64LE(base + AE_MTIME),
      });
    }
  }
  return { count, capacity, entries };
}

function writeAdjEntry(buf: Buffer, offset: number, entry: AdjEntry): void {
  buf.writeBigUInt64LE(packTargetDir(entry.targetOffset, entry.direction), offset + AE_TARGET_DIR);
  buf.writeUInt32LE(entry.relTypeId, offset + AE_RELTYPE_ID);
  buf.writeUInt32LE(0, offset + 12);  // pad
  buf.writeBigUInt64LE(entry.mtime, offset + AE_MTIME);
}

// --- NodeLog ---

export function readNodeLog(mf: MemoryFile, logOffset: bigint): { count: number; capacity: number; offsets: bigint[] } {
  const header = mf.read(logOffset, BigInt(NODE_LOG_HEADER_SIZE));
  const count = header.readUInt32LE(0);
  const capacity = header.readUInt32LE(4);

  const offsets: bigint[] = [];
  if (count > 0) {
    const data = mf.read(logOffset + BigInt(NODE_LOG_HEADER_SIZE), BigInt(count * 8));
    for (let i = 0; i < count; i++) {
      offsets.push(data.readBigUInt64LE(i * 8));
    }
  }
  return { count, capacity, offsets };
}

// --- GraphFile: high-level operations ---

export class GraphFile {
  private mf: MemoryFile;
  private st: StringTable;
  private graphHeaderOffset: bigint;

  constructor(graphPath: string, stringTable: StringTable, initialSize: number = 65536) {
    this.mf = new MemoryFile(graphPath, initialSize);
    this.st = stringTable;

    const stats = this.mf.stats();
    if (stats.allocated <= 32n) {
      // Fresh DB — initialize with current version
      this.graphHeaderOffset = this.initGraphHeader();
      this.mf.setVersion(CURRENT_GRAPH_VERSION);
    } else {
      // Existing DB — check version and migrate if needed
      this.graphHeaderOffset = 40n;
      const version = this.mf.getVersion();
      if (version === GRAPH_VERSION_V1) {
        this.migrateV1toV2(graphPath);
      } else if (version !== CURRENT_GRAPH_VERSION) {
        throw new Error(`GraphFile: unknown version ${version} (expected ${GRAPH_VERSION_V1} or ${CURRENT_GRAPH_VERSION})`);
      }
    }
  }

  /**
   * Migrate a v1 graph file (64-byte entity records) to v2 (72-byte with psi).
   *
   * Strategy: read all entities and edges from the current (v1) file into memory,
   * close it, rename it to .v1, create a fresh file, write everything back with
   * the new layout.
   */
  private migrateV1toV2(graphPath: string): void {
    // 1. Read all entities and adjacency data from v1 layout
    const offsets = this.getAllEntityOffsets();
    const oldEntities: { rec: EntityRecord; edges: AdjEntry[] }[] = [];

    for (const offset of offsets) {
      // Read v1 entity (64 bytes) — the first 64 bytes match our struct minus psi
      const buf = this.mf.read(offset, BigInt(OLD_ENTITY_RECORD_SIZE));
      const rec: EntityRecord = {
        offset,
        nameId: buf.readUInt32LE(E_NAME_ID),
        typeId: buf.readUInt32LE(E_TYPE_ID),
        adjOffset: buf.readBigUInt64LE(E_ADJ_OFFSET),
        mtime: buf.readBigUInt64LE(E_MTIME),
        obsMtime: buf.readBigUInt64LE(E_OBS_MTIME),
        obsCount: buf.readUInt8(E_OBS_COUNT),
        obs0Id: buf.readUInt32LE(E_OBS0_ID),
        obs1Id: buf.readUInt32LE(E_OBS1_ID),
        structuralVisits: buf.readBigUInt64LE(E_STRUCTURAL_VISITS),
        walkerVisits: buf.readBigUInt64LE(E_WALKER_VISITS),
        psi: 0,
      };
      const edges = rec.adjOffset !== 0n ? readAdjBlock(this.mf, rec.adjOffset).entries : [];
      oldEntities.push({ rec, edges });
    }

    // 2. Read global counters
    const structuralTotal = this.getStructuralTotal();
    const walkerTotal = this.getWalkerTotal();

    // 3. Close old file, rename to .v1 backup
    this.mf.sync();
    this.mf.close();
    const backupPath = graphPath + '.v1';
    fs.renameSync(graphPath, backupPath);

    // 4. Create fresh v2 file
    this.mf = new MemoryFile(graphPath, 65536);
    this.mf.setVersion(CURRENT_GRAPH_VERSION);
    this.graphHeaderOffset = this.initGraphHeader();

    // 5. Write global counters
    const ghBuf = Buffer.alloc(8);
    ghBuf.writeBigUInt64LE(structuralTotal, 0);
    this.mf.write(this.graphHeaderOffset + BigInt(GH_STRUCTURAL_TOTAL), ghBuf);
    ghBuf.writeBigUInt64LE(walkerTotal, 0);
    this.mf.write(this.graphHeaderOffset + BigInt(GH_WALKER_TOTAL), ghBuf);

    // 6. Write all entities with new 72-byte layout, building offset remap
    const offsetMap = new Map<bigint, bigint>();  // old offset → new offset

    for (const { rec } of oldEntities) {
      const newOffset = this.mf.alloc(BigInt(ENTITY_RECORD_SIZE));
      if (newOffset === 0n) throw new Error('GraphFile migration: entity alloc failed');
      offsetMap.set(rec.offset, newOffset);

      const newRec: EntityRecord = { ...rec, offset: newOffset, adjOffset: 0n, psi: 0 };
      writeEntityRecord(this.mf, newRec);
      this.nodeLogAppend(newOffset);
    }

    // 7. Rebuild adjacency blocks with remapped target offsets
    for (const { rec, edges } of oldEntities) {
      const newOffset = offsetMap.get(rec.offset)!;
      for (const edge of edges) {
        const newTarget = offsetMap.get(edge.targetOffset);
        if (newTarget === undefined) continue;  // skip dangling refs
        this.addEdge(newOffset, {
          targetOffset: newTarget,
          direction: edge.direction,
          relTypeId: edge.relTypeId,
          mtime: edge.mtime,
        });
      }
    }

    this.mf.sync();
  }

  private initGraphHeader(): bigint {
    // Allocate graph header block
    const hdrOffset = this.mf.alloc(BigInt(GRAPH_HEADER_SIZE));
    if (hdrOffset === 0n) throw new Error('GraphFile: failed to allocate header');

    // Allocate initial node log
    const logSize = NODE_LOG_HEADER_SIZE + INITIAL_LOG_CAPACITY * 8;
    const logOffset = this.mf.alloc(BigInt(logSize));
    if (logOffset === 0n) throw new Error('GraphFile: failed to allocate node log');

    // Write node log header: count=0, capacity
    const logHeader = Buffer.alloc(NODE_LOG_HEADER_SIZE);
    logHeader.writeUInt32LE(0, 0);
    logHeader.writeUInt32LE(INITIAL_LOG_CAPACITY, 4);
    this.mf.write(logOffset, logHeader);

    // Write graph header: node_log_offset + global PageRank counters
    const hdr = Buffer.alloc(GRAPH_HEADER_SIZE);
    hdr.writeBigUInt64LE(logOffset, GH_NODE_LOG_OFFSET);
    hdr.writeBigUInt64LE(0n, GH_STRUCTURAL_TOTAL);
    hdr.writeBigUInt64LE(0n, GH_WALKER_TOTAL);
    this.mf.write(hdrOffset, hdr);

    return hdrOffset;
  }

  // --- Header access ---

  private getNodeLogOffset(): bigint {
    const buf = this.mf.read(this.graphHeaderOffset, BigInt(GRAPH_HEADER_SIZE));
    return buf.readBigUInt64LE(GH_NODE_LOG_OFFSET);
  }

  private setNodeLogOffset(offset: bigint): void {
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64LE(offset, 0);
    this.mf.write(this.graphHeaderOffset + BigInt(GH_NODE_LOG_OFFSET), buf);
  }

  // --- Entity CRUD ---

  createEntity(name: string, entityType: string, mtime: bigint, obsMtime?: bigint): EntityRecord {
    const nameId = Number(this.st.intern(name));
    const typeId = Number(this.st.intern(entityType));

    const offset = this.mf.alloc(BigInt(ENTITY_RECORD_SIZE));
    if (offset === 0n) throw new Error('GraphFile: entity alloc failed');

    const rec: EntityRecord = {
      offset,
      nameId,
      typeId,
      adjOffset: 0n,
      mtime,
      obsMtime: obsMtime ?? mtime,
      obsCount: 0,
      obs0Id: 0,
      obs1Id: 0,
      structuralVisits: 0n,
      walkerVisits: 0n,
      psi: 0,
    };
    writeEntityRecord(this.mf, rec);
    this.nodeLogAppend(offset);
    return rec;
  }

  readEntity(offset: bigint): EntityRecord {
    return readEntityRecord(this.mf, offset);
  }

  updateEntity(rec: EntityRecord): void {
    writeEntityRecord(this.mf, rec);
  }

  deleteEntity(offset: bigint): void {
    const rec = readEntityRecord(this.mf, offset);

    // Release string table refs
    this.st.release(BigInt(rec.nameId));
    this.st.release(BigInt(rec.typeId));
    if (rec.obs0Id !== 0) this.st.release(BigInt(rec.obs0Id));
    if (rec.obs1Id !== 0) this.st.release(BigInt(rec.obs1Id));

    // Free adj block if present
    if (rec.adjOffset !== 0n) {
      this.mf.free(rec.adjOffset);
    }

    // Remove from node log
    this.nodeLogRemove(offset);

    // Free entity record
    this.mf.free(offset);
  }

  // --- Observation management ---

  addObservation(entityOffset: bigint, observation: string, mtime: bigint): void {
    const rec = readEntityRecord(this.mf, entityOffset);
    if (rec.obsCount >= 2) throw new Error('Entity already has max observations');

    const obsId = Number(this.st.intern(observation));

    if (rec.obsCount === 0) {
      rec.obs0Id = obsId;
    } else {
      rec.obs1Id = obsId;
    }
    rec.obsCount++;
    rec.obsMtime = mtime;
    rec.mtime = mtime;
    writeEntityRecord(this.mf, rec);
  }

  removeObservation(entityOffset: bigint, observation: string, mtime: bigint): boolean {
    const rec = readEntityRecord(this.mf, entityOffset);

    // Find the observation by matching the string
    const obs0 = rec.obs0Id !== 0 ? this.st.get(BigInt(rec.obs0Id)) : null;
    const obs1 = rec.obs1Id !== 0 ? this.st.get(BigInt(rec.obs1Id)) : null;

    if (obs0 === observation) {
      this.st.release(BigInt(rec.obs0Id));
      // Shift obs1 into slot 0 if present
      rec.obs0Id = rec.obs1Id;
      rec.obs1Id = 0;
      rec.obsCount--;
      rec.obsMtime = mtime;
      rec.mtime = mtime;
      writeEntityRecord(this.mf, rec);
      return true;
    } else if (obs1 === observation) {
      this.st.release(BigInt(rec.obs1Id));
      rec.obs1Id = 0;
      rec.obsCount--;
      rec.obsMtime = mtime;
      rec.mtime = mtime;
      writeEntityRecord(this.mf, rec);
      return true;
    }
    return false;
  }

  // --- Adjacency management ---

  addEdge(entityOffset: bigint, entry: AdjEntry): void {
    const rec = readEntityRecord(this.mf, entityOffset);

    if (rec.adjOffset === 0n) {
      // No adj block yet — allocate one
      const adjSize = ADJ_HEADER_SIZE + INITIAL_ADJ_CAPACITY * ADJ_ENTRY_SIZE;
      const adjOffset = this.mf.alloc(BigInt(adjSize));
      if (adjOffset === 0n) throw new Error('GraphFile: adj alloc failed');

      // Write header: count=1, capacity
      const header = Buffer.alloc(ADJ_HEADER_SIZE);
      header.writeUInt32LE(1, 0);
      header.writeUInt32LE(INITIAL_ADJ_CAPACITY, 4);
      this.mf.write(adjOffset, header);

      // Write entry
      const entryBuf = Buffer.alloc(ADJ_ENTRY_SIZE);
      writeAdjEntry(entryBuf, 0, entry);
      this.mf.write(adjOffset + BigInt(ADJ_HEADER_SIZE), entryBuf);

      // Update entity record
      rec.adjOffset = adjOffset;
      writeEntityRecord(this.mf, rec);
    } else {
      const adj = readAdjBlock(this.mf, rec.adjOffset);

      if (adj.count < adj.capacity) {
        // Append in place
        const entryBuf = Buffer.alloc(ADJ_ENTRY_SIZE);
        writeAdjEntry(entryBuf, 0, entry);
        const entryPos = rec.adjOffset + BigInt(ADJ_HEADER_SIZE + adj.count * ADJ_ENTRY_SIZE);
        this.mf.write(entryPos, entryBuf);

        // Bump count
        const countBuf = Buffer.alloc(4);
        countBuf.writeUInt32LE(adj.count + 1, 0);
        this.mf.write(rec.adjOffset, countBuf);
      } else {
        // Need to grow — allocate new block with double capacity
        const newCapacity = adj.capacity * 2;
        const newSize = ADJ_HEADER_SIZE + newCapacity * ADJ_ENTRY_SIZE;
        const newOffset = this.mf.alloc(BigInt(newSize));
        if (newOffset === 0n) throw new Error('GraphFile: adj grow failed');

        // Write new header
        const header = Buffer.alloc(ADJ_HEADER_SIZE);
        header.writeUInt32LE(adj.count + 1, 0);
        header.writeUInt32LE(newCapacity, 4);
        this.mf.write(newOffset, header);

        // Copy existing entries
        if (adj.count > 0) {
          const existing = this.mf.read(
            rec.adjOffset + BigInt(ADJ_HEADER_SIZE),
            BigInt(adj.count * ADJ_ENTRY_SIZE)
          );
          this.mf.write(newOffset + BigInt(ADJ_HEADER_SIZE), existing);
        }

        // Append new entry
        const entryBuf = Buffer.alloc(ADJ_ENTRY_SIZE);
        writeAdjEntry(entryBuf, 0, entry);
        this.mf.write(
          newOffset + BigInt(ADJ_HEADER_SIZE + adj.count * ADJ_ENTRY_SIZE),
          entryBuf
        );

        // Free old block, update entity
        this.mf.free(rec.adjOffset);
        rec.adjOffset = newOffset;
        writeEntityRecord(this.mf, rec);
      }
    }
  }

  removeEdge(entityOffset: bigint, targetOffset: bigint, relTypeId: number, direction: bigint): boolean {
    const rec = readEntityRecord(this.mf, entityOffset);
    if (rec.adjOffset === 0n) return false;

    const adj = readAdjBlock(this.mf, rec.adjOffset);
    const packed = packTargetDir(targetOffset, direction);

    for (let i = 0; i < adj.count; i++) {
      const e = adj.entries[i];
      const ePacked = packTargetDir(e.targetOffset, e.direction);
      if (ePacked === packed && e.relTypeId === relTypeId) {
        // Found — swap with last entry and decrement count
        if (i < adj.count - 1) {
          // Read last entry and write over this slot
          const lastPos = rec.adjOffset + BigInt(ADJ_HEADER_SIZE + (adj.count - 1) * ADJ_ENTRY_SIZE);
          const lastBuf = this.mf.read(lastPos, BigInt(ADJ_ENTRY_SIZE));
          const slotPos = rec.adjOffset + BigInt(ADJ_HEADER_SIZE + i * ADJ_ENTRY_SIZE);
          this.mf.write(slotPos, lastBuf);
        }

        // Decrement count
        const countBuf = Buffer.alloc(4);
        countBuf.writeUInt32LE(adj.count - 1, 0);
        this.mf.write(rec.adjOffset, countBuf);

        return true;
      }
    }
    return false;
  }

  getEdges(entityOffset: bigint): AdjEntry[] {
    const rec = readEntityRecord(this.mf, entityOffset);
    if (rec.adjOffset === 0n) return [];
    return readAdjBlock(this.mf, rec.adjOffset).entries;
  }

  // --- Node log ---

  private nodeLogAppend(entityOffset: bigint): void {
    const logOffset = this.getNodeLogOffset();
    const header = this.mf.read(logOffset, BigInt(NODE_LOG_HEADER_SIZE));
    const count = header.readUInt32LE(0);
    const capacity = header.readUInt32LE(4);

    if (count < capacity) {
      // Append in place
      const pos = logOffset + BigInt(NODE_LOG_HEADER_SIZE + count * 8);
      const buf = Buffer.alloc(8);
      buf.writeBigUInt64LE(entityOffset, 0);
      this.mf.write(pos, buf);

      const countBuf = Buffer.alloc(4);
      countBuf.writeUInt32LE(count + 1, 0);
      this.mf.write(logOffset, countBuf);
    } else {
      // Grow: allocate new log with double capacity
      const newCapacity = capacity * 2;
      const newSize = NODE_LOG_HEADER_SIZE + newCapacity * 8;
      const newLogOffset = this.mf.alloc(BigInt(newSize));
      if (newLogOffset === 0n) throw new Error('GraphFile: node log grow failed');

      // Write new header
      const newHeader = Buffer.alloc(NODE_LOG_HEADER_SIZE);
      newHeader.writeUInt32LE(count + 1, 0);
      newHeader.writeUInt32LE(newCapacity, 4);
      this.mf.write(newLogOffset, newHeader);

      // Copy existing entries
      if (count > 0) {
        const existing = this.mf.read(
          logOffset + BigInt(NODE_LOG_HEADER_SIZE),
          BigInt(count * 8)
        );
        this.mf.write(newLogOffset + BigInt(NODE_LOG_HEADER_SIZE), existing);
      }

      // Append new entry
      const entryBuf = Buffer.alloc(8);
      entryBuf.writeBigUInt64LE(entityOffset, 0);
      this.mf.write(newLogOffset + BigInt(NODE_LOG_HEADER_SIZE + count * 8), entryBuf);

      // Free old, update header
      this.mf.free(logOffset);
      this.setNodeLogOffset(newLogOffset);
    }
  }

  private nodeLogRemove(entityOffset: bigint): void {
    const logOffset = this.getNodeLogOffset();
    const log = readNodeLog(this.mf, logOffset);

    const idx = log.offsets.indexOf(entityOffset);
    if (idx === -1) return;

    const lastIdx = log.count - 1;
    if (idx < lastIdx) {
      // Swap with last
      const lastPos = logOffset + BigInt(NODE_LOG_HEADER_SIZE + lastIdx * 8);
      const lastBuf = this.mf.read(lastPos, 8n);
      const slotPos = logOffset + BigInt(NODE_LOG_HEADER_SIZE + idx * 8);
      this.mf.write(slotPos, lastBuf);
    }

    // Decrement count
    const countBuf = Buffer.alloc(4);
    countBuf.writeUInt32LE(log.count - 1, 0);
    this.mf.write(logOffset, countBuf);
  }

  // --- Scan all nodes ---

  getAllEntityOffsets(): bigint[] {
    const logOffset = this.getNodeLogOffset();
    return readNodeLog(this.mf, logOffset).offsets;
  }

  getEntityCount(): number {
    const logOffset = this.getNodeLogOffset();
    const header = this.mf.read(logOffset, BigInt(NODE_LOG_HEADER_SIZE));
    return header.readUInt32LE(0);
  }

  // --- PageRank visit counts ---

  /** Read global structural visit total */
  getStructuralTotal(): bigint {
    const buf = this.mf.read(this.graphHeaderOffset + BigInt(GH_STRUCTURAL_TOTAL), 8n);
    return buf.readBigUInt64LE(0);
  }

  /** Read global walker visit total */
  getWalkerTotal(): bigint {
    const buf = this.mf.read(this.graphHeaderOffset + BigInt(GH_WALKER_TOTAL), 8n);
    return buf.readBigUInt64LE(0);
  }

  /** Increment structural visit count for one entity and bump the global counter. */
  incrementStructuralVisit(entityOffset: bigint): void {
    // Read current entity visit count
    const vbuf = this.mf.read(entityOffset + BigInt(E_STRUCTURAL_VISITS), 8n);
    const current = vbuf.readBigUInt64LE(0);
    const wbuf = Buffer.alloc(8);
    wbuf.writeBigUInt64LE(current + 1n, 0);
    this.mf.write(entityOffset + BigInt(E_STRUCTURAL_VISITS), wbuf);

    // Bump global counter
    const gbuf = this.mf.read(this.graphHeaderOffset + BigInt(GH_STRUCTURAL_TOTAL), 8n);
    const total = gbuf.readBigUInt64LE(0);
    const gwbuf = Buffer.alloc(8);
    gwbuf.writeBigUInt64LE(total + 1n, 0);
    this.mf.write(this.graphHeaderOffset + BigInt(GH_STRUCTURAL_TOTAL), gwbuf);
  }

  /** Increment walker visit count for one entity and bump the global counter. */
  incrementWalkerVisit(entityOffset: bigint): void {
    const vbuf = this.mf.read(entityOffset + BigInt(E_WALKER_VISITS), 8n);
    const current = vbuf.readBigUInt64LE(0);
    const wbuf = Buffer.alloc(8);
    wbuf.writeBigUInt64LE(current + 1n, 0);
    this.mf.write(entityOffset + BigInt(E_WALKER_VISITS), wbuf);

    const gbuf = this.mf.read(this.graphHeaderOffset + BigInt(GH_WALKER_TOTAL), 8n);
    const total = gbuf.readBigUInt64LE(0);
    const gwbuf = Buffer.alloc(8);
    gwbuf.writeBigUInt64LE(total + 1n, 0);
    this.mf.write(this.graphHeaderOffset + BigInt(GH_WALKER_TOTAL), gwbuf);
  }

  /**
   * Get the structural PageRank score for an entity.
   * Returns structuralVisits / structuralTotal, or 0 if no visits yet.
   */
  getStructuralRank(entityOffset: bigint): number {
    const total = this.getStructuralTotal();
    if (total === 0n) return 0;
    const rec = this.readEntity(entityOffset);
    return Number(rec.structuralVisits) / Number(total);
  }

  /**
   * Get the walker PageRank score for an entity.
   * Returns walkerVisits / walkerTotal, or 0 if no visits yet.
   */
  getWalkerRank(entityOffset: bigint): number {
    const total = this.getWalkerTotal();
    if (total === 0n) return 0;
    const rec = this.readEntity(entityOffset);
    return Number(rec.walkerVisits) / Number(total);
  }

  // --- MERW eigenvector ---

  /** Write the psi (MERW eigenvector component) for an entity. */
  setPsi(entityOffset: bigint, psi: number): void {
    const buf = Buffer.alloc(8);
    buf.writeDoubleLE(psi, 0);
    this.mf.write(entityOffset + BigInt(E_PSI), buf);
  }

  /** Read the psi (MERW eigenvector component) for an entity. */
  getPsi(entityOffset: bigint): number {
    const buf = this.mf.read(entityOffset + BigInt(E_PSI), 8n);
    return buf.readDoubleLE(0);
  }

  // --- Lifecycle & Concurrency ---

  /** Acquire a shared (read) lock on the graph file. */
  lockShared(): void {
    this.mf.lockShared();
  }

  /** Acquire an exclusive (write) lock on the graph file. */
  lockExclusive(): void {
    this.mf.lockExclusive();
  }

  /** Release the lock on the graph file. */
  unlock(): void {
    this.mf.unlock();
  }

  /** Refresh the mmap if the file was grown by another process. */
  refresh(): void {
    this.mf.refresh();
  }

  sync(): void {
    this.mf.sync();
  }

  close(): void {
    this.mf.close();
  }
}
