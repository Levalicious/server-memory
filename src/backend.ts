/**
 * backend.ts — the storage backend abstraction the manager runs on.
 *
 * EmbeddedBackend wraps the in-process C store (default). DaemonStore speaks
 * the kbd4 wire protocol. Selection is environment-driven (createBackend):
 *   KB_DAEMON_SPAWN=1   → spawn a local kbd4 for the store dir (tests/dev)
 *   KB_DAEMON_ADDR=h:p  → connect to a running owner daemon, with
 *                         KB_DAEMON_TOKEN or KB_DAEMON_TOKEN_FILE
 *   (neither)           → embedded store (ensureV3 + the N-API addon)
 */
import path from 'path';
import fs from 'fs';
import { Store, type Direction } from './store.js';
import { ensureV3Locked, withMigrationLock } from './migrate.js';
import { DaemonClient } from './daemon_client.js';
import { DaemonStore } from './daemon_store.js';

export interface BackendEntity {
  name: string;
  type: string;
  observations: string[];
  mtime: bigint;
  obsMtime: bigint;
}

export interface BackendEdge {
  target: bigint;
  direction: number; // DIR_FORWARD | DIR_BACKWARD
  relType: string;
  mtime: bigint;
}

export interface BackendRanks {
  structural: Map<string, number>;
  walker: Map<string, number>;
  psi: Map<string, number>;
  structuralTotal: bigint;
  walkerTotal: bigint;
}

export interface BackendValidation {
  missing: string[];
  violations: { entity: string; count: number; oversizedObservations: number[] }[];
}

export interface ScanRow {
  eid?: bigint;
  name: string;
  type: string;
  observations: string[];
}

export interface GraphBackend {
  lookup(name: string): Promise<bigint>;
  createEntity(name: string, type: string, mtime: bigint): Promise<bigint>;
  deleteEntity(h: bigint): Promise<void>;
  readEntity(h: bigint): Promise<BackendEntity>;
  entityName(h: bigint): Promise<string>;
  addObservation(h: bigint, text: string, mtime: bigint): Promise<void>;
  removeObservation(h: bigint, text: string, mtime: bigint): Promise<void>;
  createRelation(from: bigint, to: bigint, relType: string, mtime: bigint): Promise<void>;
  deleteRelation(from: bigint, to: bigint, relType: string): Promise<void>;
  edges(h: bigint): Promise<BackendEdge[]>;
  neighbors(start: bigint, hops: number, direction: Direction): Promise<bigint[]>;
  findPath(from: bigint, to: bigint, maxDepth: number, direction: Direction, budgetBytes: bigint): Promise<{
    path: bigint[];
    targetReached: boolean;
    budgetExhausted: boolean;
    farthest: bigint;
  }>;
  search(pattern: string): Promise<bigint[]>;
  regexValid(pattern: string): Promise<boolean>;
  entitiesByType(type: string): Promise<bigint[]>;
  orphaned(): Promise<bigint[]>;
  entityTypes(): Promise<string[]>;
  relationTypes(): Promise<string[]>;
  entityCount(): Promise<number>;
  relationCount(): Promise<number>;
  ranksFor(names: string[]): Promise<BackendRanks>;
  resample(): Promise<void>;
  validate(): Promise<BackendValidation>;
  scanAll(): AsyncGenerator<ScanRow>;
  listEntities(): Promise<bigint[]>;
  randomWalk(start: bigint, depth: number, direction: Direction, merwMode: boolean, seed: bigint, avoidCycles: boolean): Promise<{ path: bigint[]; uniformSteps: number }>;
  incWalkerVisit(h: bigint): Promise<void>;
  incStructuralVisit(h: bigint): Promise<void>;
  close(): Promise<void>;
  lockShared(): Promise<void>;
  lockExclusive(): Promise<void>;
  unlock(): Promise<void>;
  refresh(): Promise<void>;
  sync(): Promise<void>;
}

/** Async facade over the in-process N-API store. */
export class EmbeddedBackend implements GraphBackend {
  constructor(private readonly store: Store) {}

  async lookup(name: string): Promise<bigint> { return this.store.lookup(name); }
  async createEntity(name: string, type: string, mtime: bigint): Promise<bigint> { return this.store.createEntity(name, type, mtime); }
  async deleteEntity(h: bigint): Promise<void> { this.store.deleteEntity(h); }
  async readEntity(h: bigint): Promise<BackendEntity> {
    const r = this.store.readEntity(h);
    return { name: r.name, type: r.type, observations: r.observations, mtime: r.mtime, obsMtime: r.obsMtime };
  }
  async entityName(h: bigint): Promise<string> { return this.store.entityName(h); }
  async addObservation(h: bigint, text: string, mtime: bigint): Promise<void> { this.store.addObservation(h, text, mtime); }
  async removeObservation(h: bigint, text: string, mtime: bigint): Promise<void> { this.store.removeObservation(h, text, mtime); }
  async createRelation(from: bigint, to: bigint, relType: string, mtime: bigint): Promise<void> { this.store.createRelation(from, to, relType, mtime); }
  async deleteRelation(from: bigint, to: bigint, relType: string): Promise<void> { this.store.deleteRelation(from, to, relType); }
  async edges(h: bigint): Promise<BackendEdge[]> { return this.store.edges(h); }
  async neighbors(start: bigint, hops: number, direction: Direction): Promise<bigint[]> { return this.store.neighbors(start, hops, direction); }
  async findPath(from: bigint, to: bigint, maxDepth: number, direction: Direction, budgetBytes: bigint): Promise<{ path: bigint[]; targetReached: boolean; budgetExhausted: boolean; farthest: bigint }> {
    return this.store.findPath(from, to, maxDepth, direction, budgetBytes);
  }
  async search(pattern: string): Promise<bigint[]> { return this.store.search(pattern); }
  async regexValid(pattern: string): Promise<boolean> { return this.store.regexValid(pattern); }
  async entitiesByType(type: string): Promise<bigint[]> { return this.store.entitiesByType(type); }
  async orphaned(): Promise<bigint[]> { return this.store.orphaned(); }
  async entityTypes(): Promise<string[]> { return this.store.entityTypes(); }
  async relationTypes(): Promise<string[]> { return this.store.relationTypes(); }
  async entityCount(): Promise<number> { return this.store.entityCount(); }
  async relationCount(): Promise<number> { return this.store.relationCount(); }

  async ranksFor(names: string[]): Promise<BackendRanks> {
    const structuralTotal = this.store.structuralTotal();
    const walkerTotal = this.store.walkerTotal();
    const structural = new Map<string, number>();
    const walker = new Map<string, number>();
    const psi = new Map<string, number>();
    for (const n of names) {
      const off = this.store.lookup(n);
      if (off === 0n) { structural.set(n, 0); walker.set(n, 0); psi.set(n, 0); continue; }
      const rec = this.store.readEntity(off);
      structural.set(n, structuralTotal > 0n ? Number(rec.structuralVisits) / Number(structuralTotal) : 0);
      walker.set(n, walkerTotal > 0n ? Number(rec.walkerVisits) / Number(walkerTotal) : 0);
      psi.set(n, rec.psi);
    }
    return { structural, walker, psi, structuralTotal, walkerTotal };
  }

  async resample(): Promise<void> {
    this.store.structuralSample(1, 0.85);
    this.store.computeMerwPsi(0.85, 200, 1e-8);
  }

  async validate(): Promise<BackendValidation> {
    const missing = new Set<string>();
    for (const d of this.store.validateDangling()) {
      try { missing.add(this.store.entityName(d.src)); } catch { /* offset already gone */ }
      try { missing.add(this.store.entityName(d.target)); } catch { /* dangling target */ }
    }
    const violations = this.store.validateObs().map((v) => {
      const oversizedObservations: number[] = [];
      if (v.oversize & 1) oversizedObservations.push(0);
      if (v.oversize & 2) oversizedObservations.push(1);
      return { entity: this.store.entityName(v.offset), count: v.count, oversizedObservations };
    });
    return { missing: Array.from(missing), violations };
  }

  async *scanAll(): AsyncGenerator<ScanRow> {
    for (const off of this.store.listEntities()) {
      const e = this.store.readEntity(off);
      yield { eid: off, name: e.name, type: e.type, observations: e.observations };
    }
  }

  async listEntities(): Promise<bigint[]> { return this.store.listEntities(); }
  async randomWalk(start: bigint, depth: number, direction: Direction, merwMode: boolean, seed: bigint, avoidCycles: boolean): Promise<{ path: bigint[]; uniformSteps: number }> {
    return this.store.randomWalk(start, depth, direction, merwMode, seed, avoidCycles);
  }
  async incWalkerVisit(h: bigint): Promise<void> { this.store.incWalkerVisit(h); }
  async incStructuralVisit(h: bigint): Promise<void> { this.store.incStructuralVisit(h); }
  async close(): Promise<void> { this.store.close(); }
  async lockShared(): Promise<void> { this.store.lockShared(); }
  async lockExclusive(): Promise<void> { this.store.lockExclusive(); }
  async unlock(): Promise<void> { this.store.unlock(); }
  async refresh(): Promise<void> { this.store.refresh(); }
  async sync(): Promise<void> { this.store.sync(); }
}

/** Pick the backend from the environment (see module comment). */
export function createBackend(memoryFilePath: string): GraphBackend {
  const dir = path.dirname(memoryFilePath);
  const base = path.basename(memoryFilePath, path.extname(memoryFilePath));
  const graphPath = path.join(dir, `${base}.graph`);
  const strPath = path.join(dir, `${base}.strings`);

  if (process.env.KB_DAEMON_SPAWN === '1') {
    return new DaemonStore(DaemonClient.spawn({ storeDir: dir }));
  }
  const addr = process.env.KB_DAEMON_ADDR;
  if (addr) {
    const i = addr.lastIndexOf(':');
    const host = addr.slice(0, i);
    const port = Number(addr.slice(i + 1));
    let token = process.env.KB_DAEMON_TOKEN ?? '';
    if (!token && process.env.KB_DAEMON_TOKEN_FILE) {
      token = fs.readFileSync(process.env.KB_DAEMON_TOKEN_FILE, 'utf8').replace(/[\r\n]+$/, '');
    }
    if (!token) throw new Error('KB_DAEMON_ADDR set but neither KB_DAEMON_TOKEN nor KB_DAEMON_TOKEN_FILE is available');
    return new DaemonStore(DaemonClient.connect(host, port, token));
  }

  // Hold the migration lock across BOTH the ensure and the store open: the
  // native create path initializes a fresh store in place (file header +
  // allocator cursor), so two processes opening a not-yet-created KB
  // concurrently would interleave those init writes and corrupt entity
  // records (observed as SIGSEGV crashes in the 30-agent concurrency fuzz).
  // The first process creates; the rest wait, then open the finished store.
  const store = withMigrationLock(graphPath, () => {
    ensureV3Locked(graphPath, strPath);
    return new Store(graphPath, strPath);
  });
  return new EmbeddedBackend(store);
}
