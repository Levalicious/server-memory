/**
 * daemon_store.ts — Store-shaped async backend over the kbd4 wire client.
 *
 * The daemon speaks NAMES (and returns name lists); this adapter presents the
 * offset-flavoured surface `server.ts`'s KnowledgeGraphManager expects by
 * keeping a per-client handle table: handle ↔ name (handles are client-local
 * bigints; 0n means absent, matching the embedded store's convention).
 *
 * Consistency note: each call is its own daemon transaction, so this adapter
 * offers per-call freshness, not cross-call snapshots. Reads never cache.
 * (The embedded store gets its snapshot property from the server-held flock;
 * under the single writer we take per-op atomicity instead.)
 */
import { DaemonClient, OP, R, W, ST_OK } from './daemon_client.js';
import { DIR_FORWARD, DIR_BACKWARD, type Direction } from './store.js';

export interface DaemonEntity {
  name: string;
  type: string;
  observations: string[];
  mtime: bigint;
  obsMtime: bigint;
}

export interface DaemonEdge {
  target: bigint;
  direction: number; // DIR_FORWARD | DIR_BACKWARD
  relType: string;
  mtime: bigint;
}

export interface DaemonRanks {
  structural: Map<string, number>;
  walker: Map<string, number>;
  psi: Map<string, number>;
  structuralTotal: bigint;
  walkerTotal: bigint;
}

/** Results-paging chunk size (frames are capped at 1 MiB by the daemon). */
const FETCH_CHUNK = 10_000;
const SCAN_CHUNK = 4096;

function dirCode(d: Direction): 0 | 1 | 2 {
  return d === 'forward' ? 0 : d === 'backward' ? 1 : 2;
}

export class DaemonStore {
  private readonly client: DaemonClient;
  private nextHandle = 1n;
  private byName = new Map<string, bigint>();
  private byHandle = new Map<bigint, string>();

  constructor(client: DaemonClient) {
    this.client = client;
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  // ------------------------------------------------------------- handles

  private handle(name: string): bigint {
    let h = this.byName.get(name);
    if (h === undefined) {
      h = this.nextHandle++;
      this.byName.set(name, h);
      this.byHandle.set(h, name);
    }
    return h;
  }

  private nameOf(h: bigint): string {
    const n = this.byHandle.get(h);
    if (n === undefined) throw new Error(`daemon store: unknown entity handle ${h}`);
    return n;
  }

  // ------------------------------------------------------------- blobs

  private async openBlob(name: string): Promise<{ entity: DaemonEntity; edges: DaemonEdge[] } | null> {
    const body = await this.client.callOk(OP.OPEN_NODES, (w) => { w.u32(1); w.str(name); });
    const r = new R(body);
    if (r.u8() === 0) return null;
    const entity: DaemonEntity = {
      name: r.strText(),
      type: r.strText(),
      mtime: r.u64(),
      obsMtime: r.u64(),
      observations: [],
    };
    const oc = r.u8();
    for (let i = 0; i < oc; i++) entity.observations.push(r.strText());
    const edges: DaemonEdge[] = [];
    const ec = r.u32();
    for (let i = 0; i < ec; i++) {
      const relType = r.strText();
      const wireDirection = r.u8();
      const target = this.handle(r.strText());
      const mtime = r.u64();
      edges.push({ target, direction: wireDirection === 0 ? DIR_FORWARD : DIR_BACKWARD, relType, mtime });
    }
    return { entity, edges };
  }

  // ------------------------------------------------------------- lookups / reads

  async lookup(name: string): Promise<bigint> {
    const blob = await this.openBlob(name);
    return blob ? this.handle(name) : 0n;
  }

  async readEntity(h: bigint): Promise<DaemonEntity> {
    const blob = await this.openBlob(this.nameOf(h));
    if (!blob) throw new Error(`daemon store: entity missing for handle ${h}`);
    return blob.entity;
  }

  async entityName(h: bigint): Promise<string> {
    return this.nameOf(h);
  }

  async edges(h: bigint): Promise<DaemonEdge[]> {
    const blob = await this.openBlob(this.nameOf(h));
    return blob ? blob.edges : [];
  }

  // ------------------------------------------------------------- writes

  async createEntity(name: string, type: string, mtime: bigint): Promise<bigint> {
    const body = await this.client.callOk(OP.CREATE_ENTITIES, (w) => {
      w.u32(1); w.str(name); w.str(type); w.u64(mtime);
    });
    const eid = new R(body).u32();
    if (!eid) throw new Error(`daemon store: create returned no eid for "${name}"`);
    return this.handle(name);
  }

  async deleteEntity(h: bigint): Promise<void> {
    await this.client.callOk(OP.DELETE_ENTITIES, (w) => { w.u32(1); w.str(this.nameOf(h)); });
  }

  async addObservation(h: bigint, text: string, mtime: bigint): Promise<void> {
    await this.client.callOk(OP.ADD_OBS, (w) => { w.u32(1); w.str(this.nameOf(h)); w.str(text); w.u64(mtime); });
  }

  async removeObservation(h: bigint, text: string, mtime: bigint): Promise<void> {
    await this.client.callOk(OP.DEL_OBS, (w) => { w.u32(1); w.str(this.nameOf(h)); w.str(text); w.u64(mtime); });
  }

  async createRelation(from: bigint, to: bigint, relType: string, mtime: bigint): Promise<void> {
    await this.client.callOk(OP.CREATE_RELATIONS, (w) => {
      w.u32(1); w.str(this.nameOf(from)); w.str(this.nameOf(to)); w.str(relType); w.u64(mtime);
    });
  }

  async deleteRelation(from: bigint, to: bigint, relType: string): Promise<void> {
    await this.client.callOk(OP.DELETE_RELATIONS, (w) => {
      w.u32(1); w.str(this.nameOf(from)); w.str(this.nameOf(to)); w.str(relType);
    });
  }

  // ------------------------------------------------------------- traversal

  /** `hops` counts hops from the start (wire depth = hops - 1, PUBLIC 0-indexed). */
  async neighbors(start: bigint, hops: number, direction: Direction): Promise<bigint[]> {
    const name = this.nameOf(start);
    const names = await this.fetchPaged(
      (skip, max) => this.client.callOk(OP.NEIGHBORS, (w) => {
        w.str(name); w.u32(Math.max(0, hops - 1)); w.u8(dirCode(direction)); w.u32(max); w.u32(skip);
      }),
    );
    return names.map((n) => this.handle(n));
  }

  async findPath(from: bigint, to: bigint, maxDepth: number, direction: Direction, _budgetBytes: bigint): Promise<{
    path: bigint[];
    targetReached: boolean;
    budgetExhausted: boolean;
    farthest: bigint;
  }> {
    const fromName = this.nameOf(from);
    const toName = this.nameOf(to);
    const body = await this.client.callOk(OP.FIND_PATH, (w) => {
      w.str(fromName); w.str(toName); w.u32(maxDepth); w.u8(dirCode(direction));
    });
    const r = new R(body);
    const n = r.u32();
    const names: string[] = [];
    for (let i = 0; i < n; i++) names.push(r.strText());
    const reached = n > 0 && names[names.length - 1] === toName;
    const farthest = !reached && names.length > 0 ? this.handle(names[names.length - 1]) : 0n;
    return {
      path: names.map((nm) => this.handle(nm)),
      targetReached: reached,
      budgetExhausted: false,   // the daemon BFS is not budget-capped in v1
      farthest,
    };
  }

  async randomWalk(start: bigint, depth: number, direction: Direction, merwMode: boolean, seed: bigint, avoidCycles: boolean): Promise<{ path: bigint[]; uniformSteps: number }> {
    const body = await this.client.callOk(OP.RANDOM_WALK, (w) => {
      w.str(this.nameOf(start)); w.u32(depth); w.u8(dirCode(direction)); w.u8(merwMode ? 1 : 0); w.u64(seed);
      w.u8(avoidCycles ? 1 : 0);
    });
    const r = new R(body);
    const n = r.u32();
    const names: string[] = [];
    for (let i = 0; i < n; i++) names.push(r.strText());
    const uniformSteps = r.u32();
    return { path: names.map((nm) => this.handle(nm)), uniformSteps };
  }

  // ------------------------------------------------------------- queries

  async search(pattern: string): Promise<bigint[]> {
    const names = await this.fetchPaged(
      (skip, max) => this.client.callOk(OP.SEARCH, (w) => { w.str(pattern); w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async regexValid(pattern: string): Promise<boolean> {
    const body = await this.client.callOk(OP.REGEX_VALID, (w) => w.str(pattern));
    return new R(body).u8() === 1;
  }

  async entitiesByType(type: string): Promise<bigint[]> {
    const names = await this.fetchPaged(
      (skip, max) => this.client.callOk(OP.BY_TYPE, (w) => { w.str(type); w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async orphaned(): Promise<bigint[]> {
    const names = await this.fetchPaged(
      (skip, max) => this.client.callOk(OP.ORPHANED, (w) => { w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async entityTypes(): Promise<string[]> {
    return this.fetchStrings(OP.ENTITY_TYPES);
  }

  async relationTypes(): Promise<string[]> {
    return this.fetchStrings(OP.RELATION_TYPES);
  }

  async entityCount(): Promise<number> {
    return (await this.client.stats()).entities;
  }

  async relationCount(): Promise<number> {
    return (await this.client.stats()).relations;
  }

  // ------------------------------------------------------------- ranks / resample

  /** Rank maps for a name list (totals included). */
  async ranksFor(names: string[]): Promise<DaemonRanks> {
    const body = await this.client.callOk(OP.RANKS, (w) => {
      w.u32(names.length);
      for (const n of names) w.str(n);
    });
    const r = new R(body);
    const structuralTotal = r.u64();
    const walkerTotal = r.u64();
    const structural = new Map<string, number>();
    const walker = new Map<string, number>();
    const psi = new Map<string, number>();
    for (const n of names) {
      walker.set(n, r.f64());
      structural.set(n, r.f64());
      psi.set(n, r.f64());
    }
    return { structural, walker, psi, structuralTotal, walkerTotal };
  }

  /** One-shot structural sample + MERW psi (post-mutation rank refresh). */
  async resample(): Promise<void> {
    await this.client.callOk(OP.RESAMPLE);
  }

  // ------------------------------------------------------------- validate

  async validate(): Promise<{
    missing: string[];
    violations: { entity: string; count: number; oversizedObservations: number[] }[];
  }> {
    const body = await this.client.callOk(OP.VALIDATE);
    const r = new R(body);
    const missing: string[] = [];
    const nmiss = r.u32();
    for (let i = 0; i < nmiss; i++) missing.push(r.strText());
    const nviol = r.u32();
    const violations: { entity: string; count: number; oversizedObservations: number[] }[] = [];
    for (let i = 0; i < nviol; i++) {
      const entity = r.strText();
      const count = r.u8();
      const mask = r.u8();
      const oversizedObservations: number[] = [];
      if (mask & 1) oversizedObservations.push(0);
      if (mask & 2) oversizedObservations.push(1);
      violations.push({ entity, count, oversizedObservations });
    }
    return { missing, violations };
  }

  // ------------------------------------------------------------- corpus / enumeration

  /** Paged full-corpus iteration (kb_load corpus pass, listEntities). */
  async *scanAll(): AsyncGenerator<{ eid: bigint; name: string; type: string; observations: string[] }> {
    let after = 0;
    for (;;) {
      const body = await this.client.callOk(OP.SCAN, (w) => { w.u32(after); w.u32(SCAN_CHUNK); });
      const r = new R(body);
      after = r.u32();
      const n = r.u32();
      for (let i = 0; i < n; i++) {
        const eid = BigInt(r.u32());
        const name = r.strText();
        const type = r.strText();
        const oc = r.u8();
        const observations: string[] = [];
        for (let o = 0; o < oc; o++) observations.push(r.strText());
        this.handle(name);
        yield { eid, name, type, observations };
      }
      if (after === 0) return;
    }
  }

  async listEntities(): Promise<bigint[]> {
    const out: bigint[] = [];
    for await (const row of this.scanAll()) out.push(this.handle(row.name));
    return out;
  }

  // ------------------------------------------------------------- no-ops (daemon owns these)

  async lockShared(): Promise<void> {}
  async lockExclusive(): Promise<void> {}
  async unlock(): Promise<void> {}
  async refresh(): Promise<void> {}
  async sync(): Promise<void> {}
  async incWalkerVisit(_h: bigint): Promise<void> {}      // daemon marks visits on its own reads
  async incStructuralVisit(_h: bigint): Promise<void> {}
  async structuralSample(_iterations: number, _damping: number): Promise<number> { return 0; }
  async computeMerwPsi(_alpha: number, _maxIter: number, _tol: number): Promise<number> { return 0; }
  async seedRng(_seed: bigint): Promise<void> {}

  // ------------------------------------------------------------- internals

  /** Drain a name-list op (NEIGHBORS/SEARCH/BY_TYPE/ORPHANED) via skip paging. */
  private async fetchPaged(call: (skip: number, max: number) => Promise<Buffer>): Promise<string[]> {
    const out: string[] = [];
    for (;;) {
      const body = await call(out.length, FETCH_CHUNK);
      const r = new R(body);
      const total = r.u32();
      let got = 0;
      while (r.remaining > 0) {
        out.push(r.strText());
        got++;
      }
      if (out.length >= total || got === 0) return out;
      if (out.length + got > 1_000_000) return out; // sanity bound
    }
  }

  private async fetchStrings(op: number): Promise<string[]> {
    const body = await this.client.callOk(op, (w) => w.u32(100_000));
    const r = new R(body);
    const total = r.u32();
    const out: string[] = [];
    while (r.remaining > 0 && out.length < total) out.push(r.strText());
    return out;
  }
}

export { W }; // convenience re-export for tests building raw payloads
