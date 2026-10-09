/**
 * daemon_store.ts — GraphBackend over the kbd4 wire client.
 *
 * The daemon speaks NAMES (and returns name lists); this adapter presents the
 * offset-flavoured surface the manager expects by keeping a per-client handle
 * table: handle ↔ name (handles are client-local bigints; 0n means absent,
 * matching the embedded store's convention).
 *
 * Connection is deferred: pass a DaemonClient or a promise of one (e.g.
 * DaemonClient.spawn(...) — kicked off synchronously, awaited by the first
 * operation via `ready`). Consistency is per-call (each daemon request is its
 * own transaction): reads are never cached.
 */
import { DaemonClient, OP, R, ST_OK } from './daemon_client.js';
import { DIR_FORWARD, DIR_BACKWARD, type Direction } from './store.js';
import { StoreRecordError } from './errors.js';
import type { GraphBackend, BackendEntity, BackendEdge, BackendRanks, BackendValidation, ScanRow } from './backend.js';

/** Results-paging chunk size (frames are capped at 1 MiB by the daemon). */
const FETCH_CHUNK = 10_000;
const SCAN_CHUNK = 4096;

function dirCode(d: Direction): 0 | 1 | 2 {
  return d === 'forward' ? 0 : d === 'backward' ? 1 : 2;
}

export class DaemonStore implements GraphBackend {
  private client: DaemonClient | null = null;
  private readonly ready: Promise<void>;
  private nextHandle = 1n;
  private byName = new Map<string, bigint>();
  private byHandle = new Map<bigint, string>();

  constructor(client: DaemonClient | Promise<DaemonClient>) {
    this.ready = Promise.resolve(client).then((c) => {
      this.client = c;
      // Rank is the daemon's own background job (spec §7 / Design_RankAmortized):
      // no boot resample here, and none on the op path — the owner maintains
      // rank in amortized idle slices.
    });
  }

  /** Await the connection. Exposed for tests/bootstraps. */
  async waitReady(): Promise<void> {
    await this.ready;
  }

  private async conn(): Promise<DaemonClient> {
    await this.ready;
    return this.client!;
  }

  async close(): Promise<void> {
    try { await this.ready; } catch { /* connection never came up */ }
    if (this.client) await this.client.close();
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

  private async openBlob(name: string): Promise<{ entity: BackendEntity; edges: BackendEdge[] } | null> {
    const client = await this.conn();
    const body = await client.callOk(OP.OPEN_NODES, (w) => { w.u32(1); w.str(name); });
    const r = new R(body);
    if (r.u8() === 0) return null;
    const entity: BackendEntity = {
      name: r.strText(),
      type: r.strText(),
      mtime: r.u64(),
      obsMtime: r.u64(),
      observations: [],
    };
    const oc = r.u8();
    for (let i = 0; i < oc; i++) entity.observations.push(r.strText());
    const edges: BackendEdge[] = [];
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

  async readEntity(h: bigint): Promise<BackendEntity> {
    const blob = await this.openBlob(this.nameOf(h));
    if (!blob) throw new Error(`daemon store: entity missing for handle ${h}`);
    return blob.entity;
  }

  async entityName(h: bigint): Promise<string> {
    return this.nameOf(h);
  }

  async edges(h: bigint): Promise<BackendEdge[]> {
    const blob = await this.openBlob(this.nameOf(h));
    return blob ? blob.edges : [];
  }

  // ------------------------------------------------------------- writes

  async createEntity(name: string, type: string, mtime: bigint): Promise<bigint> {
    const client = await this.conn();
    const body = await client.callOk(OP.CREATE_ENTITIES, (w) => {
      w.u32(1); w.str(name); w.str(type); w.u64(mtime);
    });
    const r = new R(body);
    const eid = r.u32();
    if (!eid) {
      // v1.4 reason byte: 1=name over cap, 2=type over cap, 3=other (absent on
      // older daemons). Facts, not policy — server.ts maps to the envelope.
      const reason = r.remaining >= 1 ? r.u8() : 3;
      if (reason === 1) throw new StoreRecordError('name', Buffer.byteLength(name, 'utf8'));
      if (reason === 2) throw new StoreRecordError('type', Buffer.byteLength(type, 'utf8'));
      throw new Error(`daemon store: create returned no eid for "${name.slice(0, 64)}"`);
    }
    return this.handle(name);
  }

  async deleteEntity(h: bigint): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.DELETE_ENTITIES, (w) => { w.u32(1); w.str(this.nameOf(h)); });
  }

  async addObservation(h: bigint, text: string, mtime: bigint): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.ADD_OBS, (w) => { w.u32(1); w.str(this.nameOf(h)); w.str(text); w.u64(mtime); });
  }

  async removeObservation(h: bigint, text: string, mtime: bigint): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.DEL_OBS, (w) => { w.u32(1); w.str(this.nameOf(h)); w.str(text); w.u64(mtime); });
  }

  async createRelation(from: bigint, to: bigint, relType: string, mtime: bigint): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.CREATE_RELATIONS, (w) => {
      w.u32(1); w.str(this.nameOf(from)); w.str(this.nameOf(to)); w.str(relType); w.u64(mtime);
    });
  }

  async deleteRelation(from: bigint, to: bigint, relType: string): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.DELETE_RELATIONS, (w) => {
      w.u32(1); w.str(this.nameOf(from)); w.str(this.nameOf(to)); w.str(relType);
    });
  }

  // ------------------------------------------------------------- traversal

  /** `hops` counts hops from the start (wire depth = hops - 1, PUBLIC 0-indexed). */
  async neighbors(start: bigint, hops: number, direction: Direction): Promise<bigint[]> {
    const client = await this.conn();
    const name = this.nameOf(start);
    const names = await this.fetchPaged(
      (skip, max) => client.callOk(OP.NEIGHBORS, (w) => {
        w.str(name); w.u32(Math.max(0, hops - 1)); w.u8(dirCode(direction)); w.u32(max); w.u32(skip);
      }),
    );
    return names.map((n) => this.handle(n));
  }

  async findPath(from: bigint, to: bigint, maxDepth: number, direction: Direction, budgetBytes: bigint): Promise<{
    path: bigint[];
    targetReached: boolean;
    budgetExhausted: boolean;
    farthest: bigint;
  }> {
    const client = await this.conn();
    const fromName = this.nameOf(from);
    const toName = this.nameOf(to);
    const body = await client.callOk(OP.FIND_PATH, (w) => {
      w.str(fromName); w.str(toName); w.u32(maxDepth); w.u8(dirCode(direction));
      w.u64(budgetBytes);       // v1.3 trailer: per-call BFS byte budget
    });
    const r = new R(body);
    const n = r.u32();
    const names: string[] = [];
    for (let i = 0; i < n; i++) names.push(r.strText());
    // v1.3 β-contract flags (trailing; fall back to the name check if absent).
    let reached = n > 0 && names[names.length - 1] === toName;
    let exhausted = false;
    if (r.remaining >= 2) {
      reached = r.u8() === 1;
      exhausted = r.u8() === 1;
    }
    const farthest = !reached && names.length > 0 ? this.handle(names[names.length - 1]) : 0n;
    return {
      path: names.map((nm) => this.handle(nm)),
      targetReached: reached,
      budgetExhausted: exhausted,
      farthest,
    };
  }

  async randomWalk(start: bigint, depth: number, direction: Direction, merwMode: boolean, seed: bigint, avoidCycles: boolean): Promise<{ path: bigint[]; uniformSteps: number }> {
    const client = await this.conn();
    const body = await client.callOk(OP.RANDOM_WALK, (w) => {
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
    const client = await this.conn();
    const names = await this.fetchPaged(
      (skip, max) => client.callOk(OP.SEARCH, (w) => { w.str(pattern); w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async regexValid(pattern: string): Promise<boolean> {
    const client = await this.conn();
    const body = await client.callOk(OP.REGEX_VALID, (w) => w.str(pattern));
    return new R(body).u8() === 1;
  }

  async entitiesByType(type: string): Promise<bigint[]> {
    const client = await this.conn();
    const names = await this.fetchPaged(
      (skip, max) => client.callOk(OP.BY_TYPE, (w) => { w.str(type); w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async orphaned(): Promise<bigint[]> {
    const client = await this.conn();
    const names = await this.fetchPaged(
      (skip, max) => client.callOk(OP.ORPHANED, (w) => { w.u32(max); w.u32(skip); }),
    );
    return names.map((n) => this.handle(n));
  }

  async entityTypes(): Promise<string[]> {
    const client = await this.conn();
    return this.fetchStrings(client, OP.ENTITY_TYPES);
  }

  async relationTypes(): Promise<string[]> {
    const client = await this.conn();
    return this.fetchStrings(client, OP.RELATION_TYPES);
  }

  async entityCount(): Promise<number> {
    const client = await this.conn();
    return (await client.stats()).entities;
  }

  async relationCount(): Promise<number> {
    const client = await this.conn();
    return (await client.stats()).relations;
  }

  // ------------------------------------------------------------- ranks / resample

  /** Rank maps for a name list (totals included). */
  async ranksFor(names: string[]): Promise<BackendRanks> {
    const client = await this.conn();
    const body = await client.callOk(OP.RANKS, (w) => {
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

  /** Forced (admin/debug) rank refresh via OP_RESAMPLE — NOT called on the op
   * path: the daemon maintains rank in background idle slices (spec §7). */
  async resample(): Promise<void> {
    const client = await this.conn();
    await client.callOk(OP.RESAMPLE);
  }

  // ------------------------------------------------------------- validate

  async validate(): Promise<BackendValidation> {
    const client = await this.conn();
    const body = await client.callOk(OP.VALIDATE);
    const r = new R(body);
    const missing: string[] = [];
    const nmiss = r.u32();
    for (let i = 0; i < nmiss; i++) missing.push(r.strText());
    const nviol = r.u32();
    const violations: BackendValidation['violations'] = [];
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
  async *scanAll(): AsyncGenerator<ScanRow> {
    const client = await this.conn();
    let after = 0;
    for (;;) {
      const body = await client.callOk(OP.SCAN, (w) => { w.u32(after); w.u32(SCAN_CHUNK); });
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

  // ------------------------------------------------------------- daemon-owned no-ops

  async lockShared(): Promise<void> {}
  async lockExclusive(): Promise<void> {}
  async unlock(): Promise<void> {}
  async refresh(): Promise<void> {}
  async sync(): Promise<void> {}
  async incWalkerVisit(_h: bigint): Promise<void> {}      // daemon marks visits on its own reads
  async incStructuralVisit(_h: bigint): Promise<void> {}

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

  private async fetchStrings(client: DaemonClient, op: number): Promise<string[]> {
    const body = await client.callOk(op, (w) => w.u32(100_000));
    const r = new R(body);
    const total = r.u32();
    const out: string[] = [];
    while (r.remaining > 0 && out.length < total) out.push(r.strText());
    return out;
  }
}

export { ST_OK };
