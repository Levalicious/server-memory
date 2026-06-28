/**
 * Store — typed wrapper over the v3 C graph store (graphstore.node).
 *
 * The C store IS the database: it owns the memfile, the string table, the name
 * index, traversal, search, ranking and validation. This class is a thin
 * marshaling layer. All entity/relation offsets are BigInt (u64 in C).
 */
import { createRequire } from 'module';
import { existsSync } from 'fs';
import { dirname, join, parse as parsePath } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// Edge / traversal direction codes (must match graph.h).
export const DIR_FORWARD = 0;
export const DIR_BACKWARD = 1;
export const DIR_ANY = 255;

export type Direction = 'forward' | 'backward' | 'any';
export function dirCode(d: Direction): number {
  return d === 'forward' ? DIR_FORWARD : d === 'backward' ? DIR_BACKWARD : DIR_ANY;
}

/** Resolved entity record as returned by the native readEntity op. */
export interface NativeEntity {
  name: string;
  type: string;
  observations: string[];
  mtime: bigint;
  obsMtime: bigint;
  structuralVisits: bigint;
  walkerVisits: bigint;
  psi: number;
}

/** One adjacency entry; relType is resolved to its string by the native op. */
export interface NativeEdge {
  target: bigint;
  direction: number; // DIR_FORWARD | DIR_BACKWARD
  relType: string;
  mtime: bigint;
}

interface NativeStore {
  open(graphPath: string, strPath: string, initialSize: number): unknown;
  close(h: unknown): void;
  sync(h: unknown): void;
  lockShared(h: unknown): void;
  lockExclusive(h: unknown): void;
  unlock(h: unknown): void;
  refresh(h: unknown): void;
  lookup(h: unknown, name: string): bigint;
  createEntity(h: unknown, name: string, type: string, mtime: bigint): bigint;
  deleteEntity(h: unknown, offset: bigint): boolean;
  readEntity(h: unknown, offset: bigint): NativeEntity;
  entityName(h: unknown, offset: bigint): string;
  addObservation(h: unknown, offset: bigint, obs: string, mtime: bigint): boolean;
  removeObservation(h: unknown, offset: bigint, obs: string, mtime: bigint): boolean;
  createRelation(h: unknown, from: bigint, to: bigint, relType: string, mtime: bigint): void;
  deleteRelation(h: unknown, from: bigint, to: bigint, relType: string): boolean;
  edges(h: unknown, offset: bigint): NativeEdge[];
  neighbors(h: unknown, start: bigint, depth: number, direction: number): bigint[];
  findPath(h: unknown, from: bigint, to: bigint, maxDepth: number, direction: number, budgetBytes: bigint): { path: bigint[]; targetReached: boolean; budgetExhausted: boolean; farthest: bigint };
  search(h: unknown, pattern: string): bigint[];
  regexValid(pattern: string): boolean;
  entitiesByType(h: unknown, type: string): bigint[];
  orphaned(h: unknown): bigint[];
  listEntities(h: unknown): bigint[];
  entityTypes(h: unknown): string[];
  relationTypes(h: unknown): string[];
  entityCount(h: unknown): number;
  relationCount(h: unknown): number;
  incWalkerVisit(h: unknown, offset: bigint): void;
  incStructuralVisit(h: unknown, offset: bigint): void;
  structuralTotal(h: unknown): bigint;
  walkerTotal(h: unknown): bigint;
  structuralRank(h: unknown, offset: bigint): number;
  walkerRank(h: unknown, offset: bigint): number;
  getPsi(h: unknown, offset: bigint): number;
  structuralSample(h: unknown, iterations: number, damping: number): number;
  computeMerwPsi(h: unknown, alpha: number, maxIter: number, tol: number): number;
  seedRng(h: unknown, seed: bigint): void;
  randomWalk(h: unknown, start: bigint, depth: number, direction: number, merwMode: number, seed: bigint): bigint[];
  validateObs(h: unknown): { offset: bigint; count: number; oversize: number }[];
  validateDangling(h: unknown): { src: bigint; target: bigint }[];
  setEntityFields(h: unknown, off: bigint, mtime: bigint, obsMtime: bigint, structuralVisits: bigint, walkerVisits: bigint, psi: number): void;
  setTotals(h: unknown, structuralTotal: bigint, walkerTotal: bigint): void;
  lockPath(path: string): number;
  unlockPath(fd: number): void;
}

/**
 * Acquire an exclusive kernel flock on a lock file (blocks until held;
 * auto-released if this process dies). Used to serialize the one-time v3
 * migration so concurrent server startups can't race it. Returns an fd handle
 * (< 0 / huge on failure — caller proceeds best-effort).
 */
export function migrationLock(path: string): number { return native.lockPath(path); }
export function migrationUnlock(fd: number): void { native.unlockPath(fd); }

// Walk up from __dirname to find build/Release/graphstore.node. Works from
// source (src/), compiled (dist/src/), and npx cache contexts.
function findNative(): string {
  let dir = __dirname;
  const { root } = parsePath(dir);
  while (dir !== root) {
    const candidate = join(dir, 'build', 'Release', 'graphstore.node');
    if (existsSync(candidate)) return candidate;
    dir = dirname(dir);
  }
  throw new Error('Could not find native graphstore.node — was the C addon built? Run: node-gyp rebuild');
}

const native: NativeStore = require(findNative());

export class Store {
  private h: unknown;

  constructor(graphPath: string, strPath: string, initialSize: number = 65536) {
    this.h = native.open(graphPath, strPath, initialSize);
  }

  // lifecycle / concurrency
  close(): void { native.close(this.h); }
  sync(): void { native.sync(this.h); }
  lockShared(): void { native.lockShared(this.h); }
  lockExclusive(): void { native.lockExclusive(this.h); }
  unlock(): void { native.unlock(this.h); }
  refresh(): void { native.refresh(this.h); }

  // entities
  lookup(name: string): bigint { return native.lookup(this.h, name); }
  createEntity(name: string, type: string, mtime: bigint): bigint { return native.createEntity(this.h, name, type, mtime); }
  deleteEntity(offset: bigint): boolean { return native.deleteEntity(this.h, offset); }
  readEntity(offset: bigint): NativeEntity { return native.readEntity(this.h, offset); }
  entityName(offset: bigint): string { return native.entityName(this.h, offset); }
  addObservation(offset: bigint, obs: string, mtime: bigint): boolean { return native.addObservation(this.h, offset, obs, mtime); }
  removeObservation(offset: bigint, obs: string, mtime: bigint): boolean { return native.removeObservation(this.h, offset, obs, mtime); }

  // relations
  createRelation(from: bigint, to: bigint, relType: string, mtime: bigint): void { native.createRelation(this.h, from, to, relType, mtime); }
  deleteRelation(from: bigint, to: bigint, relType: string): boolean { return native.deleteRelation(this.h, from, to, relType); }
  edges(offset: bigint): NativeEdge[] { return native.edges(this.h, offset); }

  // traversal / search / scans
  neighbors(start: bigint, depth: number, direction: Direction): bigint[] { return native.neighbors(this.h, start, depth, dirCode(direction)); }
  findPath(from: bigint, to: bigint, maxDepth: number, direction: Direction, budgetBytes: bigint): { path: bigint[]; targetReached: boolean; budgetExhausted: boolean; farthest: bigint } {
    return native.findPath(this.h, from, to, maxDepth, dirCode(direction), budgetBytes);
  }
  search(pattern: string): bigint[] { return native.search(this.h, pattern); }
  /** True iff `pattern` compiles under the C POSIX ERE engine (same dialect as search). */
  regexValid(pattern: string): boolean { return native.regexValid(pattern); }
  entitiesByType(type: string): bigint[] { return native.entitiesByType(this.h, type); }
  orphaned(): bigint[] { return native.orphaned(this.h); }
  listEntities(): bigint[] { return native.listEntities(this.h); }
  entityTypes(): string[] { return native.entityTypes(this.h); }
  relationTypes(): string[] { return native.relationTypes(this.h); }
  entityCount(): number { return native.entityCount(this.h); }
  relationCount(): number { return native.relationCount(this.h); }

  // ranking
  incWalkerVisit(offset: bigint): void { native.incWalkerVisit(this.h, offset); }
  incStructuralVisit(offset: bigint): void { native.incStructuralVisit(this.h, offset); }
  structuralTotal(): bigint { return native.structuralTotal(this.h); }
  walkerTotal(): bigint { return native.walkerTotal(this.h); }
  structuralRank(offset: bigint): number { return native.structuralRank(this.h, offset); }
  walkerRank(offset: bigint): number { return native.walkerRank(this.h, offset); }
  getPsi(offset: bigint): number { return native.getPsi(this.h, offset); }
  structuralSample(iterations: number, damping: number): number { return native.structuralSample(this.h, iterations, damping); }
  computeMerwPsi(alpha: number, maxIter: number, tol: number): number { return native.computeMerwPsi(this.h, alpha, maxIter, tol); }
  seedRng(seed: bigint): void { native.seedRng(this.h, seed); }
  randomWalk(start: bigint, depth: number, direction: Direction, merwMode: boolean, seed: bigint): bigint[] {
    return native.randomWalk(this.h, start, depth, dirCode(direction), merwMode ? 1 : 0, seed);
  }

  // validate
  validateObs(): { offset: bigint; count: number; oversize: number }[] { return native.validateObs(this.h); }
  validateDangling(): { src: bigint; target: bigint }[] { return native.validateDangling(this.h); }

  // migration: restore preserved fields after the logical rebuild.
  setEntityFields(off: bigint, mtime: bigint, obsMtime: bigint, structuralVisits: bigint, walkerVisits: bigint, psi: number): void {
    native.setEntityFields(this.h, off, mtime, obsMtime, structuralVisits, walkerVisits, psi);
  }
  setTotals(structuralTotal: bigint, walkerTotal: bigint): void { native.setTotals(this.h, structuralTotal, walkerTotal); }
}
