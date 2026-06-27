/**
 * v1/v2 -> v3 migration (logical rebuild), wired to run AUTOMATICALLY on open.
 *
 * Per Decision_V3Migration_Biscuit: enumerate the old KB by name/strings,
 * recreate every entity + relation in a fresh v3 store, and PRESERVE the exact
 * mtime/obsMtime/visits/psi/totals. No JSONL; the high-level C ops build the
 * structure, then the migration setters restore the fields the create /
 * relation ops clobber (addObservation forces mtime=obsMtime; createRelation
 * bumps the source mtime).
 *
 * Per Decision_AutoMigrateOnOpen: {@link autoMigrateToV3} runs in the server's
 * open path — detect old format, back the old files up to `.premigrate`,
 * rebuild v3 in place, validate, then open. Mirrors the old v1->v2 auto-migrate.
 *
 * Per Constraint_NoLiveMigration: in-place migration replaces the files, which
 * breaks any concurrently-running old instance still mmap'ing them. The deploy
 * must stop old instances first. The `.premigrate` backup is kept for recovery.
 */
import path from 'path';
import { existsSync, openSync, readSync, closeSync, renameSync, rmSync } from 'fs';
import { GraphFile, DIR_FORWARD } from './graphfile.js';
import { StringTable } from './stringtable.js';
import { Store, migrationLock, migrationUnlock } from './store.js';

const MEMFILE_MAGIC = 0x4d454d46; // "MEMF" (native MEMFILE_MAGIC)

interface OldEntity {
  name: string;
  type: string;
  obs: string[];
  mtime: bigint;
  obsMtime: bigint;
  sv: bigint;
  wv: bigint;
  psi: number;
}
interface OldRelation { from: string; to: string; relType: string; mtime: bigint; }
interface OldData { entities: OldEntity[]; relations: OldRelation[]; structuralTotal: bigint; walkerTotal: bigint; }
interface PathPair { graph: string; strings: string; }

export interface MigrateReport {
  entities: number;
  relations: number;
  ok: boolean;
  mismatches: string[];
}

function derivePaths(base: string): PathPair {
  const dir = path.dirname(base);
  const b = path.basename(base, path.extname(base));
  return { graph: path.join(dir, `${b}.graph`), strings: path.join(dir, `${b}.strings`) };
}

/**
 * Classify a `.graph` file by its memfile header (both formats store the u32
 * version at offset 4; v3 also carries the MEMF magic at offset 0).
 *   'fresh' = absent / too small to have a header (open will create v3)
 *   'v3'    = already current
 *   'old'   = v1/v2 (needs migration)
 */
export function detectGraphFormat(graphPath: string): 'v3' | 'old' | 'fresh' {
  if (!existsSync(graphPath)) return 'fresh';
  const fd = openSync(graphPath, 'r');
  try {
    const buf = Buffer.alloc(8);
    if (readSync(fd, buf, 0, 8, 0) < 8) return 'fresh';
    const magic = buf.readUInt32LE(0);
    const version = buf.readUInt32LE(4);
    if (magic === MEMFILE_MAGIC && version === 3) return 'v3';
    if (version === 1 || version === 2) return 'old';
    throw new Error(`unrecognized graph format (magic=0x${magic.toString(16)}, version=${version})`);
  } finally {
    closeSync(fd);
  }
}

/** Read everything out of an old (v1/v2) KB into plain JS. */
function readOld(p: PathPair): OldData {
  const st = new StringTable(p.strings);
  const gf = new GraphFile(p.graph, st);  // may migrate v1->v2 in place (on the copy/backup)

  const entities: OldEntity[] = [];
  const relations: OldRelation[] = [];
  let structuralTotal = 0n;
  let walkerTotal = 0n;

  gf.lockShared();
  try {
    const offsets = gf.getAllEntityOffsets();
    const nameOf = (off: bigint): string => st.get(BigInt(gf.readEntity(off).nameId));

    for (const off of offsets) {
      const r = gf.readEntity(off);
      const obs: string[] = [];
      if (r.obs0Id !== 0) obs.push(st.get(BigInt(r.obs0Id)));
      if (r.obs1Id !== 0) obs.push(st.get(BigInt(r.obs1Id)));
      entities.push({
        name: st.get(BigInt(r.nameId)),
        type: st.get(BigInt(r.typeId)),
        obs,
        mtime: r.mtime,
        obsMtime: r.obsMtime,
        sv: r.structuralVisits,
        wv: r.walkerVisits,
        psi: r.psi,
      });
    }

    for (const off of offsets) {
      const r = gf.readEntity(off);
      const fromName = st.get(BigInt(r.nameId));
      for (const e of gf.getEdges(off)) {
        if (e.direction !== DIR_FORWARD) continue;  // forward edges only (each relation once)
        relations.push({
          from: fromName,
          to: nameOf(e.targetOffset),
          relType: st.get(BigInt(e.relTypeId)),
          mtime: e.mtime,
        });
      }
    }

    structuralTotal = gf.getStructuralTotal();
    walkerTotal = gf.getWalkerTotal();
  } finally {
    gf.unlock();
  }
  gf.close();
  st.close();
  return { entities, relations, structuralTotal, walkerTotal };
}

/** Write the old data into a fresh v3 store, preserving every field. */
function writeV3(p: PathPair, old: OldData): Store {
  const store = new Store(p.graph, p.strings);
  store.lockExclusive();
  try {
    store.refresh();
    for (const e of old.entities) {
      const off = store.createEntity(e.name, e.type, e.mtime);
      for (const o of e.obs) store.addObservation(off, o, e.obsMtime);
    }
    for (const r of old.relations) {
      const f = store.lookup(r.from);
      const t = store.lookup(r.to);
      if (f === 0n || t === 0n) continue;  // dangling ref in old data; skip
      store.createRelation(f, t, r.relType, r.mtime);
    }
    // Restore preserved fields LAST: addObservation + createRelation clobbered
    // mtime/obsMtime and bumped the source mtime.
    for (const e of old.entities) {
      const off = store.lookup(e.name);
      store.setEntityFields(off, e.mtime, e.obsMtime, e.sv, e.wv, e.psi);
    }
    store.setTotals(old.structuralTotal, old.walkerTotal);
    store.sync();
  } finally {
    store.unlock();
  }
  return store;
}

/** Validate the v3 store matches the old data field-for-field. */
function validate(store: Store, old: OldData): string[] {
  const mismatches: string[] = [];
  store.lockShared();
  try {
    store.refresh();
    if (store.entityCount() !== old.entities.length)
      mismatches.push(`entityCount ${store.entityCount()} != ${old.entities.length}`);
    if (store.relationCount() !== old.relations.length)
      mismatches.push(`relationCount ${store.relationCount()} != ${old.relations.length}`);

    for (const e of old.entities) {
      const off = store.lookup(e.name);
      if (off === 0n) { mismatches.push(`missing entity ${e.name}`); continue; }
      const r = store.readEntity(off);
      if (r.type !== e.type) mismatches.push(`${e.name}: type ${r.type} != ${e.type}`);
      if (r.mtime !== e.mtime) mismatches.push(`${e.name}: mtime ${r.mtime} != ${e.mtime}`);
      if (r.obsMtime !== e.obsMtime) mismatches.push(`${e.name}: obsMtime ${r.obsMtime} != ${e.obsMtime}`);
      if (r.structuralVisits !== e.sv) mismatches.push(`${e.name}: sv ${r.structuralVisits} != ${e.sv}`);
      if (r.walkerVisits !== e.wv) mismatches.push(`${e.name}: wv ${r.walkerVisits} != ${e.wv}`);
      if (r.psi !== e.psi) mismatches.push(`${e.name}: psi ${r.psi} != ${e.psi}`);
      const want = new Set(e.obs);
      if (r.observations.length !== e.obs.length || !r.observations.every(o => want.has(o)))
        mismatches.push(`${e.name}: obs [${r.observations}] != [${e.obs}]`);
    }

    for (const rel of old.relations) {
      const f = store.lookup(rel.from);
      if (f === 0n) { mismatches.push(`relation source missing: ${rel.from}`); continue; }
      const t = store.lookup(rel.to);
      const hit = store.edges(f).some(e => e.direction === 0 && e.target === t && e.relType === rel.relType);
      if (!hit) mismatches.push(`missing relation ${rel.from} -${rel.relType}-> ${rel.to}`);
    }

    if (store.structuralTotal() !== old.structuralTotal)
      mismatches.push(`structuralTotal ${store.structuralTotal()} != ${old.structuralTotal}`);
    if (store.walkerTotal() !== old.walkerTotal)
      mismatches.push(`walkerTotal ${store.walkerTotal()} != ${old.walkerTotal}`);
  } finally {
    store.unlock();
  }
  return mismatches;
}

/** Migrate old files (`oldP`, MUST be a copy/backup) into fresh v3 files (`newP`). */
export function migratePaths(oldP: PathPair, newP: PathPair): MigrateReport {
  const old = readOld(oldP);
  const store = writeV3(newP, old);
  const mismatches = validate(store, old);
  store.close();
  return {
    entities: old.entities.length,
    relations: old.relations.length,
    ok: mismatches.length === 0,
    mismatches,
  };
}

/** Migrate by base path (`.graph`/`.strings` derived). `oldBase` MUST be a copy. */
export function migrate(oldBase: string, newBase: string): MigrateReport {
  return migratePaths(derivePaths(oldBase), derivePaths(newBase));
}

/**
 * Auto-migrate an old (v1/v2) KB to v3 IN PLACE. Backs the old files up to
 * `.premigrate`, rebuilds v3 at the original paths, validates, and (on any
 * failure) restores the backup before throwing. Caller must have already
 * confirmed the format is 'old' (via {@link detectGraphFormat}).
 */
/**
 * Core in-place rebuild: rename old files to `.premigrate`, build v3 at the
 * original paths, validate, and restore the backup on any failure. The caller
 * MUST hold the migration lock and have confirmed the format is 'old'.
 */
function migrateInPlace(graphPath: string, strPath: string): MigrateReport {
  const bakGraph = `${graphPath}.premigrate`;
  const bakStr = `${strPath}.premigrate`;
  renameSync(graphPath, bakGraph);
  renameSync(strPath, bakStr);

  const restore = (): void => {
    rmSync(graphPath, { force: true });
    rmSync(strPath, { force: true });
    renameSync(bakGraph, graphPath);
    renameSync(bakStr, strPath);
  };

  let report: MigrateReport;
  try {
    report = migratePaths({ graph: bakGraph, strings: bakStr }, { graph: graphPath, strings: strPath });
  } catch (e) {
    restore();
    throw e;
  }
  if (!report.ok) {
    restore();
    throw new Error(`v3 auto-migration failed (data preserved in ${bakGraph}/${bakStr}): ${report.mismatches.join('; ')}`);
  }
  return report;
}

/**
 * Ensure the KB is v3, migrating from v1/v2 in place if needed. Holds an
 * exclusive migration flock across BOTH detection and migration, so concurrent
 * server startups serialize: the first migrates, the rest wait then observe v3.
 * This closes the race where a process reads the file mid-rename, sees it as
 * 'fresh', and clobbers the KB with an empty store. Returns the migration
 * report if a migration ran, else null (already v3, or a fresh/empty KB).
 */
export function ensureV3(graphPath: string, strPath: string): MigrateReport | null {
  const lockFd = migrationLock(`${graphPath}.migrate.lock`);
  try {
    if (detectGraphFormat(graphPath) !== 'old') return null;  // fresh or already v3
    return migrateInPlace(graphPath, strPath);
  } finally {
    migrationUnlock(lockFd);
  }
}
