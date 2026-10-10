/**
 * shard-fuzz.test.ts — the sharded multi-daemon convergence fuzz
 * (docs/shard-seam-design-note.md §11, seam build step 5 / 5b-i+ii gate).
 *
 * N kbd4 daemons over N byte-cloned stores (one lineage, disjoint mint
 * ranges via KBD_NODE_BASE); concurrent writer agents per daemon; serialized
 * pairwise OP_RE_ROUND anti-entropy (the frozen single-round protocol from
 * 5b — rounds never overlap, so the documented mutual-drive deadlock cannot
 * occur); one SIGKILL mid-run with respawn on the same store; then:
 *
 *   1. quiesce writers, sweep all pairs until a full sweep applies NOTHING
 *      (a true fixpoint), with a hard sweep cap;
 *   2. assert client-visible full-state equality across every daemon
 *      (SCAN for the name set; batched OPEN_NODES tuples: type, obs,
 *      mtimes, sorted edge lists);
 *   3. VALIDATE on each daemon reports no structural violations.
 *
 * Knobs (env-overridable):
 *   FUZZ_DAEMONS       daemon count                    (default 4)
 *   FUZZ_AGENTS        writer agents per daemon       (default 2)
 *   FUZZ_BUDGET_MS     concurrent write phase         (default 30_000)
 *   FUZZ_SEED          PRNG seed                      (default 20261009)
 *   FUZZ_KILLS         SIGKILL+respawn events         (default 1)
 *   FUZZ_MAX_SWEEPS    convergence sweep cap          (default 40)
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import { describe, expect, it, jest } from '@jest/globals';
import { DaemonClient, OP, R, ST_OK, replApplies, type ReplStats } from '../src/daemon_client.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const NUM_DAEMONS = Number(process.env.FUZZ_DAEMONS ?? '4');
const AGENTS_PER = Number(process.env.FUZZ_AGENTS ?? '2');
const BUDGET_MS = Number(process.env.FUZZ_BUDGET_MS ?? '10000');
const SEED = Number(process.env.FUZZ_SEED ?? '20261009');
const KILLS = Number(process.env.FUZZ_KILLS ?? '1');
const MAX_SWEEPS = Number(process.env.FUZZ_MAX_SWEEPS ?? '20');

jest.setTimeout(BUDGET_MS + 420_000);

/** mulberry32 — small deterministic PRNG. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface DaemonSlot {
  dir: string;
  env: Record<string, string>;
  client: DaemonClient;
  alive: boolean;
}

const BASE_NAMES: string[] = Array.from({ length: 120 }, (_, i) => `B_${String(i).padStart(4, '0')}`);

describe('shard fuzz — N daemons, concurrent writers, anti-entropy rounds, kill/respawn', () => {
  it('converges to client-visible equality at a true fixpoint', async () => {
    execFileSync('make', ['-C', path.join(ROOT, 'native'), 'kbd4'], { stdio: 'inherit' });

    const rng = mulberry32(SEED);
    const pick = <T>(arr: T[]): T => arr[Math.floor(rng() * arr.length)];
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-shardfuzz-'));

    /* ---- base corpus in one store, then N byte-clones ---- */
    const baseDir = path.join(root, 'base');
    fs.mkdirSync(baseDir);
    {
      const c = await DaemonClient.spawn({ storeDir: baseDir });
      await c.callOk(OP.CREATE_ENTITIES, (w) => {
        w.u32(BASE_NAMES.length);
        for (const nm of BASE_NAMES) {
          w.str(nm);
          w.str(`T${Math.floor(rng() * 4)}`);
          w.u64(BigInt(1000 + Math.floor(rng() * 500)));
        }
      });
      await c.callOk(OP.ADD_OBS, (w) => {
        const first = BASE_NAMES.slice(0, 30);
        w.u32(first.length);
        for (const nm of first) {
          w.str(nm);
          w.str(`base obs ${nm}`);
          w.u64(2000n);
        }
      });
      await c.callOk(OP.CREATE_RELATIONS, (w) => {
        w.u32(180);
        for (let i = 0; i < 180; i++) {
          w.str(pick(BASE_NAMES));
          w.str(pick(BASE_NAMES));
          w.str(`r${i % 3}`);
          w.u64(BigInt(3000 + i));
        }
      });
      await c.close();
    }

    const daemons: DaemonSlot[] = [];
    for (let i = 0; i < NUM_DAEMONS; i++) {
      const dir = path.join(root, `d${i}`);
      fs.mkdirSync(dir);
      for (const f of ['manifest.kb', 'graph.kb', 'strings.kb']) {
        fs.copyFileSync(path.join(baseDir, f), path.join(dir, f));
      }
      const env = { KBD_NODE_BASE: String((i + 1) << 13) };
      const client = await DaemonClient.spawn({ storeDir: dir, env });
      daemons.push({ dir, env, client, alive: true });
    }
    process.stderr.write(`[shardfuzz] seed=${SEED} daemons=${NUM_DAEMONS} agents=${AGENTS_PER} budget=${BUDGET_MS}ms\n`);

    try {

    /* ---- writer agents ---- */
    interface AgentStats { ops: number; refused: number; transient: number; other: number }
    const stats: AgentStats[] = [];
    const created: string[][] = daemons.map(() => []);
    const ownRels: Array<Array<[string, string, string]>> = daemons.map(() => []);
    let seq = 0;
    let stop = false;

    const agent = async (di: number): Promise<void> => {
      const st: AgentStats = { ops: 0, refused: 0, transient: 0, other: 0 };
      stats.push(st);
      const d = daemons[di];
      let mine = 0;
      while (!stop) {
        const op = Math.floor(rng() * 10);
        try {
          if (op < 3 && created[di].length < 160) {        /* create entity */
            const nm = `F${di}_${seq++}`;
            const r = await d.client.call(OP.CREATE_ENTITIES, (w) => {
              w.u32(1);
              w.str(nm);
              w.str(`T${di}`);
              w.u64(BigInt(10_000 + seq));
            });
            if (r.status === ST_OK) created[di].push(nm); else st.refused++;
          } else if (op < 3 && created[di].length && rng() < 0.25) {
            /* at pool cap: occasional churn (delete churn leaves one tombstone
             * row per name FOREVER until the §9 directory GC lands — keep the
             * population modest so the ndir family stays cheap) */
            const nm = pick(created[di]);
            const r = await d.client.call(OP.DELETE_ENTITIES, (w) => { w.u32(1); w.str(nm); });
            if (r.status === ST_OK) {
              const ix = created[di].indexOf(nm);
              if (ix >= 0) created[di].splice(ix, 1);
            } else st.refused++;
          } else if (op === 3 && created[di].length) {     /* delete own */
            const nm = pick(created[di]);
            const r = await d.client.call(OP.DELETE_ENTITIES, (w) => { w.u32(1); w.str(nm); });
            if (r.status === ST_OK) {
              const ix = created[di].indexOf(nm);
              if (ix >= 0) created[di].splice(ix, 1);
            } else st.refused++;
          } else if (op === 4 && mine++ % 5 === 0) {       /* delete a base entity */
            const nm = pick(BASE_NAMES);
            const r = await d.client.call(OP.DELETE_ENTITIES, (w) => { w.u32(1); w.str(nm); });
            if (r.status !== ST_OK) st.refused++;
          } else if (op < 7) {                             /* create relation */
            const pool = rng() < 0.6 || !created[di].length ? BASE_NAMES : created[di];
            {
              const f = pick(pool);
              const tpool = rng() < 0.6 || !created[di].length ? BASE_NAMES : created[di];
              const t = pick(tpool);
              const rt = `f${di}`;
              const r = await d.client.call(OP.CREATE_RELATIONS, (w) => {
                w.u32(1);
                w.str(f); w.str(t); w.str(rt);
                w.u64(BigInt(20_000 + seq));
              });
              if (r.status === ST_OK) ownRels[di].push([f, t, rt]); else st.refused++;
            }
          } else if (op === 7 && ownRels[di].length) {     /* delete own relation */
            const [f, t, rt] = pick(ownRels[di]);
            const r = await d.client.call(OP.DELETE_RELATIONS, (w) => {
              w.u32(1);
              w.str(f); w.str(t); w.str(rt);
            });
            if (r.status !== ST_OK) st.refused++;
          } else if (op === 8) {                           /* add obs */
            const pool = created[di].length && rng() < 0.5 ? created[di] : BASE_NAMES;
            const r = await d.client.call(OP.ADD_OBS, (w) => {
              w.u32(1);
              w.str(pick(pool));
              w.str(`o${di}_${seq++}`);
              w.u64(BigInt(30_000 + seq));
            });
            if (r.status !== ST_OK) st.refused++;
          } else {                                         /* read */
            const batch = Array.from({ length: 1 + Math.floor(rng() * 8) }, () => pick(BASE_NAMES));
            const r = await d.client.call(OP.OPEN_NODES, (w) => {
              w.u32(batch.length);
              for (const nm of batch) w.str(nm);
            });
            if (r.status !== ST_OK) st.refused++;
          }
          st.ops++;
        } catch {
          st.transient++;      /* kill window / respawn gap */
          await sleep(20);
        }
      }
    };

    /* ---- round orchestrator (strictly serialized) ---- */
    let roundsDone = 0;
    let roundFails = 0;
    const roundOnce = async (i: number, j: number): Promise<ReplStats | null> => {
      const driver = daemons[i];
      const peer = daemons[j];
      if (!driver.alive || !peer.alive) return null;
      try {
        const st = await driver.client.replRound({
          host: '127.0.0.1', port: peer.client.port, token: peer.client.token,
        });
        roundsDone++;
        return st;
      } catch {
        roundFails++;
        return null;
      }
    };

    const agents = daemons.flatMap((_, di) => Array.from({ length: AGENTS_PER }, () => agent(di)));
    const orchestrator = (async () => {
      const deadline = Date.now() + BUDGET_MS;
      while (Date.now() < deadline && !stop) {
        const i = Math.floor(rng() * NUM_DAEMONS);
        let j = Math.floor(rng() * NUM_DAEMONS);
        if (j === i) j = (j + 1) % NUM_DAEMONS;
        await roundOnce(i, j);
        await sleep(250 + Math.floor(rng() * 250));
      }
    })();

    /* ---- kill/respawn events ---- */
    const killer = (async () => {
      for (let k = 0; k < KILLS; k++) {
        await sleep(Math.floor(BUDGET_MS * (0.35 + 0.3 * rng())));
        if (stop) return;
        const di = Math.floor(rng() * NUM_DAEMONS);
        const d = daemons[di];
        process.stderr.write(`[shardfuzz] SIGKILL daemon ${di} (pid ${String(d.client.pid)})\n`);
        d.alive = false;
        await d.client.hardKill();
        await sleep(600 + Math.floor(rng() * 600));
        d.client = await DaemonClient.spawn({ storeDir: d.dir, env: d.env });
        d.alive = true;
        process.stderr.write(`[shardfuzz] daemon ${di} respawned (pid ${String(d.client.pid)})\n`);
      }
    })();

    await Promise.all([orchestrator, killer]);
    stop = true;
    await Promise.all(agents);

    /* ---- convergence: freeze owner rank jobs, then sweeps to a fixpoint ----
     * ψ is an asymptotically-converging MERW iterate kept LIVE by each
     * owner's background rank job; two owners' iterates keep moving (and
     * the per-row LWW chases them) until both declare |Δ|<ε — a rank-cadence
     * question, measurement-deferred (note §10), NOT replication. The gate
     * therefore pauses every rank job (KBD_RANK_DISABLE) so the sweeps
     * assert REPLICATION convergence of the frozen state; live-rank churn
     * under fire ran throughout the write phase. */
    for (const d of daemons) {
      if (d.alive) {
        d.alive = false;
        await d.client.close();
      }
      d.client = await DaemonClient.spawn({
        storeDir: d.dir,
        env: { ...d.env, KBD_RANK_DISABLE: '1' },
      });
      d.alive = true;
    }
    process.stderr.write('[shardfuzz] rank jobs frozen, starting sweeps\n');

    let converged = false;
    let sweeps = 0;
    let lastDiag = '';
    const sweepT0 = Date.now();
    const sweepDeadline = sweepT0 + 240_000;
    for (; sweeps < MAX_SWEEPS && !converged && Date.now() < sweepDeadline; sweeps++) {
      let total = 0;
      const diag: string[] = [];
      const pairs: Array<[number, number]> = [];
      for (let i = 0; i < NUM_DAEMONS; i++)
        for (let j = i + 1; j < NUM_DAEMONS; j++) {
          pairs.push([i, j]);
          pairs.push([j, i]);                    /* both drive directions */
        }
      /* deterministic shuffle */
      for (let i = pairs.length - 1; i > 0; i--) {
        const j = Math.floor(rng() * (i + 1));
        [pairs[i], pairs[j]] = [pairs[j], pairs[i]];
      }
      for (const [i, j] of pairs) {
        const st = await roundOnce(i, j);
        if (!st) { total++; diag.push(`${i}->${j} FAILED`); continue; }
        const a = replApplies(st);
        total += a;
        if (a) diag.push(`${i}->${j} ${JSON.stringify(st)}`);
      }
      process.stderr.write(`[shardfuzz] sweep ${String(sweeps)} applies=${String(total)} elapsed=${String(Date.now() - sweepT0)}ms\n`);
      if (total === 0) converged = true;
      lastDiag = diag.join('\n');
    }
    if (!converged) {
      /* forensics: FULL row diff between daemons 0 and 1 */
      let probe = '';
      try {
        const live: string[] = [];
        let next = 0;
        for (;;) {
          const sb = await daemons[0].client.callOk(OP.SCAN, (w) => { w.u32(next); w.u32(512); });
          const sr = new R(sb);
          const ne = sr.u32();
          const nn = sr.u32();
          for (let k = 0; k < nn; k++) {
            sr.u32();
            live.push(sr.strText());
            sr.strText();
            const oc = sr.u8();
            for (let o = 0; o < oc; o++) sr.strText();
          }
          if (!ne || nn === 0) break;
          next = ne;
        }
        const diffRows: string[] = [];
        for (const nm of live) {
          const r0 = await daemons[0].client.rawName(nm).catch(() => null);
          const r1 = await daemons[1].client.rawName(nm).catch(() => null);
          if (!r0 || !r1 || r0.node !== r1.node || r0.gen !== r1.gen) {
            diffRows.push(`${nm}: rows ${JSON.stringify(r0)} vs ${JSON.stringify(r1)}`);
            continue;
          }
          const e0 = r0.node ? await daemons[0].client.rawEntity(r0.node).catch(() => null) : null;
          const e1 = r1.node ? await daemons[1].client.rawEntity(r1.node).catch(() => null) : null;
          const b0 = e0 ? e0.blob.toString('hex') : 'none';
          const b1 = e1 ? e1.blob.toString('hex') : 'none';
          if (b0 !== b1) diffRows.push(`${nm}: blob ${b0} vs ${b1}`);
        }
        probe = `full row diff d0/d1: ${diffRows.length} of ${live.length} names\n` +
          diffRows.slice(0, 5).join('\n');
      } catch (e) { probe = `probe failed: ${String(e)}`; }
      throw new Error(`shard fuzz did not reach a fixpoint in ${MAX_SWEEPS} sweeps; ` +
        `last sweep applied:\n${lastDiag}\n${probe}`);
    }

    /* ---- client-visible full-state equality ---- */
    const dumpStore = async (client: DaemonClient): Promise<Map<string, string>> => {
      const names: string[] = [];
      let next = 0;
      for (;;) {
        const body = await client.callOk(OP.SCAN, (w) => { w.u32(next); w.u32(512); });
        const r = new R(body);
        const nextEid = r.u32();
        const n = r.u32();
        for (let k = 0; k < n; k++) {
          r.u32();
          names.push(r.strText());
          r.strText();
          const oc = r.u8();
          for (let o = 0; o < oc; o++) r.strText();
        }
        if (!nextEid || n === 0) break;
        next = nextEid;
      }
      names.sort();
      const map = new Map<string, string>();
      for (let off = 0; off < names.length; off += 64) {
        const batch = names.slice(off, off + 64);
        const body = await client.callOk(OP.OPEN_NODES, (w) => {
          w.u32(batch.length);
          for (const nm of batch) w.str(nm);
        });
        const r = new R(body);
        for (const nm of batch) {
          const found = r.u8();
          if (!found) { map.set(nm, 'GONE'); continue; }
          r.strText();                                   /* name */
          const type = r.strText();
          const mt = r.u64();
          const omt = r.u64();
          const oc = r.u8();
          const obs: string[] = [];
          for (let o = 0; o < oc; o++) obs.push(r.strText());
          const ec = r.u32();
          const edges: string[] = [];
          for (let e = 0; e < ec; e++) {
            const rel = r.strText();
            const dir = r.u8();
            const tgt = r.strText();
            const emt = r.u64();
            edges.push(`${rel}:${dir}:${tgt}:${emt}`);
          }
          edges.sort();
          obs.sort();
          map.set(nm, `${type}|${mt}|${omt}|${obs.join(',')}|${edges.join(',')}`);
        }
      }
      return map;
    };

    const dumps: Array<Map<string, string>> = [];
    for (const d of daemons) dumps.push(await dumpStore(d.client));
    for (let i = 1; i < dumps.length; i++) {
      const ref = dumps[0];
      const other = dumps[i];
      const diffs: string[] = [];
      for (const [k, v] of ref) {
        const w = other.get(k);
        if (w !== v) diffs.push(`${k}: [${v}] vs [${String(w)}]`);
      }
      for (const k of other.keys()) if (!ref.has(k)) diffs.push(`${k}: only on daemon ${i}`);
      if (diffs.length) {
        /* node-level forensics: resolve one diffed name on both daemons */
        let nodeProbe = '';
        try {
          const nm0 = diffs[0].split(':')[0];
          const r0 = await daemons[0].client.rawName(nm0).catch(() => null);
          const r1 = await daemons[i].client.rawName(nm0).catch(() => null);
          nodeProbe =
            `\nname=${nm0} row0=${JSON.stringify(r0)} row${i}=${JSON.stringify(r1)}`;
          if (r1?.node) {
            const e0 = await daemons[0].client.rawEntity(r1.node).catch(() => null);
            const e1 = await daemons[i].client.rawEntity(r1.node).catch(() => null);
            nodeProbe += `\nnode=${r1.node} d0: ${e0 ? `name=${e0.name} gen=${e0.gen} blob=${e0.blob.toString('hex').slice(0, 60)}` : 'ABSENT'}`;
            nodeProbe += `\nnode=${r1.node} d${i}: ${e1 ? `name=${e1.name} gen=${e1.gen} blob=${e1.blob.toString('hex').slice(0, 60)}` : 'ABSENT'}`;
          }
        } catch (e) { nodeProbe = `\nprobe failed: ${String(e)}`; }
        const probeR = async (a: number, b: number): Promise<string> => {
          try {
            const st = await daemons[a].client.replRound({
              host: '127.0.0.1', port: daemons[b].client.port, token: daemons[b].client.token,
            });
            return JSON.stringify(st);
          } catch (e) { return `ERR ${String(e)}`; }
        };
        const pab = await probeR(0, i);
        const pba = await probeR(i, 0);
        throw new Error(`client-visible state differs on daemon ${i} (${diffs.length} diffs):\n` +
          diffs.slice(0, 10).join('\n') + nodeProbe +
          `\ndirect rounds 0->${i}: ${pab}\n${i}->0: ${pba}`);
      }
    }

    /* ---- structural validation on every daemon ---- */
    for (const d of daemons) {
      const body = await d.client.callOk(OP.VALIDATE);
      const r = new R(body);
      expect(r.u32()).toBe(0);                         /* missing */
      expect(r.u32()).toBe(0);                         /* violations */
    }

    /* ---- summary + teardown ---- */
    const totals = stats.reduce(
      (a, s) => ({ ops: a.ops + s.ops, refused: a.refused + s.refused, transient: a.transient + s.transient }),
      { ops: 0, refused: 0, transient: 0 },
    );
    process.stderr.write(
      `[shardfuzz] ops=${totals.ops} refused=${totals.refused} transient=${totals.transient} ` +
      `rounds=${roundsDone} roundFails=${roundFails} sweeps=${sweeps} entities/daemon=${String(dumps[0].size)}\n`,
    );
    expect(dumps[0].size).toBeGreaterThan(BASE_NAMES.length);   /* agents wrote something */

    } finally {
      /* always reap daemons (a failed/timed-out assertion must not leak
       * kbd4 processes or a temp store) */
      for (const d of daemons) {
        try { await d.client.close(); } catch { /* already gone */ }
      }
      if (process.env.FUZZ_KEEP_TMP === '1') {
        process.stderr.write(`[shardfuzz] kept: ${root}\n`);
      } else {
        fs.rmSync(root, { recursive: true, force: true });
      }
    }
  });
});
