/**
 * daemon-store.test.ts — behavioral pass over the DaemonStore adapter against
 * a real spawned kbd4: handles, blobs, edges, depth mapping, walks, queries,
 * ranks, validate, scan.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';
import { DaemonClient } from '../src/daemon_client.js';
import { DaemonStore } from '../src/daemon_store.js';
import { DIR_FORWARD, DIR_BACKWARD } from '../src/store.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

jest.setTimeout(120_000);

describe('DaemonStore adapter', () => {
  let client: DaemonClient;
  let store: DaemonStore;
  let dir: string;

  beforeAll(async () => {
    execFileSync('make', ['-C', path.join(ROOT, 'native'), 'kbd4'], { stdio: 'inherit' });
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kbd4-store-'));
    client = await DaemonClient.spawn({ storeDir: dir });
    store = new DaemonStore(client);
  });

  afterAll(async () => {
    await store?.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('create / lookup / read with handles', async () => {
    const a = await store.createEntity('AdA', 'adsT', 1000n);
    await store.addObservation(a, 'obs A', 1001n);
    expect(await store.lookup('AdA')).toBe(a);
    expect(await store.lookup('NoSuchName')).toBe(0n);

    const e = await store.readEntity(a);
    expect(e.name).toBe('AdA');
    expect(e.type).toBe('adsT');
    // obs add stamps BOTH mtime and obs_mtime (parity-gate semantics, Milestone_V4ParityGate)
    expect(e.mtime).toBe(1001n);
    expect(e.obsMtime).toBe(1001n);
    expect(e.observations).toEqual(['obs A']);
    expect(await store.entityName(a)).toBe('AdA');
  });

  it('relations: both directions visible, mtime-from-only bump', async () => {
    const a = await store.lookup('AdA');
    const b = await store.createEntity('AdB', 'adsT', 1002n);
    await store.createRelation(a, b, 'ADJ', 1003n);

    const ea = await store.edges(a);
    expect(ea).toHaveLength(1);
    expect(ea[0]).toMatchObject({ relType: 'ADJ', direction: DIR_FORWARD, target: b });
    const eb = await store.edges(b);
    expect(eb.some((x) => x.direction === DIR_BACKWARD && x.target === a)).toBe(true);

    // create_relations bumps the from entity's mtime only (BGS_D_MtimeFromOnly)
    expect((await store.readEntity(a)).mtime).toBe(1003n);
    expect((await store.readEntity(b)).mtime).toBe(1002n);
  });

  it('deletes: observation, relation, entity', async () => {
    const a = await store.lookup('AdA');
    const b = await store.lookup('AdB');
    await store.removeObservation(a, 'obs A', 1010n);
    expect((await store.readEntity(a)).observations).toEqual([]);

    await store.deleteRelation(a, b, 'ADJ');
    expect((await store.edges(a)).filter((x) => x.direction === DIR_FORWARD)).toHaveLength(0);

    const c = await store.createEntity('AdC', 'adsT', 1011n);
    await store.deleteEntity(c);
    expect(await store.lookup('AdC')).toBe(0n);
  });

  it('neighbors depth mapping + findPath', async () => {
    const p1 = await store.createEntity('ChainP1', 'chainT', 1100n);
    const p2 = await store.createEntity('ChainP2', 'chainT', 1101n);
    const p3 = await store.createEntity('ChainP3', 'chainT', 1102n);
    await store.createRelation(p1, p2, 'CHAIN', 1103n);
    await store.createRelation(p2, p3, 'CHAIN', 1104n);

    // depth-1 = immediate neighbors (one hop), not two
    expect((await store.neighbors(p1, 1, 'forward')).map(String)).toEqual([String(p2)]);
    const nb2 = (await store.neighbors(p1, 2, 'forward')).map(String);
    expect(new Set(nb2)).toEqual(new Set([String(p2), String(p3)]));

    const fp = await store.findPath(p1, p3, 5, 'forward', 0n);
    expect(fp.targetReached).toBe(true);
    expect(fp.path.map(String)).toEqual([String(p1), String(p2), String(p3)]);

    const fnone = await store.findPath(p3, p1, 5, 'forward', 0n);
    expect(fnone.targetReached).toBe(false);
    expect(fnone.farthest).toBe(0n);
  });

  it('randomWalk avoidCycles + uniformSteps trailer', async () => {
    const ping = await store.createEntity('DaemonPing', 'pingT', 1200n);
    const pong = await store.createEntity('DaemonPong', 'pingT', 1201n);
    await store.createRelation(ping, pong, 'BOUNCE', 1202n);
    await store.createRelation(pong, ping, 'BOUNCE', 1203n);

    const free = await store.randomWalk(ping, 5, 'forward', true, 7n, false);
    expect(free.path).toHaveLength(6);              // cycles freely

    const self = await store.randomWalk(ping, 5, 'forward', true, 7n, true);
    expect(self.path).toHaveLength(2);              // Ping, Pong, then blocked
    expect(self.uniformSteps).toBeGreaterThanOrEqual(0);
  });

  it('search / byType / orphaned / types / counts', async () => {
    const s = (await store.search('^ChainP')).map(String);
    expect(s).toHaveLength(3);
    expect(await store.regexValid('a+b')).toBe(true);
    expect(await store.regexValid('([bad')).toBe(false);

    expect((await store.entitiesByType('chainT')).map(String)).toHaveLength(3);

    const iso = await store.createEntity('DaemonIso', 'isoT', 1300n);
    const orphans = (await store.orphaned()).map(String);
    expect(orphans).toContain(String(iso));

    const types = await store.entityTypes();
    expect(types).toEqual(expect.arrayContaining(['chainT', 'pingT', 'isoT']));
    expect(await store.relationCount()).toBeGreaterThanOrEqual(4);
    expect(await store.entityCount()).toBeGreaterThanOrEqual(8);
  });

  it('ranksFor + resample', async () => {
    await store.resample();
    const ranks = await store.ranksFor(['ChainP1', 'NoSuchName']);
    expect(ranks.walker.has('ChainP1')).toBe(true);
    expect(ranks.walker.get('NoSuchName')).toBe(0);
    expect(ranks.structuralTotal).toBeGreaterThanOrEqual(0n);
  });

  it('validate + scanAll drains the corpus', async () => {
    const v = await store.validate();
    expect(v.missing).toEqual([]);
    expect(v.violations).toEqual([]);

    const seen: string[] = [];
    for await (const row of store.scanAll()) seen.push(row.name);
    expect(seen).toContain('ChainP1');
    expect(seen).not.toContain('AdC');              // deleted earlier
    expect(seen.length).toBe(await store.entityCount());
  });
});
