/**
 * daemon-client.test.ts — end-to-end smoke test for the kbd4 wire client:
 * spawn a real daemon on a temp store (loopback TCP + token), exercise the
 * v1/v1.1/v1.2 op surface, close cleanly.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';
import { DaemonClient, OP, R, ST_OK } from '../src/daemon_client.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

jest.setTimeout(120_000);

describe('kbd4 daemon client', () => {
  let client: DaemonClient;
  let dir: string;

  beforeAll(async () => {
    execFileSync('make', ['-C', path.join(ROOT, 'native'), 'kbd4'], { stdio: 'inherit' });
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kbd4-jest-'));
    client = await DaemonClient.spawn({ storeDir: dir });
  });

  afterAll(async () => {
    await client?.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('ping + stats round-trip', async () => {
    expect(await client.ping()).toBe(1); // KBD_PROTO_VER
    const s = await client.stats();
    expect(s.entities).toBe(0);
    expect(s.relations).toBe(0);
  });

  it('creates entities/relation and opens the node blob', async () => {
    await client.callOk(OP.CREATE_ENTITIES, (w) => {
      w.u32(2);
      w.str('WireA'); w.str('wireT'); w.u64(1000n);
      w.str('WireB'); w.str('wireT'); w.u64(1001n);
    });
    await client.callOk(OP.ADD_OBS, (w) => {
      w.u32(1); w.str('WireA'); w.str('obs one'); w.u64(1002n);
    });
    await client.callOk(OP.CREATE_RELATIONS, (w) => {
      w.u32(1); w.str('WireA'); w.str('WireB'); w.str('WIRES'); w.u64(1003n);
    });

    const body = await client.callOk(OP.OPEN_NODES, (w) => { w.u32(1); w.str('WireA'); });
    const r = new R(body);
    expect(r.u8()).toBe(1);                 // found
    expect(r.strText()).toBe('WireA');
    expect(r.strText()).toBe('wireT');
    expect(r.u64()).toBe(1003n);            // mtime: bumped by the from-side relation create (mtime-from-only)
    r.u64();                                // obs_mtime
    expect(r.u8()).toBe(1);                 // obs_count
    expect(r.strText()).toBe('obs one');
    expect(r.u32()).toBe(1);                // edge count
    expect(r.strText()).toBe('WIRES');
    expect(r.u8()).toBe(0);                 // forward
    expect(r.strText()).toBe('WireB');
    r.u64();
  });

  it('search with skip paging + regexValid', async () => {
    const page1 = await client.callOk(OP.BY_TYPE, (w) => { w.str('wireT'); w.u32(1); });
    let r = new R(page1);
    expect(r.u32()).toBe(2);                // total
    expect(r.strText()).toBe('WireA');

    const page2 = await client.callOk(OP.BY_TYPE, (w) => { w.str('wireT'); w.u32(1); w.u32(1); });
    r = new R(page2);
    expect(r.u32()).toBe(2);
    expect(r.strText()).toBe('WireB');

    const search = await client.callOk(OP.SEARCH, (w) => { w.str('^Wire'); w.u32(10); });
    r = new R(search);
    expect(r.u32()).toBe(2);

    const ok = await client.callOk(OP.REGEX_VALID, (w) => w.str('a+b'));
    expect(new R(ok).u8()).toBe(1);
    const bad = await client.callOk(OP.REGEX_VALID, (w) => w.str('([bad'));
    expect(new R(bad).u8()).toBe(0);
  });

  it('ranks / resample / validate / scan / walk trailer', async () => {
    await client.callOk(OP.RESAMPLE);
    const ranks = await client.callOk(OP.RANKS, (w) => { w.u32(1); w.str('WireA'); });
    let r = new R(ranks);
    r.u64();                                // walker_total
    expect(r.u64()).toBeGreaterThanOrEqual(0n); // structural_total
    r.f64(); r.f64(); r.f64();

    const val = await client.callOk(OP.VALIDATE);
    r = new R(val);
    expect(r.u32()).toBe(0);                // missing
    expect(r.u32()).toBe(0);                // violations

    const scan = await client.callOk(OP.SCAN, (w) => { w.u32(0); w.u32(10); });
    r = new R(scan);
    expect(r.u32()).toBe(0);                // drained in one page
    expect(r.u32()).toBe(2);                // both entities

    const walk = await client.callOk(OP.RANDOM_WALK, (w) => {
      w.str('WireA'); w.u32(3); w.u8(0); w.u8(1); w.u64(9n); w.u8(0);
    });
    r = new R(walk);
    const n = r.u32();
    for (let i = 0; i < n; i++) r.strText();
    expect(r.u32()).toBeGreaterThanOrEqual(0); // v1.1 uniform_steps trailer present
  });

  it('ERR status carries a message and the connection stays usable', async () => {
    const res = await client.call(OP.SEARCH, (w) => { w.str('([bad'); w.u32(4); });
    expect(res.status).not.toBe(ST_OK);
    expect(res.body.toString('utf8')).toContain('invalid pattern');
    expect(await client.ping()).toBe(1);
  });

  it('serializes concurrent calls safely', async () => {
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        client.call(OP.STATS, undefined).then((res) => {
          expect(res.status).toBe(ST_OK);
          return i;
        }),
      ),
    );
    expect(results).toHaveLength(20);
  });
});
