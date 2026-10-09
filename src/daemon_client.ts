/**
 * daemon_client.ts — client for the kbd4 owner daemon (spec r3 §6.1,
 * native/daemon_proto.h v1.2).
 *
 * Frame:    [u32 len][u32 reqid][u8 op][payload]      (len = 5 + payload)
 * Response: [u32 len][u32 reqid][u8 status][payload]  (status: ST_OK | ST_ERR)
 * All integers little-endian; str = [u16 len][bytes]; max frame 1 MiB.
 * The first frame on a connection MUST be OP_AUTH (bearer token).
 *
 * The daemon's poll loop answers per connection strictly in order, so this
 * client keeps ONE request in flight at a time: `call()` chains internally
 * and every call has a timeout (a mute server must not hang a session —
 * Bug_MCPClientNoTimeoutNoReconnect_2026_06_01).
 */
import net from 'net';
import { spawn, type ChildProcess } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';

export const KBD_MAX_FRAME = 1 << 20;
export const KBD_PROTO_VER = 1;

/** Opcodes — mirror of native/daemon_proto.h. */
export const OP = {
  AUTH: 0x01,
  PING: 0x02,
  STATS: 0x03,
  CREATE_ENTITIES: 0x10,
  DELETE_ENTITIES: 0x11,
  CREATE_RELATIONS: 0x12,
  DELETE_RELATIONS: 0x13,
  ADD_OBS: 0x14,
  DEL_OBS: 0x15,
  OPEN_NODES: 0x20,
  NEIGHBORS: 0x21,
  FIND_PATH: 0x22,
  SEARCH: 0x23,
  BY_TYPE: 0x24,
  ENTITY_TYPES: 0x25,
  RELATION_TYPES: 0x26,
  ORPHANED: 0x27,
  RANDOM_WALK: 0x28,
  RANKS: 0x29,
  RESAMPLE: 0x2a,
  VALIDATE: 0x2b,
  SCAN: 0x2c,
  REGEX_VALID: 0x2d,
  RESUME: 0x2e,
} as const;

export const ST_OK = 0;

/** Direction codes on the wire. */
export const WIRE_DIR = { FORWARD: 0, BACKWARD: 1, ANY: 2 } as const;
export type WireDir = (typeof WIRE_DIR)[keyof typeof WIRE_DIR];

function wireDir(d: 'forward' | 'backward' | 'any'): WireDir {
  return d === 'backward' ? WIRE_DIR.BACKWARD : d === 'any' ? WIRE_DIR.ANY : WIRE_DIR.FORWARD;
}

/** Request payload builder. */
export class W {
  private chunks: Buffer[] = [];
  u8(v: number): this { const b = Buffer.allocUnsafe(1); b.writeUInt8(v & 0xff); this.chunks.push(b); return this; }
  u32(v: number): this { const b = Buffer.allocUnsafe(4); b.writeUInt32LE(v >>> 0); this.chunks.push(b); return this; }
  u64(v: bigint): this { const b = Buffer.allocUnsafe(8); b.writeBigUInt64LE(v); this.chunks.push(b); return this; }
  f64(v: number): this { const b = Buffer.allocUnsafe(8); b.writeDoubleLE(v); this.chunks.push(b); return this; }
  str(s: string | Buffer): this {
    const bytes = Buffer.isBuffer(s) ? s : Buffer.from(s, 'utf8');
    const l = Buffer.allocUnsafe(2);
    l.writeUInt16LE(bytes.length);
    this.chunks.push(l, bytes);
    return this;
  }
  buf(): Buffer { return Buffer.concat(this.chunks); }
}

/** Response payload reader. */
export class R {
  private p = 0;
  constructor(private readonly b: Buffer) {}
  get remaining(): number { return this.b.length - this.p; }
  u8(): number { const v = this.b.readUInt8(this.p); this.p += 1; return v; }
  u32(): number { const v = this.b.readUInt32LE(this.p); this.p += 4; return v; }
  u64(): bigint { const v = this.b.readBigUInt64LE(this.p); this.p += 8; return v; }
  f64(): number { const v = this.b.readDoubleLE(this.p); this.p += 8; return v; }
  str(): Buffer { const l = this.b.readUInt16LE(this.p); this.p += 2; const s = this.b.subarray(this.p, this.p + l); this.p += l; return s; }
  strText(): string { return this.str().toString('utf8'); }
}

export interface CallResult { status: number; body: Buffer; }
export type PayloadBuilder = (w: W) => void;

export interface DaemonStats { entities: number; relations: number; txid: bigint; }

interface Pending { reqid: number; resolve: (r: CallResult) => void; reject: (e: Error) => void; timer: NodeJS.Timeout; }

export class DaemonClient {
  private sock: net.Socket | null = null;
  private rxBuf: Buffer = Buffer.alloc(0);
  private pending: Pending | null = null;
  private chain: Promise<unknown> = Promise.resolve();
  private nextReqId = 1;
  private child: ChildProcess | null = null;
  private tokenFilePath: string | null = null;
  private closed = false;

  private constructor() {}

  /** Connect to a running daemon and authenticate. */
  static async connect(host: string, port: number, token: string, timeoutMs = 5000): Promise<DaemonClient> {
    const c = new DaemonClient();
    await c.openSocket(host, port);
    const auth = await c.call(OP.AUTH, (w) => w.str(token), timeoutMs);
    if (auth.status !== ST_OK) {
      c.destroyNow();
      throw new Error(`kbd4 auth failed: ${auth.body.toString('utf8')}`);
    }
    return c;
  }

  /**
   * Spawn a local kbd4 for `storeDir` on a free loopback port (token file
   * written 0600 inside the store dir) and connect to it. `close()` reaps
   * the child with SIGTERM.
   */
  static async spawn(opts: { storeDir: string; binaryPath?: string; readyTimeoutMs?: number }): Promise<DaemonClient> {
    const binary = opts.binaryPath ?? defaultBinaryPath();
    if (!fs.existsSync(binary)) {
      throw new Error(`kbd4 binary not found at ${binary}; build it with: make -C native kbd4`);
    }
    const port = await freeLoopbackPort();
    const token = randomBytes(24).toString('hex');
    const tokenPath = path.join(opts.storeDir, 'kbd4.token');
    fs.writeFileSync(tokenPath, token, { mode: 0o600 });

    const child = spawn(binary, [opts.storeDir, String(port), tokenPath], {
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr?.on('data', (d) => { stderr += String(d); });

    const deadline = Date.now() + (opts.readyTimeoutMs ?? 5000);
    let lastErr: unknown = null;
    while (Date.now() < deadline) {
      if (child.exitCode !== null) break;
      try {
        const c = await DaemonClient.connect('127.0.0.1', port, token, 1000);
        c.child = child;
        c.tokenFilePath = tokenPath;
        return c;
      } catch (e) {
        lastErr = e;
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    child.kill('SIGTERM');
    throw new Error(
      `kbd4 did not become ready on port ${port} (exit=${child.exitCode}): ${String(lastErr)}` +
      (stderr ? `\nstderr: ${stderr.slice(0, 400)}` : ''),
    );
  }

  /** One request/response round trip; calls are serialized per connection. */
  call(op: number, build?: PayloadBuilder, timeoutMs = 30_000): Promise<CallResult> {
    const exec = (): Promise<CallResult> => this.roundTrip(op, build, timeoutMs);
    const p = this.chain.then(exec, exec);
    this.chain = p.then(() => undefined, () => undefined);
    return p;
  }

  /** Convenience: call + throw on ERR status. */
  async callOk(op: number, build?: PayloadBuilder, timeoutMs = 30_000): Promise<Buffer> {
    const { status, body } = await this.call(op, build, timeoutMs);
    if (status !== ST_OK) {
      throw new Error(`kbd4 op 0x${op.toString(16)} failed: ${body.toString('utf8') || 'unknown error'}`);
    }
    return body;
  }

  async ping(): Promise<number> {
    const body = await this.callOk(OP.PING);
    return new R(body).u32();
  }

  async stats(): Promise<DaemonStats> {
    const body = await this.callOk(OP.STATS);
    const r = new R(body);
    return { entities: r.u32(), relations: r.u32(), txid: r.u64() };
  }

  /** Close the connection and reap a spawned daemon (SIGTERM, then SIGKILL). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const child = this.child;
    this.destroyNow();
    if (child && child.exitCode === null) {
      const exited = new Promise<void>((res) => child.once('exit', () => res()));
      child.kill('SIGTERM');
      const timer = setTimeout(() => { child.kill('SIGKILL'); }, 3000);
      await exited;
      clearTimeout(timer);
    }
    if (this.tokenFilePath) {
      try { fs.unlinkSync(this.tokenFilePath); } catch { /* best effort */ }
    }
  }

  // ---------------------------------------------------------------- internals

  private openSocket(host: string, port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host, port });
      const onErr = (e: Error) => { sock.destroy(); reject(e); };
      sock.once('error', onErr);
      sock.once('connect', () => {
        sock.off('error', onErr);
        this.sock = sock;
        sock.setNoDelay(true);
        sock.on('data', (d: Buffer | string) => this.onData(Buffer.isBuffer(d) ? d : Buffer.from(d)));
        sock.on('error', (e) => this.onFailure(new Error(`kbd4 connection error: ${e.message}`)));
        sock.on('close', () => this.onFailure(new Error('kbd4 connection closed')));
        resolve();
      });
    });
  }

  private roundTrip(op: number, build: PayloadBuilder | undefined, timeoutMs: number): Promise<CallResult> {
    if (this.closed || !this.sock) {
      return Promise.reject(new Error('kbd4 client is closed'));
    }
    const payload = build ? (() => { const w = new W(); build(w); return w.buf(); })() : Buffer.alloc(0);
    const len = 5 + payload.length;
    if (len > KBD_MAX_FRAME) {
      return Promise.reject(new Error(`kbd4 request too large: ${len} bytes (max ${KBD_MAX_FRAME})`));
    }
    const reqid = this.nextReqId++;
    const frame = Buffer.allocUnsafe(9 + payload.length);
    frame.writeUInt32LE(len, 0);
    frame.writeUInt32LE(reqid, 4);
    frame.writeUInt8(op, 8);
    payload.copy(frame, 9);

    return new Promise<CallResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.onFailure(new Error(`kbd4 call timeout after ${timeoutMs}ms (op 0x${op.toString(16)})`));
      }, timeoutMs);
      this.pending = { reqid, resolve, reject, timer };
      this.sock!.write(frame);
    });
  }

  private onData(d: Buffer): void {
    this.rxBuf = this.rxBuf.length === 0 ? d : Buffer.concat([this.rxBuf, d]);
    for (;;) {
      if (this.rxBuf.length < 4) return;
      const len = this.rxBuf.readUInt32LE(0);
      if (len < 5 || len > KBD_MAX_FRAME) {
        this.onFailure(new Error(`kbd4 protocol error: bad frame length ${len}`));
        return;
      }
      if (this.rxBuf.length < 4 + len) return;
      const reqid = this.rxBuf.readUInt32LE(4);
      const status = this.rxBuf.readUInt8(8);
      const body = this.rxBuf.subarray(9, 4 + len);
      this.rxBuf = this.rxBuf.subarray(4 + len);

      const p = this.pending;
      if (!p || p.reqid !== reqid) {
        this.onFailure(new Error(`kbd4 protocol error: unexpected response reqid ${reqid}`));
        return;
      }
      this.pending = null;
      clearTimeout(p.timer);
      p.resolve({ status, body: Buffer.from(body) });
    }
  }

  private onFailure(e: Error): void {
    const p = this.pending;
    this.pending = null;
    if (p) {
      clearTimeout(p.timer);
      p.reject(e);
    }
    this.destroyNow();
  }

  private destroyNow(): void {
    if (this.sock) {
      this.sock.removeAllListeners();
      this.sock.destroy();
      this.sock = null;
    }
  }
}

// ------------------------------------------------------------------ helpers

function repoRoot(): string {
  const here = path.dirname(fileURLToPath(import.meta.url));
  // dist/src/daemon_client.js → repo root is two up; src/ (ts) → one up.
  for (const up of ['../..', '..']) {
    const cand = path.resolve(here, up, 'native', 'kbd4');
    if (fs.existsSync(cand)) return path.resolve(here, up);
  }
  return path.resolve(here, '..', '..');
}

export function defaultBinaryPath(): string {
  return path.join(repoRoot(), 'native', 'kbd4');
}

function freeLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address();
      if (addr === null || typeof addr === 'string') { srv.close(); reject(new Error('no port')); return; }
      const port = addr.port;
      srv.close(() => resolve(port));
    });
  });
}

// re-exported for callers building raw op payloads
export { wireDir };
