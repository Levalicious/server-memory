/**
 * Fuzz test — N concurrent simulated agents, each as a separate server PID,
 * all sharing the same on-disk graph file.
 *
 * This is the *only* faithful reproduction of the production failure mode:
 * single-process fuzzing serializes everything through one event loop and
 * never exercises real `flock(2)` blocking between independent processes.
 * Here every agent is its own `node dist/index.js` child, with its own PID,
 * its own event loop, its own OTel SDK init, and its own kernel file
 * descriptors — exactly the topology that has been deadlocking in the wild
 * (one PID pinning 99% CPU under LOCK_SH while N writers queue on LOCK_EX).
 *
 * Knobs (env-overridable):
 *   FUZZ_AGENTS         number of child server PIDs       (default 30)
 *   FUZZ_BUDGET_MS      total wall-clock for the fuzz     (default 60_000)
 *   FUZZ_CALL_MS        per-call watchdog timeout         (default 20_000)
 *   FUZZ_SEED           PRNG seed (numeric)               (default Date.now())
 *   FUZZ_FORENSIC_DIR   directory for sync-flushed log    (default tmpDir)
 *   FUZZ_KEEP_TMP       keep tmp dir after test for forensics (default 0)
 *
 * Forensics: every call's start and finish are appended *synchronously* to
 * `<FUZZ_FORENSIC_DIR>/fuzz.log` along with the child PID. If a child hangs
 * in a synchronous JS loop the watchdog Promise on the parent side WILL
 * fire (separate event loops) and identify the stuck PID; you can then
 * `gdb -p <pid>` or `cat /proc/<pid>/stack` to see the V8 frame. The last
 * unfinished `start` line in the log gives the tool name and arguments
 * that triggered it.
 *
 * Build requirement: needs `dist/index.js` to exist. The test fails fast
 * with a clear message if it's missing or stale.
 */

import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SERVER_DIST = path.resolve(__dirname, '..', 'dist', 'index.js');

const NUM_AGENTS    = Number(process.env.FUZZ_AGENTS    ?? '30');
const TOTAL_BUDGET  = Number(process.env.FUZZ_BUDGET_MS ?? '60000');
const PER_CALL_MS   = Number(process.env.FUZZ_CALL_MS   ?? '20000');
const SEED          = Number(process.env.FUZZ_SEED      ?? Date.now());
const KEEP_TMP      = process.env.FUZZ_KEEP_TMP === '1';

const POOL_SIZE      = 24;
const ENTITY_TYPES   = ['Person', 'Place', 'Concept', 'Event', 'Object'] as const;
const RELATION_TYPES = ['knows', 'contains', 'caused', 'follows', 'opposes'] as const;
const POOL: readonly string[] = Array.from(
  { length: POOL_SIZE },
  (_, i) => `Node${i.toString().padStart(2, '0')}`,
);

// Mulberry32 — fast deterministic PRNG. Each agent gets its own stream.
function mkRng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(rng: () => number, arr: readonly T[]): T {
  return arr[Math.floor(rng() * arr.length)];
}

interface FuzzOp {
  tool: string;
  args: Record<string, unknown>;
}

function buildOp(rng: () => number): FuzzOp {
  const r = rng();

  // ---- writes (35%) -------------------------------------------------
  if (r < 0.10) {
    return {
      tool: 'create_entities',
      args: {
        entities: [{
          name: pick(rng, POOL),
          entityType: pick(rng, ENTITY_TYPES),
          observations: rng() < 0.5 ? [] : [`obs ${Math.floor(rng() * 1e6)}`],
        }],
      },
    };
  }
  if (r < 0.18) {
    return {
      tool: 'create_relations',
      args: {
        relations: [{
          from: pick(rng, POOL),
          to:   pick(rng, POOL),
          relationType: pick(rng, RELATION_TYPES),
        }],
      },
    };
  }
  if (r < 0.22) {
    return {
      tool: 'add_observations',
      args: {
        observations: [{
          entityName: pick(rng, POOL),
          contents: [`add ${Math.floor(rng() * 1e6)}`],
        }],
      },
    };
  }
  if (r < 0.26) {
    return {
      tool: 'delete_observations',
      args: {
        deletions: [{
          entityName: pick(rng, POOL),
          observations: [`add ${Math.floor(rng() * 1e6)}`],
        }],
      },
    };
  }
  if (r < 0.30) {
    return { tool: 'delete_entities', args: { entityNames: [pick(rng, POOL)] } };
  }
  if (r < 0.35) {
    return {
      tool: 'delete_relations',
      args: {
        relations: [{
          from: pick(rng, POOL),
          to:   pick(rng, POOL),
          relationType: pick(rng, RELATION_TYPES),
        }],
      },
    };
  }

  // ---- reads (65%) --------------------------------------------------
  if (r < 0.45) {
    const k = rng();
    let query: string;
    if (k < 0.30)      query = `find me a thing about ${pick(rng, ENTITY_TYPES)}`;
    else if (k < 0.65) query = pick(rng, POOL);
    else               query = `^${pick(rng, POOL)}|${pick(rng, ENTITY_TYPES)}$`;
    return { tool: 'search_nodes', args: { query } };
  }
  if (r < 0.55) {
    return { tool: 'open_nodes', args: { names: [pick(rng, POOL), pick(rng, POOL)] } };
  }
  if (r < 0.65) {
    return {
      tool: 'get_neighbors',
      args: { entityName: pick(rng, POOL), depth: 1 + Math.floor(rng() * 3) },
    };
  }
  if (r < 0.72) {
    return {
      tool: 'find_path',
      args: {
        fromEntity: pick(rng, POOL),
        toEntity:   pick(rng, POOL),
        maxDepth:   1 + Math.floor(rng() * 5),
      },
    };
  }
  if (r < 0.78) {
    return { tool: 'get_entities_by_type', args: { entityType: pick(rng, ENTITY_TYPES) } };
  }
  if (r < 0.82) return { tool: 'get_entity_types',     args: {} };
  if (r < 0.86) return { tool: 'get_relation_types',   args: {} };
  if (r < 0.90) return { tool: 'get_stats',            args: {} };
  if (r < 0.93) return { tool: 'validate_graph',       args: {} };
  if (r < 0.96) return { tool: 'get_orphaned_entities', args: {} };

  return {
    tool: 'random_walk',
    args: {
      start: pick(rng, POOL),
      depth: 1 + Math.floor(rng() * 10),
      mode:  rng() < 0.5 ? 'merw' : 'uniform',
    },
  };
}

interface CallRecord {
  agentId: number;
  pid: number;
  callId: number;
  tool: string;
  argsPreview: string;
  startedAt: number;
  finishedAt?: number;
  ok?: boolean;
  isError?: boolean;
  err?: string;
  timedOut?: boolean;
  /**
   * On watchdog timeout we probe `/proc/<pid>/stat` to classify the failure:
   *   'cpu_loop'   — child in state R, hot on a CPU (the production bug
   *                  signature: a sync JS loop holding LOCK_SH for hours)
   *   'contention' — child in state S/D (idle in ep_poll, or kernel-blocked
   *                  in flock(2) waiting for LOCK_EX — slow but progressing)
   *   'gone'       — pid no longer exists (crashed)
   */
  hangType?: 'cpu_loop' | 'contention' | 'gone';
}

interface ProcState { state: string; utimeJiffies: number; stimeJiffies: number }

/**
 * Snapshot a Linux process's run-state and accumulated CPU jiffies.
 * Returns null on Darwin/Windows or if the pid has exited.
 *
 * The /proc/[pid]/stat format wraps `comm` in parens which can themselves
 * contain spaces — so we slice from the last `)` and index the remaining
 * fields by their position-after-state (state is field 3 overall).
 */
function probeProcState(pid: number): ProcState | null {
  if (process.platform !== 'linux' || pid <= 0) return null;
  try {
    const raw = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const lastParen = raw.lastIndexOf(')');
    if (lastParen < 0) return null;
    const after = raw.slice(lastParen + 2).split(' ');
    // after[0]=state, after[11]=utime, after[12]=stime  (per `man 5 proc`)
    return {
      state:        after[0],
      utimeJiffies: Number(after[11]) || 0,
      stimeJiffies: Number(after[12]) || 0,
    };
  } catch {
    return null;
  }
}

type RaceResult<T> =
  | { kind: 'ok';      value: T }
  | { kind: 'err';     err: Error }
  | { kind: 'timeout' };

async function withWatchdog<T>(p: Promise<T>, ms: number): Promise<RaceResult<T>> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race<RaceResult<T>>([
      p
        .then((v): RaceResult<T> => ({ kind: 'ok', value: v }))
        .catch((e: unknown): RaceResult<T> => ({ kind: 'err', err: e as Error })),
      new Promise<RaceResult<T>>((resolve) => {
        timer = setTimeout(() => resolve({ kind: 'timeout' }), ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

interface Agent {
  id: number;
  client: Client;
  transport: StdioClientTransport;
  pid: number;
  /** Per-agent stderr fd. Closed in afterAll to prevent fd leaks. */
  stderrFd: number;
  /** ms-since-epoch when transport.onclose fired (child exited or stdio EOF) */
  closedAt?: number;
  /** message from transport.onerror (e.g. spawn errors) */
  errorMsg?: string;
}

/**
 * Best-effort: query systemd-coredump for crashes during the test window
 * and return a `pid → "TIME SIGNAL"` map for any of `candidatePids` that
 * dumped core. Returns empty map if coredumpctl is missing or no matches.
 */
function getCrashedPids(testStartMs: number, candidatePids: readonly number[]): Map<number, string> {
  const result = new Map<number, string>();
  if (process.platform !== 'linux' || candidatePids.length === 0) return result;
  let out: string;
  try {
    // coredumpctl --since takes LOCAL wall-clock time (not UTC), so format
    // testStartMs as "YYYY-MM-DD HH:MM:SS" in the parent's local zone.
    const d = new Date(testStartMs);
    const pad = (n: number): string => String(n).padStart(2, '0');
    const since =
      `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
      `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    out = execSync(`coredumpctl list --since="${since}" --no-pager 2>/dev/null`, {
      encoding: 'utf8',
      timeout: 5000,
    });
  } catch {
    return result;
  }
  const candidates = new Set<number>(candidatePids);
  // Lines look like:
  // Wed 2026-04-29 11:34:23 EDT 883233 1000 1000 SIGSEGV present  /path/to/node  15.9M
  for (const line of out.split('\n')) {
    const m = line.match(/^\S+\s+\d{4}-\d\d-\d\d\s+(\d\d:\d\d:\d\d)\s+\S+\s+(\d+)\s+\d+\s+\d+\s+(\S+)/);
    if (!m) continue;
    const pid = Number(m[2]);
    if (candidates.has(pid)) result.set(pid, `${m[1]} ${m[3]}`);
  }
  return result;
}

async function spawnAgent(id: number, memoryPath: string, stderrFile: string): Promise<Agent> {
  // Pass through OTel env if the user has it set, so the test reproduces
  // the exact bootstrap path of their stuck process. Otherwise, force the
  // SDK off so we don't spam DEADLINE_EXCEEDED retries against a missing
  // collector.
  const env: Record<string, string> = {
    PATH:             process.env.PATH ?? '',
    HOME:             process.env.HOME ?? '',
    NODE_OPTIONS:     '',                        // jest's --experimental-vm-modules would reach the child otherwise
    MEMORY_FILE_PATH: memoryPath,
  };
  if (process.env.OTEL_EXPORTER_OTLP_ENDPOINT) {
    env.OTEL_EXPORTER_OTLP_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    if (process.env.OTEL_SERVICE_NAME) env.OTEL_SERVICE_NAME = process.env.OTEL_SERVICE_NAME;
    if (process.env.OTEL_LOG_LEVEL)    env.OTEL_LOG_LEVEL    = process.env.OTEL_LOG_LEVEL;
  } else {
    env.OTEL_SDK_DISABLED = 'true';
  }

  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER_DIST],
    env,
    stderr: 'pipe',
  });

  // The agent record is mutable so the close/error handlers can record
  // exit timestamps before the agent ever runs a tool call. Easier than
  // monkey-patching the SDK to expose `_process.on('exit')`.
  // Drain stderr to a per-agent file (open synchronously, closed in afterAll).
  const stderrFd = fs.openSync(stderrFile, 'a');

  const agent: Agent = {
    id,
    client: null as unknown as Client,
    transport,
    pid: -1,
    stderrFd,
  };
  transport.onclose = () => { agent.closedAt = Date.now(); };
  transport.onerror = (e: Error) => { agent.errorMsg = e.message; };

  const client = new Client({ name: `fuzz-agent-${id}`, version: '0.0.0' }, { capabilities: {} });
  await client.connect(transport);

  const stderrStream = transport.stderr;
  if (stderrStream) {
    stderrStream.on('data', (chunk: Buffer) => {
      try { fs.writeSync(agent.stderrFd, chunk); } catch { /* ignore */ }
    });
  }

  agent.client = client;
  agent.pid    = transport.pid ?? -1;
  return agent;
}

describe('fuzz — multi-process concurrent agents stress test', () => {
  let tmpDir: string;
  let memoryPath: string;
  let forensicPath: string;
  const agents: Agent[] = [];

  beforeAll(async () => {
    if (!fs.existsSync(SERVER_DIST)) {
      throw new Error(
        `Built server entry not found at ${SERVER_DIST}. Run \`npm run build\` first ` +
        `(this fuzz test must spawn real child server PIDs to exercise OS-level flock contention).`,
      );
    }

    tmpDir       = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-fuzz-'));
    memoryPath   = path.join(tmpDir, 'fuzz.json');
    forensicPath = path.join(process.env.FUZZ_FORENSIC_DIR ?? tmpDir, 'fuzz.log');

    fs.writeFileSync(
      forensicPath,
      `# fuzz seed=${SEED} agents=${NUM_AGENTS} budget=${TOTAL_BUDGET}ms call=${PER_CALL_MS}ms tmp=${tmpDir}\n`,
    );

    // Spawn all children in parallel — boot is the slow part.
    const results = await Promise.allSettled(
      Array.from({ length: NUM_AGENTS }, (_, i) =>
        spawnAgent(i, memoryPath, path.join(tmpDir, `agent-${i}.stderr.log`)),
      ),
    );

    const failures: string[] = [];
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === 'fulfilled') {
        agents.push(r.value);
      } else {
        failures.push(`agent ${i}: ${(r.reason as Error).message}`);
      }
    }
    if (failures.length > 0) {
      // Tear down any that did spawn before throwing.
      for (const a of agents) {
        try { await a.client.close(); } catch { /* */ }
      }
      throw new Error(`Failed to spawn ${failures.length}/${NUM_AGENTS} agents:\n  ${failures.join('\n  ')}`);
    }

    // Print PID map up front so a hang has a `gdb -p` target list.
    const pids = agents.map(a => `${a.id}:${a.pid}`).join(' ');
    process.stderr.write(`[fuzz] spawned ${agents.length} child server PIDs → ${pids}\n`);
    process.stderr.write(`[fuzz] memoryFile=${memoryPath}\n`);
    process.stderr.write(`[fuzz] forensicLog=${forensicPath}\n`);
  }, 120_000);

  afterAll(async () => {
    // Parallel close — each transport.close() sends SIGTERM and awaits exit.
    await Promise.allSettled(agents.map(async (a) => {
      try { await a.client.close(); } catch { /* */ }
    }));

    // Backstop: any child still alive gets SIGKILL'd. Defends against a
    // child stuck in a sync JS loop that ignored SIGTERM.
    for (const a of agents) {
      if (a.pid > 0) {
        try { process.kill(a.pid, 0); /* alive */ process.kill(a.pid, 'SIGKILL'); } catch { /* gone */ }
      }
      // Close per-agent stderr fd to prevent leaks that keep jest's worker
      // event loop alive past test completion.
      try { fs.closeSync(a.stderrFd); } catch { /* */ }
    }

    if (!KEEP_TMP) {
      try {
        for (const f of fs.readdirSync(tmpDir)) {
          try { fs.unlinkSync(path.join(tmpDir, f)); } catch { /* */ }
        }
        fs.rmdirSync(tmpDir);
      } catch { /* best-effort */ }
    } else {
      process.stderr.write(`[fuzz] FUZZ_KEEP_TMP=1 — preserved ${tmpDir}\n`);
    }
  }, 60_000);

  it(
    `runs ${NUM_AGENTS} child server PIDs for ${TOTAL_BUDGET}ms with no stuck calls (seed=${SEED})`,
    async () => {
      const records: CallRecord[] = [];
      const testStartMs = Date.now();
      const deadline = testStartMs + TOTAL_BUDGET;
      let logSeq = 0;

      const FUZZ_DISABLE_LOG = process.env.FUZZ_DISABLE_LOG === '1';
      const logRecord = (rec: CallRecord, phase: 'start' | 'end'): void => {
        if (FUZZ_DISABLE_LOG) return;
        const line = JSON.stringify({ phase, seq: ++logSeq, ...rec }) + '\n';
        try { fs.appendFileSync(forensicPath, line); } catch { /* never throw from logger */ }
      };

      async function agentLoop(a: Agent): Promise<void> {
        const rng = mkRng((SEED ^ (a.id * 0x9e3779b1)) >>> 0);
        let callId = 0;
        let consecutiveTimeouts = 0;

        while (Date.now() < deadline) {
          const { tool, args } = buildOp(rng);
          const startedAt = Date.now();
          const argsPreview = JSON.stringify(args).slice(0, 200);
          const rec: CallRecord = {
            agentId: a.id, pid: a.pid, callId: callId++, tool, argsPreview, startedAt,
          };
          logRecord(rec, 'start');

          const res = await withWatchdog(
            a.client.callTool({ name: tool, arguments: args }),
            PER_CALL_MS,
          );
          rec.finishedAt = Date.now();

          switch (res.kind) {
            case 'ok':
              rec.ok = true;
              rec.isError = (res.value as { isError?: boolean }).isError === true;
              consecutiveTimeouts = 0;
              break;
            case 'err':
              rec.ok = false;
              rec.err = res.err.message?.slice(0, 200) ?? String(res.err);
              consecutiveTimeouts = 0;
              break;
            case 'timeout': {
              rec.timedOut = true;
              consecutiveTimeouts++;

              // Classify the hang. Sample twice ~50ms apart so we can detect
              // CPU progress (utime+stime delta > 0 means the child is
              // actively burning cycles → looks like the production bug).
              const probe1 = probeProcState(a.pid);
              await new Promise(r => setTimeout(r, 50));
              const probe2 = probeProcState(a.pid);
              if (!probe1 || !probe2) {
                rec.hangType = 'gone';
              } else {
                const cpuAdvanced =
                  (probe2.utimeJiffies + probe2.stimeJiffies) >
                  (probe1.utimeJiffies + probe1.stimeJiffies);
                if (probe1.state === 'R' || cpuAdvanced) {
                  rec.hangType = 'cpu_loop';   // production-bug signature
                } else {
                  rec.hangType = 'contention'; // flock queue or idle
                }
              }
              break;
            }
          }

          records.push(rec);
          logRecord(rec, 'end');

          // Bail out only after 3 consecutive timeouts. Single slow calls
          // under flock-EX queueing are expected with N writers — we only
          // want to abandon a child that's truly wedged (a real CPU-loop
          // hang or a series of contention timeouts that aren't draining).
          if (rec.hangType === 'cpu_loop' || consecutiveTimeouts >= 3) return;
        }
      }

      await Promise.all(agents.map(agentLoop));

      // -------- Diagnostic summary --------------------------------------
      interface ToolStats { n: number; toolErrs: number; protoErrs: number; timeouts: number; maxMs: number; sumMs: number }
      const byTool = new Map<string, ToolStats>();
      for (const r of records) {
        let s = byTool.get(r.tool);
        if (!s) {
          s = { n: 0, toolErrs: 0, protoErrs: 0, timeouts: 0, maxMs: 0, sumMs: 0 };
          byTool.set(r.tool, s);
        }
        s.n++;
        if (r.timedOut)         s.timeouts++;
        else if (r.ok === false) s.protoErrs++;
        else if (r.isError)      s.toolErrs++;
        const ms = (r.finishedAt ?? Date.now()) - r.startedAt;
        s.sumMs += ms;
        if (ms > s.maxMs) s.maxMs = ms;
      }

      process.stderr.write(
        `[fuzz] seed=${SEED} agents=${agents.length} totalCalls=${records.length} ` +
        `forensic=${forensicPath}\n`,
      );
      const tools = [...byTool.entries()].sort((a, b) => b[1].n - a[1].n);
      for (const [tool, s] of tools) {
        process.stderr.write(
          `[fuzz]   ${tool.padEnd(24)} n=${String(s.n).padStart(4)} ` +
          `protoErr=${s.protoErrs} toolErr=${s.toolErrs} timeout=${s.timeouts} ` +
          `maxMs=${s.maxMs} avgMs=${Math.round(s.sumMs / Math.max(1, s.n))}\n`,
        );
      }

      // -------- Hang classification ---------------------------------------
      // Two failure modes are distinct and we treat them differently:
      //  - cpu_loop  → production bug signature (sync JS loop holding a
      //                read lock for hours). Test FAILS on any of these.
      //  - contention/gone → expected under N-writer flock-EX queueing.
      //                Warned about but not fatal unless persistent.
      const hangs       = records.filter((r) => r.hangType === 'cpu_loop');
      const contention  = records.filter((r) => r.hangType === 'contention');
      const gone        = records.filter((r) => r.hangType === 'gone');

      // -------- Cross-reference closed transports with coredumpctl -------
      const closedAgents = agents.filter((a) => a.closedAt !== undefined);
      const candidatePids = closedAgents.map((a) => a.pid).filter((p) => p > 0);
      const crashes = getCrashedPids(testStartMs, candidatePids);

      if (closedAgents.length > 0) {
        const crashedCount = crashes.size;
        const closedPids = closedAgents.map((a) =>
          crashes.has(a.pid) ? `${a.pid}!` : `${a.pid}`,
        ).join(',');
        process.stderr.write(
          `[fuzz] ${closedAgents.length} child(ren) exited mid-test ` +
          `(${crashedCount} crashed via coredumpctl): pids=${closedPids}\n`,
        );
        for (const [pid, sig] of crashes) {
          process.stderr.write(`[fuzz]   pid=${pid} ${sig} → coredumpctl info ${pid}\n`);
        }
      }

      if (contention.length > 0 || gone.length > 0) {
        process.stderr.write(
          `[fuzz] non-fatal: ${contention.length} contention timeout(s), ${gone.length} gone\n`,
        );
      }

      // CRASHES are fatal — a child segfaulting under multi-process
      // contention is a real concurrency bug in the native binding /
      // shared-memory layer.
      if (crashes.size > 0) {
        const lines = [...crashes].map(([pid, sig]) => {
          const a = agents.find((x) => x.pid === pid);
          return `pid=${pid} agent=${a?.id ?? '?'} ${sig}`;
        }).join('\n  ');
        throw new Error(
          `${crashes.size} child server PID(s) crashed (SIGSEGV/SIGABRT/etc) ` +
          `during the fuzz run (seed=${SEED}):\n  ${lines}\n` +
          `Run \`coredumpctl info <pid>\` for backtraces. ` +
          `Set FUZZ_KEEP_TMP=1 to also preserve ${tmpDir}.`,
        );
      }

      if (hangs.length > 0) {
        const stuckPids = new Set(hangs.map((r) => r.pid));
        const sample = hangs.slice(0, 5)
          .map((r) => `pid=${r.pid} agent=${r.agentId} call=${r.callId} tool=${r.tool} args=${r.argsPreview}`)
          .join('\n  ');
        const aliveStuck: number[] = [];
        for (const pid of stuckPids) {
          try { process.kill(pid, 0); aliveStuck.push(pid); } catch { /* exited */ }
        }
        throw new Error(
          `PRODUCTION-BUG SIGNATURE: ${hangs.length} CPU-loop hang(s) across ` +
          `${stuckPids.size} child PID(s) — child was in state R (running) and ` +
          `accumulating CPU during the watchdog window (seed=${SEED}):\n  ${sample}\n` +
          (aliveStuck.length > 0
            ? `Still-alive stuck PIDs: ${aliveStuck.join(', ')} ` +
              `→ inspect with \`cat /proc/<pid>/stack\` or \`gdb -p <pid>\` before afterAll SIGKILL.\n` +
              `Set FUZZ_KEEP_TMP=1 to preserve ${tmpDir}.\n`
            : '') +
          `Forensic log: ${forensicPath}`,
        );
      }

      // Liveness probe: every child should still respond to a trivial call,
      // even after a heavy contention run. If a child crashed (e.g. PID gone)
      // or stopped responding, that's also a regression worth catching.
      for (const a of agents) {
        const probe = await withWatchdog(
          a.client.callTool({ name: 'get_stats', arguments: {} }),
          PER_CALL_MS,
        );
        if (probe.kind !== 'ok') {
          // For 'timeout' here we again classify — a liveness-probe timeout
          // in CPU-running state is the hang signature.
          let detail = `kind=${probe.kind}`;
          if (probe.kind === 'err') detail += `, err=${probe.err.message}`;
          if (probe.kind === 'timeout') {
            const ps = probeProcState(a.pid);
            detail += `, procState=${ps?.state ?? 'unknown'}`;
          }
          throw new Error(
            `Post-fuzz liveness probe failed for agent ${a.id} (pid=${a.pid}, ${detail})`,
          );
        }
      }

      expect(records.length).toBeGreaterThan(0);
    },
    TOTAL_BUDGET + 120_000,
  );
});
