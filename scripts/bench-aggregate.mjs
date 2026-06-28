#!/usr/bin/env node
/*
 * Aggregate N op-bench run JSONs into ONE sample line for the bench-history
 * NDJSON time-series. Keeps the `reference` op value alongside the per-op medians
 * so downstream creep analysis can frequency-normalize across runners and time.
 *
 *   node scripts/bench-aggregate.mjs --runs r1.json,r2.json,... --sha <sha> --date <iso> [--metric min]
 *
 * Emits one line: {"sha","date","reference",<refMin>,"ops":{op:<medianMin>,...}}
 */
import { readFileSync } from 'fs';

function flag(name, def) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : def;
}
const files = (flag('--runs', '') || '').split(',').filter(Boolean);
const sha = flag('--sha', 'unknown');
const date = flag('--date', 'unknown');
const metric = flag('--metric', 'p50');
if (!files.length) {
  console.error('usage: bench-aggregate --runs r1.json[,r2,...] --sha <sha> --date <iso> [--metric min]');
  process.exit(2);
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const n = s.length;
  if (!n) return 0;
  return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

const runs = files.map((f) => JSON.parse(readFileSync(f, 'utf8')).ops);
const ops = {};
for (const op of Object.keys(runs[0])) {
  ops[op] = median(runs.map((r) => r[op]?.[metric]).filter((x) => x != null));
}
const reference = ops.reference || 0;
delete ops.reference;

process.stdout.write(`${JSON.stringify({ sha, date, reference, ops })}\n`);
