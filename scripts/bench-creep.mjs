#!/usr/bin/env node
/*
 * Slow-creep detector for the bench-history NDJSON time-series. The per-PR gate
 * (bench-compare) only sees head-vs-base, so a change that adds a sliver per PR
 * compounds invisibly. This compares the newest sample's reference-NORMALIZED
 * per-op cost (op/reference — frequency-independent across runners and time)
 * against a rolling baseline (median over a recent window of history).
 *
 *   node scripts/bench-creep.mjs --history history.ndjson [--sample sample.json]
 *                                [--pct 10] [--window 20]
 *
 * With --sample, that sample is the "now" and the whole file is the baseline.
 * Without it, the last line of history is "now" and the rest is the baseline.
 * Exits 1 if any op drifted > pct% above the rolling median (soft-fail signal).
 */
import { readFileSync } from 'fs';

function flag(name, def) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : def;
}
const histFile = flag('--history', '');
const sampleFile = flag('--sample', '');
const pct = Number(flag('--pct', '10'));
const cycFloor = Number(flag('--cyc', '40')); // absolute floor: ignore sub-noise wiggle on tiny ops
const windowN = Number(flag('--window', '20'));
if (!histFile) {
  console.error('usage: bench-creep --history history.ndjson [--sample sample.json] [--pct N] [--window N]');
  process.exit(2);
}

const lines = readFileSync(histFile, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
let sample;
let baseline;
if (sampleFile) {
  sample = JSON.parse(readFileSync(sampleFile, 'utf8'));
  baseline = lines;
} else {
  sample = lines[lines.length - 1];
  baseline = lines.slice(0, -1);
}
if (!sample || !baseline.length) {
  console.log('No prior history — creep check skipped (first sample establishes the baseline).');
  process.exit(0);
}

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const n = s.length;
  if (!n) return 0;
  return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}
// reference-normalize: op cost as a fraction of the fixed-work reference op.
function normalize(s) {
  const o = {};
  for (const k of Object.keys(s.ops)) o[k] = s.reference ? s.ops[k] / s.reference : s.ops[k];
  return o;
}

const win = baseline.slice(-windowN).map(normalize);
const now = normalize(sample);

// Convert a normalized delta back to approximate cycles at this sample's
// frequency, so a tiny op's large % wiggle below the absolute floor is ignored.
const refNow = sample.reference || 1;
let creep = false;
const rows = [];
for (const op of Object.keys(now)) {
  const base = median(win.map((w) => w[op]).filter((x) => x != null));
  if (!base) continue;
  const c = now[op];
  const dp = (100 * (c - base)) / base;
  const dcyc = (c - base) * refNow;
  const isCreep = dp > pct && dcyc > cycFloor;
  const isFaster = dp < -pct && -dcyc > cycFloor;
  const mark = isCreep ? '\u{1F534} CREEP' : isFaster ? '\u{1F7E2} improved' : '';
  if (isCreep) creep = true;
  rows.push({ op, base, c, dp, dcyc, mark });
}

console.log(`### Bench creep — normalized cost vs ${win.length}-sample rolling baseline\n`);
console.log(`sample: \`${String(sample.sha || '').slice(0, 9)}\` (${sample.date || '?'}) · flag drift > ${pct}% AND > ${cycFloor} cyc above rolling median\n`);
console.log('| op | baseline | now | Δ% | Δcyc | |');
console.log('|---|---:|---:|---:|---:|---|');
for (const r of rows) {
  console.log(`| \`${r.op}\` | ${r.base.toFixed(4)} | ${r.c.toFixed(4)} | ${r.dp >= 0 ? '+' : ''}${r.dp.toFixed(1)}% | ${r.dcyc >= 0 ? '+' : ''}${Math.round(r.dcyc)} | ${r.mark} |`);
}
console.log(creep ? '\n**Creep detected.**' : '\n_No creep beyond threshold._');
process.exit(creep ? 1 : 0);
