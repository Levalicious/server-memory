#!/usr/bin/env node
/*
 * Compare op-bench JSON outputs (from native/op_bench), base vs head, and flag
 * per-op regressions. No committed baselines: CI runs the SAME harness against
 * base and head graph sources on one runner, interleaved N times, and diffs.
 *
 *   node scripts/bench-compare.mjs --base b1.json,b2.json --head h1.json,h2.json
 *                                  [--pct 5] [--cyc 40] [--metric min]
 *
 * Methodology (for noisy shared CI runners):
 *  - Each side may have N runs; we take the MEDIAN of the per-run `min` per op.
 *  - We FREQUENCY-CORRECT head to base using the `reference` op (fixed work):
 *    invariant TSC counts wall-clock, so the reference's cycles track CPU
 *    frequency; scaling head by ref_base/ref_head cancels the global turbo drift
 *    that makes identical code look 5-13% different.
 * Exits 1 if any op regressed beyond max(cyc, pct%) — wire to a SOFT-fail check.
 */
import { readFileSync } from 'fs';

function flag(name, def) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : def;
}
const baseFiles = (flag('--base', '') || '').split(',').filter(Boolean);
const headFiles = (flag('--head', '') || '').split(',').filter(Boolean);
if (!baseFiles.length || !headFiles.length) {
  console.error('usage: bench-compare --base b1.json[,b2,...] --head h1.json[,h2,...] [--pct N] [--cyc N] [--metric min|p50]');
  process.exit(2);
}
const pct = Number(flag('--pct', '5'));
const cycFloor = Number(flag('--cyc', '40')); // ~15 ns at ~2.6 GHz
const metric = flag('--metric', 'p50');

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const n = s.length;
  if (!n) return 0;
  return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}
function mergeRuns(files) {
  const runs = files.map((f) => JSON.parse(readFileSync(f, 'utf8')).ops);
  const out = {};
  for (const op of Object.keys(runs[0])) {
    out[op] = median(runs.map((r) => r[op]?.[metric]).filter((x) => x != null));
  }
  return out;
}

const base = mergeRuns(baseFiles);
const head = mergeRuns(headFiles);

// Frequency-correct head -> base via the reference op.
const corr = base.reference && head.reference ? base.reference / head.reference : 1;

let regressed = false;
const rows = [];
for (const op of Object.keys(head)) {
  if (op === 'reference') continue;
  if (!(op in base)) { rows.push({ op, isNew: true }); continue; }
  const b = base[op];
  const h = Math.round(head[op] * corr);
  const d = h - b;
  const dp = b ? (100 * d / b) : 0;
  const thr = Math.max(cycFloor, (b * pct) / 100);
  const mark = d > thr ? '\u{1F534} REGRESSION' : d < -thr ? '\u{1F7E2} faster' : '';
  if (d > thr) regressed = true;
  rows.push({ op, b, h, d, dp, mark });
}

console.log(`### Per-op benchmark — ${metric} cycles (base vs head)\n`);
console.log(`base runs: ${baseFiles.length}, head runs: ${headFiles.length} (median of mins) · ` +
            `freq-correction ×${corr.toFixed(4)} · threshold head−base > max(${cycFloor} cyc, ${pct}%)\n`);
console.log('| op | base | head* | Δ | Δ% | |');
console.log('|---|---:|---:|---:|---:|---|');
for (const r of rows) {
  if (r.isNew) { console.log(`| \`${r.op}\` | — | — | — | — | new |`); continue; }
  const sd = `${r.d >= 0 ? '+' : ''}${r.d.toFixed(1)}`;
  const sp = `${r.dp >= 0 ? '+' : ''}${r.dp.toFixed(1)}%`;
  console.log(`| \`${r.op}\` | ${r.b} | ${r.h} | ${sd} | ${sp} | ${r.mark} |`);
}
console.log(`\n\\* head frequency-corrected to base via the \`reference\` op.`);
console.log(regressed ? '\n**Regressions detected.**' : '\n_No regressions beyond threshold._');
process.exit(regressed ? 1 : 0);
