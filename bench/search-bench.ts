/**
 * Benchmark: trigram inverted index vs. linear regex scan for `search_nodes`.
 *
 * Opens a GraphFile + StringTable read-only and runs the same query set
 * through two filtering kernels:
 *
 *   baseline: full linear scan (what `searchNodes` does today)
 *     for each entity: regex.test(name) || regex.test(type) || obs.some(regex.test)
 *
 *   trigram:  required-trigram filter + post-filter
 *     for each query, given the user-supplied required-literal set,
 *     intersect posting lists for the literal's trigrams to produce a
 *     candidate set, then run regex.test on those candidates only.
 *
 * The trigram impl here takes hand-supplied required literals. A production
 * impl would parse the regex AST and extract literals via Cox's algorithm
 * (Russ Cox, "Regular Expression Matching with a Trigram Index"). That parser
 * is out of scope for the benchmark — what we want to know is whether the
 * speedup is worth building the parser.
 *
 * Usage:
 *   npm run build && node dist/bench/search-bench.js [path-to-kb-base]
 *
 * Default KB path: /home/lev/.local/share/memory/vscode (the active KB).
 */

import path from 'path';
import { GraphFile, type EntityRecord } from '../src/graphfile.js';
import { StringTable } from '../src/stringtable.js';
import { TrigramIndex } from '../src/trigram.js';

interface Entity {
  name: string;
  entityType: string;
  observations: string[];
}

interface BenchQuery {
  /** Source for `new RegExp(source, 'i')`. */
  regex: string;
  /**
   * Hand-supplied required-literal set the trigram impl will use to derive
   * candidate trigrams. Each literal must be ≥ 3 chars or it adds no filter
   * (and we'd fall back to scan). Empty array = "no trigram filter possible,
   * scan all entities" (e.g. `.*`, single char, etc.).
   *
   * Cox semantics:
   *  - one literal "foo" = REQUIRE all trigrams of "foo"
   *  - multiple literals = OR — candidate is union of per-literal candidate sets
   *    (this matches alternation regexes like `foo|bar`)
   */
  literals: string[];
  /** Human label for the report. */
  label: string;
}

const KB_BASE = process.argv[2] ?? '/home/lev/.local/share/memory/vscode';

// -------------------------------------------------------------------------
// Open KB
// -------------------------------------------------------------------------

console.log(`opening KB at ${KB_BASE}.{graph,strings}`);
const dir = path.dirname(KB_BASE);
const base = path.basename(KB_BASE);
const stringsPath = path.join(dir, `${base}.strings`);
const graphPath = path.join(dir, `${base}.graph`);

const st = new StringTable(stringsPath);
const gf = new GraphFile(graphPath, st);

gf.lockShared();
let entities: Entity[];
try {
  gf.refresh();
  st.refresh();

  const recordToEntity = (rec: EntityRecord): Entity => {
    const name = st.get(BigInt(rec.nameId));
    const entityType = st.get(BigInt(rec.typeId));
    const observations: string[] = [];
    if (rec.obs0Id !== 0) observations.push(st.get(BigInt(rec.obs0Id)));
    if (rec.obs1Id !== 0) observations.push(st.get(BigInt(rec.obs1Id)));
    return { name, entityType, observations };
  };

  const offsets = gf.getAllEntityOffsets();
  entities = offsets.map(o => recordToEntity(gf.readEntity(o)));
} finally {
  gf.unlock();
}

console.log(`loaded ${entities.length} entities`);

// -------------------------------------------------------------------------
// Build trigram index — uses the production TrigramIndex
// -------------------------------------------------------------------------

console.log(`building trigram index over ${entities.length} entities…`);

/**
 * The production TrigramIndex keys posting lists by string-ID (bigint). For
 * the bench we don't have separate string-IDs per field; instead we use the
 * entity index (as a bigint) as the ID, and concatenate name + type + obs
 * into one indexed text per entity. This keeps the bench direct.
 */
const idx = new TrigramIndex();
const t0 = process.hrtime.bigint();
idx.rebuild((function* () {
  for (let i = 0; i < entities.length; i++) {
    const e = entities[i];
    const text = e.name + '\n' + e.entityType + '\n' + e.observations.join('\n');
    yield [BigInt(i), text] as const;
  }
})());
const t1 = process.hrtime.bigint();
const buildMs = Number(t1 - t0) / 1e6;
console.log(`  built in ${buildMs.toFixed(1)}ms, ${idx.distinctTrigrams} distinct trigrams, ${idx.size} strings indexed`);

// -------------------------------------------------------------------------
// Filtering kernels
// -------------------------------------------------------------------------

function baselineFilter(regex: RegExp, entities: Entity[]): number[] {
  const matches: number[] = [];
  for (let i = 0; i < entities.length; i++) {
    const e = entities[i];
    if (regex.test(e.name) || regex.test(e.entityType) || e.observations.some(o => regex.test(o))) {
      matches.push(i);
    }
  }
  return matches;
}

function trigramFilter(regex: RegExp, entities: Entity[], literals: string[], idx: TrigramIndex): number[] {
  const candidateIds = idx.candidates(literals);
  let candidates: Iterable<number>;
  if (candidateIds === null) {
    // Either no literals or an unindexable literal — must full-scan.
    candidates = (function* () { for (let i = 0; i < entities.length; i++) yield i; })();
  } else {
    candidates = (function* () { for (const id of candidateIds) yield Number(id); })();
  }
  const matches: number[] = [];
  for (const i of candidates) {
    const e = entities[i];
    if (regex.test(e.name) || regex.test(e.entityType) || e.observations.some(o => regex.test(o))) {
      matches.push(i);
    }
  }
  return matches;
}

// -------------------------------------------------------------------------
// Query set
// -------------------------------------------------------------------------

const queries: BenchQuery[] = [
  // Common single-word substring queries — typical "find anything about X".
  { label: 'memory',                  regex: 'memory',                 literals: ['memory'] },
  { label: 'graph',                   regex: 'graph',                  literals: ['graph'] },
  { label: 'pagerank',                regex: 'pagerank',               literals: ['pagerank'] },
  { label: 'StringTable',             regex: 'StringTable',            literals: ['stringtable'] },
  { label: 'lock',                    regex: 'lock',                   literals: ['lock'] },
  { label: 'observation',             regex: 'observation',            literals: ['observation'] },
  { label: 'concurrent',              regex: 'concurrent',             literals: ['concurrent'] },
  { label: 'rebuildNameIndex',        regex: 'rebuildNameIndex',       literals: ['rebuildnameindex'] },

  // Anchored exact name queries — these should be highly selective.
  { label: '^Self$',                  regex: '^Self$',                 literals: ['self'] },
  { label: '^Lev$',                   regex: '^Lev$',                  literals: [] }, // 'Lev' is < 3 trigrams (length 3 → 1 trigram), still ok to filter
  { label: '^Claude$',                regex: '^Claude$',               literals: ['claude'] },

  // Alternations — typical multi-term LLM query.
  { label: 'memory|graph',            regex: 'memory|graph',           literals: ['memory', 'graph'] },
  { label: 'foo|bar|baz|qux',         regex: 'foo|bar|baz|qux',        literals: ['foo', 'bar', 'baz', 'qux'] },

  // Worst-case: no literals at all → must full-scan.
  { label: '.*',                      regex: '.*',                     literals: [] },
  { label: 'a.c',                     regex: 'a.c',                    literals: [] },
];

// -------------------------------------------------------------------------
// Run benchmark
// -------------------------------------------------------------------------

const ITERATIONS = 50;
const WARMUP = 5;

interface Result {
  label: string;
  count: number;
  baselineUs: number;
  trigramUs: number;
  speedup: number;
  match: boolean;
}

const results: Result[] = [];

for (const q of queries) {
  const re = new RegExp(q.regex, 'i');

  // Warmup
  for (let w = 0; w < WARMUP; w++) {
    baselineFilter(re, entities);
    trigramFilter(re, entities, q.literals, idx);
  }

  // Baseline timing
  const baselineRuns: number[] = [];
  let baselineCount = 0;
  for (let r = 0; r < ITERATIONS; r++) {
    const t0 = process.hrtime.bigint();
    const matches = baselineFilter(re, entities);
    const t1 = process.hrtime.bigint();
    baselineRuns.push(Number(t1 - t0));
    baselineCount = matches.length;
  }

  // Trigram timing
  const trigramRuns: number[] = [];
  let trigramCount = 0;
  for (let r = 0; r < ITERATIONS; r++) {
    const t0 = process.hrtime.bigint();
    const matches = trigramFilter(re, entities, q.literals, idx);
    const t1 = process.hrtime.bigint();
    trigramRuns.push(Number(t1 - t0));
    trigramCount = matches.length;
  }

  baselineRuns.sort((a, b) => a - b);
  trigramRuns.sort((a, b) => a - b);
  const baselineMedianNs = baselineRuns[Math.floor(baselineRuns.length / 2)];
  const trigramMedianNs = trigramRuns[Math.floor(trigramRuns.length / 2)];

  results.push({
    label: q.label,
    count: baselineCount,
    baselineUs: baselineMedianNs / 1000,
    trigramUs: trigramMedianNs / 1000,
    speedup: baselineMedianNs / trigramMedianNs,
    match: baselineCount === trigramCount,
  });
}

// -------------------------------------------------------------------------
// Report
// -------------------------------------------------------------------------

const pad = (s: string | number, n: number, right = false) => {
  const str = String(s);
  return right ? str.padStart(n) : str.padEnd(n);
};

console.log('');
console.log('Query                              matches    baseline μs  trigram μs    speedup    match');
console.log('-'.repeat(95));
for (const r of results) {
  console.log(
    pad(r.label, 32) +
    pad(r.count, 11, true) + '   ' +
    pad(r.baselineUs.toFixed(1), 11, true) + '  ' +
    pad(r.trigramUs.toFixed(1), 11, true) + '   ' +
    pad((r.speedup >= 1 ? r.speedup.toFixed(2) + 'x' : '(' + (1 / r.speedup).toFixed(2) + 'x slower)'), 18, true) + '   ' +
    (r.match ? 'OK' : 'MISMATCH')
  );
}

// Aggregate
const filterableResults = results.filter(r => r.speedup > 1);
if (filterableResults.length > 0) {
  const avgSpeedup = filterableResults.reduce((s, r) => s + r.speedup, 0) / filterableResults.length;
  const maxSpeedup = Math.max(...filterableResults.map(r => r.speedup));
  console.log('');
  console.log(`avg speedup over ${filterableResults.length} filterable queries: ${avgSpeedup.toFixed(2)}x`);
  console.log(`max speedup: ${maxSpeedup.toFixed(2)}x`);
}

const slowdowns = results.filter(r => r.speedup < 1);
if (slowdowns.length > 0) {
  console.log(`${slowdowns.length} queries are slower with trigram (likely no-literal fallbacks paying overhead).`);
}

gf.close();
st.close();
