#!/usr/bin/env node
/**
 * Quick verification that binary files are readable after migration.
 */
import { StringTable } from '../src/stringtable.js';
import { GraphFile, DIR_FORWARD } from '../src/graphfile.js';
import * as path from 'path';

const inputPath = process.argv[2] || path.join(
  process.env.HOME || '.',
  '.local', 'share', 'memory', 'vscode.json'
);
const dir = path.dirname(inputPath);
const base = path.basename(inputPath, path.extname(inputPath));
const graphPath = path.join(dir, `${base}.graph`);
const strPath = path.join(dir, `${base}.strings`);

const st = new StringTable(strPath);
const gf = new GraphFile(graphPath, st);

const offsets = gf.getAllEntityOffsets();
console.log(`Entity count: ${offsets.length}`);

// Sample first 5 entities
console.log('\nSample entities:');
for (const off of offsets.slice(0, 5)) {
  const rec = gf.readEntity(off);
  const name = st.get(BigInt(rec.nameId));
  const type = st.get(BigInt(rec.typeId));
  const obs: string[] = [];
  if (rec.obs0Id) obs.push(st.get(BigInt(rec.obs0Id)));
  if (rec.obs1Id) obs.push(st.get(BigInt(rec.obs1Id)));
  console.log(`  ${name} [${type}] obs=${obs.length} mtime=${Number(rec.mtime)}`);
}

// Count total relations
let relCount = 0;
for (const off of offsets) {
  const edges = gf.getEdges(off);
  relCount += edges.filter(e => e.direction === DIR_FORWARD).length;
}
console.log(`\nRelation count (forward edges): ${relCount}`);

gf.close();
st.close();
console.log('\nVerification passed.');
