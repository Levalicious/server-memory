#!/usr/bin/env node
/**
 * delete-document.ts — Remove a kb_load-style document and its TextChunk chain
 * from the binary knowledge graph.
 *
 * Usage:
 *   MEMORY_FILE_PATH=~/.local/share/memory/vscode.json npx tsx scripts/delete-document.ts <document-entity-name> [--live]
 *
 * Without --live, runs in dry-run mode: walks the chain, counts chunks, prints
 * what would be deleted, but does not mutate anything.
 *
 * With --live, actually deletes the document entity, the index entity (if any),
 * and every TextChunk in the chain.
 */

import { KnowledgeGraphManager } from '../server.js';

const DOC_NAME = process.argv[2];
const LIVE = process.argv.includes('--live');
const BATCH_SIZE = 200;

if (!DOC_NAME) {
  console.error('Usage: npx tsx scripts/delete-document.ts <document-entity-name> [--live]');
  process.exit(1);
}

const memoryFilePath = process.env.MEMORY_FILE_PATH ?? `${process.env.HOME}/.local/share/memory/vscode.json`;
console.log(`Opening graph at: ${memoryFilePath}`);
console.log(`Mode: ${LIVE ? '🔴 LIVE — will delete' : '🟢 DRY RUN — read only'}`);
console.log();

const mgr = new KnowledgeGraphManager(memoryFilePath);

// ── Step 1: Open the document node, find starts_with target ──────────
const docGraph = await mgr.openNodes([DOC_NAME], 'forward');
const docEntity = docGraph.entities.find(e => e.name === DOC_NAME);
if (!docEntity) {
  console.error(`Entity "${DOC_NAME}" not found.`);
  process.exit(1);
}
console.log(`Found document: "${DOC_NAME}" (type: ${docEntity.entityType})`);

const startsWithRel = docGraph.relations.find(r => r.relationType === 'starts_with');
if (!startsWithRel) {
  console.error(`No "starts_with" relation found on "${DOC_NAME}". Is this a kb_load document?`);
  process.exit(1);
}

const headChunkName = startsWithRel.to;
console.log(`Head chunk: ${headChunkName}`);

// ── Step 2: Walk the chain via "follows" relations ───────────────────
const toDelete: string[] = [];
let currentName = headChunkName;
let visited = 0;

while (currentName) {
  toDelete.push(currentName);
  visited++;

  if (visited % 500 === 0) {
    process.stdout.write(`  … walked ${visited} chunks\r`);
  }

  // Find the "follows" relation from this chunk
  const chunkGraph = await mgr.openNodes([currentName], 'forward');
  const followsRel = chunkGraph.relations.find(r => r.relationType === 'follows');
  currentName = followsRel ? followsRel.to : '';
}

console.log(`\nChain walk complete: ${toDelete.length} TextChunks found.`);

// ── Step 3: Check for an index entity ────────────────────────────────
const indexName = `${DOC_NAME}__index`;
const indexGraph = await mgr.openNodes([indexName], 'forward');
const indexEntity = indexGraph.entities.find(e => e.name === indexName);

const extraDeletes: string[] = [DOC_NAME];
if (indexEntity) {
  extraDeletes.push(indexName);
  console.log(`Index entity found: "${indexName}"`);
} else {
  console.log(`No index entity "${indexName}" found (old-style import).`);
}

const totalDeletes = extraDeletes.length + toDelete.length;
console.log(`\nTotal entities to delete: ${totalDeletes} (${extraDeletes.length} header + ${toDelete.length} chunks)`);

if (!LIVE) {
  console.log('\n✅ Dry run complete. Re-run with --live to actually delete.');
  process.exit(0);
}

// ── Step 4: Delete in batches ────────────────────────────────────────
console.log(`\nDeleting ${totalDeletes} entities in batches of ${BATCH_SIZE}...`);

// Delete chunks first (the bulk), then the header entities
let deleted = 0;
for (let i = 0; i < toDelete.length; i += BATCH_SIZE) {
  const batch = toDelete.slice(i, i + BATCH_SIZE);
  await mgr.deleteEntities(batch);
  deleted += batch.length;
  process.stdout.write(`  Deleted ${deleted}/${toDelete.length} chunks\r`);
}
console.log(`\n  Chunks done.`);

// Delete document + index
await mgr.deleteEntities(extraDeletes);
console.log(`  Deleted document header${indexEntity ? ' + index' : ''}.`);

console.log(`\n🔴 Done. Removed ${totalDeletes} entities from the graph.`);
