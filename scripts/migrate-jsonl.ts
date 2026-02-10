#!/usr/bin/env node
/**
 * migrate-jsonl.ts — Convert a JSONL knowledge graph to binary (GraphFile + StringTable).
 *
 * Usage:
 *   npx tsx scripts/migrate-jsonl.ts [path/to/memory.json]
 *
 * If no path given, defaults to ~/.local/share/memory/vscode.json
 *
 * Creates:
 *   <base>.graph   — binary graph store
 *   <base>.strings — binary string table
 *
 * The original .json file is NOT modified or deleted.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { StringTable } from '../src/stringtable.js';
import { GraphFile, DIR_FORWARD, DIR_BACKWARD, AdjEntry } from '../src/graphfile.js';

interface JsonlEntity {
  type: 'entity';
  name: string;
  entityType: string;
  observations: string[];
  mtime?: number;
  obsMtime?: number;
}

interface JsonlRelation {
  type: 'relation';
  from: string;
  to: string;
  relationType: string;
  mtime?: number;
}

type JsonlLine = JsonlEntity | JsonlRelation;

async function migrate(jsonlPath: string) {
  const dir = path.dirname(jsonlPath);
  const base = path.basename(jsonlPath, path.extname(jsonlPath));
  const graphPath = path.join(dir, `${base}.graph`);
  const strPath = path.join(dir, `${base}.strings`);

  // Safety: don't clobber existing binary files
  if (fs.existsSync(graphPath) || fs.existsSync(strPath)) {
    console.error(`ERROR: Binary files already exist:\n  ${graphPath}\n  ${strPath}`);
    console.error('Delete them first if you want to re-migrate.');
    process.exit(1);
  }

  console.log(`Source: ${jsonlPath}`);
  console.log(`Target: ${graphPath}`);
  console.log(`        ${strPath}`);
  console.log();

  // --- Pass 1: Parse JSONL, collect entities and relations ---
  const entities: JsonlEntity[] = [];
  const relations: JsonlRelation[] = [];
  let lineNum = 0;
  let parseErrors = 0;

  const fileStream = fs.createReadStream(jsonlPath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  for await (const line of rl) {
    lineNum++;
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const obj = JSON.parse(trimmed) as JsonlLine;
      if (obj.type === 'entity') {
        entities.push(obj);
      } else if (obj.type === 'relation') {
        relations.push(obj);
      }
    } catch (e) {
      parseErrors++;
      if (parseErrors <= 5) {
        console.warn(`  WARN: parse error on line ${lineNum}: ${(e as Error).message}`);
      }
    }
  }

  console.log(`Parsed: ${entities.length} entities, ${relations.length} relations`);
  if (parseErrors > 0) {
    console.warn(`  (${parseErrors} lines had parse errors — skipped)`);
  }

  // --- Pass 2: Build binary store ---
  // Start with a generous initial size to reduce remaps.
  // Rough estimate: 64B per entity + ~200B string overhead per entity + 24B per adj entry
  const estimatedSize = Math.max(
    65536,
    entities.length * 300 + relations.length * 100
  );

  const st = new StringTable(strPath, estimatedSize);
  const gf = new GraphFile(graphPath, st, estimatedSize);

  // Create all entities first, build name→offset map
  const nameToOffset = new Map<string, bigint>();
  let created = 0;
  let skippedDuplicates = 0;

  for (const e of entities) {
    if (nameToOffset.has(e.name)) {
      skippedDuplicates++;
      continue;
    }

    const mtime = BigInt(e.mtime ?? 0);
    const obsMtime = BigInt(e.obsMtime ?? 0);

    const rec = gf.createEntity(e.name, e.entityType, mtime, obsMtime);

    // Add observations (max 2)
    const obs = e.observations.slice(0, 2);
    for (const o of obs) {
      // Truncate to 140 chars if needed
      const truncated = o.length > 140 ? o.substring(0, 140) : o;
      gf.addObservation(rec.offset, truncated, obsMtime);
    }

    // Fix timestamps (addObservation clobbers mtime)
    if (obs.length > 0) {
      const updated = gf.readEntity(rec.offset);
      updated.mtime = mtime;
      updated.obsMtime = obsMtime;
      gf.updateEntity(updated);
    }

    nameToOffset.set(e.name, rec.offset);
    created++;

    if (created % 1000 === 0) {
      process.stdout.write(`  Entities: ${created}/${entities.length}\r`);
    }
  }
  console.log(`  Entities: ${created} created, ${skippedDuplicates} duplicates skipped`);

  // Create all relations
  let relCreated = 0;
  let relSkipped = 0;

  for (const r of relations) {
    const fromOffset = nameToOffset.get(r.from);
    const toOffset = nameToOffset.get(r.to);

    if (fromOffset === undefined || toOffset === undefined) {
      relSkipped++;
      continue;
    }

    const mtime = BigInt(r.mtime ?? 0);
    const relTypeId = Number(st.intern(r.relationType));

    // Forward edge on 'from'
    const forwardEntry: AdjEntry = {
      targetOffset: toOffset,
      direction: DIR_FORWARD,
      relTypeId,
      mtime,
    };
    gf.addEdge(fromOffset, forwardEntry);

    // Backward edge on 'to' (intern again to bump refcount)
    const relTypeId2 = Number(st.intern(r.relationType));
    const backwardEntry: AdjEntry = {
      targetOffset: fromOffset,
      direction: DIR_BACKWARD,
      relTypeId: relTypeId2,
      mtime,
    };
    gf.addEdge(toOffset, backwardEntry);

    relCreated++;

    if (relCreated % 1000 === 0) {
      process.stdout.write(`  Relations: ${relCreated}/${relations.length}\r`);
    }
  }
  console.log(`  Relations: ${relCreated} created, ${relSkipped} skipped (missing endpoints)`);

  // Sync and close
  gf.sync();
  st.sync();

  // Report sizes
  const graphSize = fs.statSync(graphPath).size;
  const strSize = fs.statSync(strPath).size;
  const jsonlSize = fs.statSync(jsonlPath).size;

  console.log();
  console.log(`File sizes:`);
  console.log(`  JSONL:   ${(jsonlSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Graph:   ${(graphSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Strings: ${(strSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Binary total: ${((graphSize + strSize) / 1024 / 1024).toFixed(2)} MB`);

  gf.close();
  st.close();

  console.log();
  console.log('Migration complete. Original JSONL file preserved.');
}

// --- Main ---
const inputPath = process.argv[2] || path.join(
  process.env.HOME || process.env.USERPROFILE || '.',
  '.local', 'share', 'memory', 'vscode.json'
);

if (!fs.existsSync(inputPath)) {
  console.error(`ERROR: File not found: ${inputPath}`);
  process.exit(1);
}

migrate(inputPath).catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
