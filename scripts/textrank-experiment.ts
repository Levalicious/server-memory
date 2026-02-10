#!/usr/bin/env node
/**
 * textrank-experiment.ts — Chunking + Document Vectors
 *
 * Takes a document, produces:
 *   1. TextChunk chain (same scheme as kb_load.py) — the structural backbone
 *   2. TF-IDF weight vector for the WHOLE DOCUMENT
 *   3. Cosine similarity function over subvectors (for chunk-chunk comparison)
 *
 * The weight vector is document-level. Chunks project into it via their word sets.
 *
 * Toggle USE_LOG_TF to switch between raw TF and log(1+TF).
 *
 * Usage:
 *   MEMORY_FILE_PATH=~/.local/share/memory/vscode.json \
 *     npx tsx scripts/textrank-experiment.ts <file>
 */

import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { StringTable } from '../src/stringtable.js';

// ─── Configuration ──────────────────────────────────────────────────

/** Flip this to switch between log(1 + rawCount) and rawCount for TF. */
const USE_LOG_TF = false;

// KB constraints (matching kb_load.py)
const MAX_OBS_LENGTH = 140;
const MAX_OBS_PER_ENTITY = 2;

// ─── Data Structures ────────────────────────────────────────────────

interface Word {
  text: string;       // raw token as it appears
  normalized: string; // lowercased for vector keying
  start: number;      // offset in normalized string
  end: number;        // offset end (exclusive)
}

interface Observation {
  text: string;
  start: number; // offset in normalized string
  end: number;   // offset end (exclusive)
  words: Word[];
}

interface Chunk {
  index: number;        // position in chain
  observations: Observation[];
}

// ─── Helpers ────────────────────────────────────────────────────────

/** String length as JS counts it (UTF-16 code units). */
function jsLength(s: string): number {
  // In JS, string.length already returns UTF-16 code units
  return s.length;
}

/** Find every space-delimited word in text, with offsets relative to `offset`. */
function labelWords(text: string, offset: number): Word[] {
  const words: Word[] = [];
  let i = 0;
  const n = text.length;
  while (i < n) {
    // Skip spaces
    while (i < n && text[i] === ' ') i++;
    if (i >= n) break;
    // Start of word
    const start = i;
    while (i < n && text[i] !== ' ') i++;
    const raw = text.slice(start, i);
    words.push({
      text: raw,
      normalized: raw.toLowerCase(),
      start: offset + start,
      end: offset + i,
    });
  }
  return words;
}

// ─── Splitting ──────────────────────────────────────────────────────

/** Split normalized text into observations of max 140 JS chars, word-boundary aligned. */
function splitIntoObservations(text: string): Observation[] {
  const observations: Observation[] = [];
  let pos = 0;

  while (pos < text.length) {
    const remaining = text.slice(pos);

    if (jsLength(remaining) <= MAX_OBS_LENGTH) {
      observations.push({
        text: remaining,
        start: pos,
        end: pos + remaining.length,
        words: labelWords(remaining, pos),
      });
      break;
    }

    // Find last space that keeps JS length <= 140
    let splitAt = 0;
    for (let i = 0; i < remaining.length; i++) {
      if (remaining[i] === ' ') {
        if (jsLength(remaining.slice(0, i)) <= MAX_OBS_LENGTH) {
          splitAt = i;
        } else {
          break;
        }
      }
    }

    if (splitAt === 0) {
      // No space fits — hard split at length boundary
      let jsLen = 0;
      for (let i = 0; i < remaining.length; i++) {
        const charLen = remaining.codePointAt(i)! > 0xFFFF ? 2 : 1;
        if (jsLen + charLen > MAX_OBS_LENGTH) {
          splitAt = i;
          break;
        }
        jsLen += charLen;
        // Skip surrogate pair
        if (charLen === 2) i++;
      }
      if (splitAt === 0) splitAt = remaining.length;
    }

    const obsText = remaining.slice(0, splitAt).trimEnd();
    observations.push({
      text: obsText,
      start: pos,
      end: pos + obsText.length,
      words: labelWords(obsText, pos),
    });

    pos += splitAt;
    // Skip whitespace after split
    while (pos < text.length && text[pos] === ' ') pos++;
  }

  return observations;
}

/** Group observations into chunks of up to 2 (one entity each). */
function chunkObservations(observations: Observation[]): Chunk[] {
  const chunks: Chunk[] = [];
  for (let i = 0; i < observations.length; i += MAX_OBS_PER_ENTITY) {
    chunks.push({
      index: chunks.length,
      observations: observations.slice(i, i + MAX_OBS_PER_ENTITY),
    });
  }
  return chunks;
}

// ─── Accessors ──────────────────────────────────────────────────────

/** Get all words across a chunk's observations. */
function _chunkWords(chunk: Chunk): Word[] {
  const words: Word[] = [];
  for (const obs of chunk.observations) {
    words.push(...obs.words);
  }
  return words;
}

/** Get the full text of a chunk (all observations joined). */
function _chunkText(chunk: Chunk): string {
  return chunk.observations.map(o => o.text).join(' ');
}

// ─── Pipeline ───────────────────────────────────────────────────────

/** Normalize text the same way kb_load.py does. */
function normalize(text: string): string {
  text = text.replace(/\r\n/g, '\n');
  text = text.replace(/[ \t]+/g, ' ');
  text = text.replace(/\n{3,}/g, '\n\n');
  text = text.trim();
  return text.split(/\s+/).join(' ');
}

function generateId(): string {
  return crypto.randomBytes(12).toString('hex');
}

/** Full pipeline: normalize -> observations -> chunks with labeled words + IDs. */
export function processDocument(text: string): { chunks: Chunk[]; allWords: Word[] } {
  const normalized = normalize(text);
  const observations = splitIntoObservations(normalized);
  const chunks = chunkObservations(observations);

  // Assign hex IDs
  for (const chunk of chunks) {
    (chunk as any).id = generateId();
  }

  // Collect ALL words across the entire document
  const allWords: Word[] = [];
  for (const chunk of chunks) {
    for (const obs of chunk.observations) {
      allWords.push(...obs.words);
    }
  }

  return { chunks, allWords };
}

// ─── TF-IDF Vector ──────────────────────────────────────────────────

/**
 * Build the document's TF-IDF weight vector.
 * word → tf(word) * idf(word)
 *
 * TF is either raw count or log(1 + count) depending on USE_LOG_TF.
 * IDF is log(totalCorpusWords / (1 + wordFreq)) + 1.
 */
function buildWeightVector(
  allWords: Word[],
  idf: Map<string, number>,
): Map<string, number> {
  // Raw counts
  const rawCounts = new Map<string, number>();
  for (const w of allWords) {
    rawCounts.set(w.normalized, (rawCounts.get(w.normalized) ?? 0) + 1);
  }

  const weights = new Map<string, number>();
  for (const [word, raw] of rawCounts) {
    const tf = USE_LOG_TF ? Math.log(1 + raw) : raw;
    const idfW = idf.get(word) ?? 0;
    weights.set(word, tf * idfW);
  }
  return weights;
}

// ─── Cosine Similarity ──────────────────────────────────────────────

/**
 * Cosine similarity between two subsets of the document weight vector.
 *
 * Given the full weight vector W and two sets of word keys A and B,
 * computes cosine(W|_A, W|_B) where W|_X is the subvector restricted
 * to the dimensions in X.
 *
 * The shared dimensions (A ∩ B) contribute to the dot product.
 * Each side's norm is computed over its own dimensions only.
 *
 * Returns 0 if either subvector has zero norm.
 */
function cosineSimilarity(
  weights: Map<string, number>,
  keysA: Set<string>,
  keysB: Set<string>,
): number {
  // Dot product: only shared dimensions
  let dot = 0;
  for (const word of keysA) {
    if (keysB.has(word)) {
      const w = weights.get(word) ?? 0;
      dot += w * w;  // same vector, so weight(word) appears on both sides
    }
  }

  // Norms: each over its own dimensions
  let normA = 0;
  for (const word of keysA) {
    const w = weights.get(word) ?? 0;
    normA += w * w;
  }

  let normB = 0;
  for (const word of keysB) {
    const w = weights.get(word) ?? 0;
    normB += w * w;
  }

  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  return denom === 0 ? 0 : dot / denom;
}

// ─── IDF Vector (from string table) ─────────────────────────────────

/**
 * Derive classic document-frequency IDF from the string table.
 *
 * For each interned string: find the unique words it contains.
 * Each unique word gets +refcount to its document frequency
 * (refcount = number of entities sharing that string = number of
 * "documents" it appears in).
 *
 * N = sum of all refcounts (total entity-string references).
 * df(word) = number of entity-string references containing that word.
 * IDF(word) = log(N / (1 + df(word))) + 1
 *
 * Returns { df, corpusSize }.
 */
function deriveCorpusDocFreqs(st: StringTable): { df: Map<string, number>; corpusSize: number } {
  const df = new Map<string, number>();
  let corpusSize = 0;

  for (const entry of st.entries()) {
    corpusSize += entry.refcount;
    // Unique words in this string (presence, not count)
    const uniqueWords = new Set(
      entry.text.toLowerCase().split(/\s+/).filter(w => w.length > 0)
    );
    for (const word of uniqueWords) {
      df.set(word, (df.get(word) ?? 0) + entry.refcount);
    }
  }

  return { df, corpusSize };
}

/**
 * Build IDF vector using classic document-frequency IDF.
 * IDF(word) = log(N / (1 + df(word))) + 1
 */
function buildIdfVector(
  docVocab: Set<string>,
  df: Map<string, number>,
  corpusSize: number,
): Map<string, number> {
  const idf = new Map<string, number>();
  for (const word of docVocab) {
    const docFreq = df.get(word) ?? 0;
    idf.set(word, Math.log(corpusSize / (1 + docFreq)) + 1);
  }

  return idf;
}

// ─── TextRank Core ──────────────────────────────────────────────────

const TEXTRANK_DAMPING = 0.85;
const TEXTRANK_ITERATIONS = 30000;
const TEXTRANK_CONVERGENCE = 1e-6;

/**
 * Generic PageRank over any square weight matrix.
 * Returns scores[i] for each node.
 */
function pageRank(matrix: number[][]): number[] {
  const n = matrix.length;
  if (n === 0) return [];

  const rowSums = matrix.map(row => row.reduce((a, b) => a + b, 0));

  let scores = new Array(n).fill(1 / n);

  for (let iter = 0; iter < TEXTRANK_ITERATIONS; iter++) {
    const newScores = new Array(n).fill(0);

    for (let i = 0; i < n; i++) {
      let sum = 0;
      for (let j = 0; j < n; j++) {
        if (j !== i && rowSums[j] > 0) {
          sum += (matrix[j][i] / rowSums[j]) * scores[j];
        }
      }
      newScores[i] = (1 - TEXTRANK_DAMPING) / n + TEXTRANK_DAMPING * sum;
    }

    let delta = 0;
    for (let i = 0; i < n; i++) {
      delta += Math.abs(newScores[i] - scores[i]);
    }

    scores = newScores;
    if (delta < TEXTRANK_CONVERGENCE) break;
  }

  return scores;
}

// ─── TextRank for Keywords (word co-occurrence graph) ───────────────

const COOCCURRENCE_WINDOW = 5;

/**
 * Build word co-occurrence graph.
 * Nodes = unique normalized words in the document.
 * Edge weight between w_i and w_j = number of times they co-occur
 * within a sliding window of COOCCURRENCE_WINDOW words.
 */
function wordTextRank(
  allWords: Word[],
): { word: string; score: number }[] {
  // Unique vocabulary with stable indices
  const vocabList: string[] = [];
  const vocabIndex = new Map<string, number>();
  for (const w of allWords) {
    if (!vocabIndex.has(w.normalized)) {
      vocabIndex.set(w.normalized, vocabList.length);
      vocabList.push(w.normalized);
    }
  }

  const n = vocabList.length;
  const matrix: number[][] = Array.from({ length: n }, () => new Array(n).fill(0));

  // Slide window over word sequence
  for (let i = 0; i < allWords.length; i++) {
    const idxI = vocabIndex.get(allWords[i].normalized)!;
    for (let j = i + 1; j < Math.min(i + COOCCURRENCE_WINDOW, allWords.length); j++) {
      const idxJ = vocabIndex.get(allWords[j].normalized)!;
      if (idxI !== idxJ) {
        matrix[idxI][idxJ] += 1;
        matrix[idxJ][idxI] += 1;
      }
    }
  }

  const scores = pageRank(matrix);

  return vocabList
    .map((word, i) => ({ word, score: scores[i] }))
    .sort((a, b) => b.score - a.score);
}

// ─── TextRank for Sentences ─────────────────────────────────────────

interface Sentence {
  index: number;
  text: string;
  start: number;    // offset in normalized text
  words: string[];  // normalized words
}

/**
 * Split normalized document text into sentences on . ? !
 * (Crude but functional for this experiment.)
 */
function splitSentences(normalizedText: string): Sentence[] {
  const sentences: Sentence[] = [];
  // Match sentence-ending punctuation followed by whitespace (or end of string)
  const re = /(?<=[.?!])\s+/g;
  let pos = 0;
  let match: RegExpExecArray | null;
  while ((match = re.exec(normalizedText)) !== null) {
    const text = normalizedText.slice(pos, match.index + 1).trim(); // include the punctuation
    if (text.length > 0) {
      const words = text.toLowerCase().split(/\s+/).filter(w => w.length > 0);
      if (words.length >= 3) {
        sentences.push({ index: sentences.length, text, start: pos, words });
      }
    }
    pos = match.index + match[0].length;
  }
  // Remainder after last split
  if (pos < normalizedText.length) {
    const text = normalizedText.slice(pos).trim();
    if (text.length > 0) {
      const words = text.toLowerCase().split(/\s+/).filter(w => w.length > 0);
      if (words.length >= 3) {
        sentences.push({ index: sentences.length, text, start: pos, words });
      }
    }
  }
  return sentences;
}

/**
 * TextRank over sentences.
 * Nodes = sentences. Edge weight = cosine similarity of TF-IDF subvectors.
 */
function sentenceTextRank(
  sentences: Sentence[],
  weights: Map<string, number>,
): { sentence: Sentence; score: number }[] {
  const n = sentences.length;
  const keySets = sentences.map(s => new Set(s.words));

  const matrix: number[][] = Array.from({ length: n }, () => new Array(n).fill(0));
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const sim = cosineSimilarity(weights, keySets[i], keySets[j]);
      matrix[i][j] = sim;
      matrix[j][i] = sim;
    }
  }

  const scores = pageRank(matrix);

  return sentences
    .map((sentence, i) => ({ sentence, score: scores[i] }))
    .sort((a, b) => b.score - a.score);
}

// ─── Chain Description ──────────────────────────────────────────────

interface ChainChunk extends Chunk {
  id: string;
}

interface ChainDescription {
  title: string;
  chunks: ChainChunk[];
  relations: Array<{ from: string; to: string; relationType: string }>;
}

function buildChain(title: string, chunks: Chunk[]): ChainDescription {
  const cc = chunks as ChainChunk[];
  const relations: Array<{ from: string; to: string; relationType: string }> = [];

  if (cc.length === 0) return { title, chunks: cc, relations };

  // Title <-> first chunk
  relations.push({ from: title, to: cc[0].id, relationType: 'starts_with' });
  relations.push({ from: cc[0].id, to: title, relationType: 'belongs_to' });

  // Title <-> last chunk (if different)
  if (cc.length > 1) {
    relations.push({ from: title, to: cc[cc.length - 1].id, relationType: 'ends_with' });
    relations.push({ from: cc[cc.length - 1].id, to: title, relationType: 'belongs_to' });
  }

  // Chain: chunk_i <-> chunk_{i+1}
  for (let i = 0; i < cc.length - 1; i++) {
    relations.push({ from: cc[i].id, to: cc[i + 1].id, relationType: 'follows' });
    relations.push({ from: cc[i + 1].id, to: cc[i].id, relationType: 'preceded_by' });
  }

  return { title, chunks: cc, relations };
}

// ─── Index Entity (sentence TextRank → chunk references) ────────────

interface IndexDescription {
  indexId: string;
  /** The index entity's observations (summary text, max 2 × 140 chars) */
  observations: string[];
  /** Relations from index → chunks containing top sentences */
  relations: Array<{ from: string; to: string; relationType: string }>;
  /** Which chunk IDs are referenced, with the sentence that caused it */
  references: Array<{ chunkId: string; chunkIndex: number; sentence: string; score: number }>;
}

/**
 * Find which chunk contains the start of a sentence, using offsets.
 * Chunks tile the normalized text via their observation start/end spans.
 * We find the chunk whose span contains sentence.start.
 */
function sentenceToChunk(
  sentence: Sentence,
  chunks: ChainChunk[],
): ChainChunk | null {
  const target = sentence.start;
  for (const chunk of chunks) {
    const first = chunk.observations[0];
    const last = chunk.observations[chunk.observations.length - 1];
    if (target >= first.start && target < last.end) {
      return chunk;
    }
  }
  return null;
}

/**
 * Build the index entity for a document.
 *
 * - Takes the top-K ranked sentences
 * - Maps each to its containing chunk
 * - Deduplicates chunk references
 * - Packs top sentence previews into the index entity's 2 observations
 * - Creates relations: title → has_index → index, index → highlights → chunk
 */
function buildIndex(
  title: string,
  chunks: ChainChunk[],
  rankedSentences: { sentence: Sentence; score: number }[],
  topK: number,
): IndexDescription {
  const indexId = `${title}__index`;

  const topSents = rankedSentences.slice(0, topK);

  // Map sentences to chunks, deduplicate
  const references: IndexDescription['references'] = [];
  const seenChunks = new Set<string>();

  for (const { sentence, score } of topSents) {
    const chunk = sentenceToChunk(sentence, chunks);
    if (!chunk) continue;
    if (seenChunks.has(chunk.id)) continue;
    seenChunks.add(chunk.id);
    references.push({
      chunkId: chunk.id,
      chunkIndex: chunk.index,
      sentence: sentence.text,
      score,
    });
  }

  // Pack top sentence previews into observations (max 2 × 140 chars)
  // Truncate sentences to fit, separate with " | "
  const observations: string[] = [];
  let current = '';
  for (const ref of references) {
    const preview = ref.sentence.length > 60
      ? ref.sentence.slice(0, 57) + '...'
      : ref.sentence;
    const candidate = current ? current + ' | ' + preview : preview;
    if (candidate.length <= MAX_OBS_LENGTH) {
      current = candidate;
    } else {
      if (current) observations.push(current);
      if (observations.length >= MAX_OBS_PER_ENTITY) break;
      current = preview.length <= MAX_OBS_LENGTH ? preview : preview.slice(0, MAX_OBS_LENGTH);
    }
  }
  if (current && observations.length < MAX_OBS_PER_ENTITY) {
    observations.push(current);
  }

  // Relations
  const relations: Array<{ from: string; to: string; relationType: string }> = [];

  // title → index
  relations.push({ from: title, to: indexId, relationType: 'has_index' });
  relations.push({ from: indexId, to: title, relationType: 'indexes' });

  // index → highlighted chunks
  for (const ref of references) {
    relations.push({ from: indexId, to: ref.chunkId, relationType: 'highlights' });
    relations.push({ from: ref.chunkId, to: indexId, relationType: 'highlighted_by' });
  }

  return { indexId, observations, relations, references };
}

// ─── Main ───────────────────────────────────────────────────────────

function main() {
  const args = process.argv.slice(2);
  let _verbose = false;
  const filtered: string[] = [];
  for (const arg of args) {
    if (arg === '-v' || arg === '--verbose') _verbose = true;
    else filtered.push(arg);
  }

  // Read document
  let text: string;
  let title = 'untitled';
  if (filtered[0] === '--text' && filtered[1]) {
    text = filtered[1];
    title = filtered[2] ?? 'untitled';
  } else if (filtered[0] && filtered[0] !== '-') {
    text = fs.readFileSync(filtered[0], 'utf-8');
    title = path.basename(filtered[0], path.extname(filtered[0]));
  } else if (!process.stdin.isTTY) {
    text = fs.readFileSync(0, 'utf-8');
  } else {
    console.error('Usage: npx tsx scripts/textrank-experiment.ts <file>');
    process.exit(1);
  }

  // Process document
  const { chunks, allWords } = processDocument(text);

  // Build chain
  const chain = buildChain(title, chunks);

  // Open KB string table for IDF (required)
  const memPath = process.env.MEMORY_FILE_PATH;
  if (!memPath) {
    console.error('MEMORY_FILE_PATH must be set.');
    process.exit(1);
  }
  const dir = path.dirname(memPath);
  const base = path.basename(memPath, path.extname(memPath));
  const strPath = path.join(dir, `${base}.strings`);
  if (!fs.existsSync(strPath)) {
    console.error(`String table not found at ${strPath}`);
    process.exit(1);
  }

  const st = new StringTable(strPath);
  console.error(`String table loaded: ${strPath} (${st.count} entries)`);
  const { df, corpusSize } = deriveCorpusDocFreqs(st);

  // Vocab = all unique words in this document
  const vocab = new Set(allWords.map(w => w.normalized));
  const idf = buildIdfVector(vocab, df, corpusSize);

  // Build the single document weight vector
  const weights = buildWeightVector(allWords, idf);

  // ─── Output ─────────────────────────────────────────────────────

  const tfMode = USE_LOG_TF ? 'log(1+count)' : 'raw count';
  console.log(`Document: "${title}"  [TF mode: ${tfMode}]`);
  console.log(`  ${text.length} chars, ${allWords.length} words, ${vocab.size} unique`);
  console.log(`  ${chain.chunks.length} chunks, ${chain.relations.length} relations`);
  console.log(`  Corpus: N=${corpusSize} entity-string refs, ${df.size} unique words`);
  console.log();

  // ─── Word TextRank (co-occurrence graph) ────────────────────────

  console.error('Running word TextRank...');
  const rankedWords = wordTextRank(allWords);

  console.log('=== Word TextRank (top 40 keywords) ===');
  for (const { word, score } of rankedWords.slice(0, 40)) {
    const idfW = idf.get(word) ?? 0;
    const tfidf = weights.get(word) ?? 0;
    console.log(`  ${score.toFixed(6)}  ${word.padEnd(25)} TF-IDF: ${tfidf.toFixed(2).padStart(7)}  IDF: ${idfW.toFixed(2).padStart(7)}`);
  }
  console.log();

  // ─── Sentence TextRank ──────────────────────────────────────────

  const normalized = normalize(text);
  const sentences = splitSentences(normalized);
  console.error(`Running sentence TextRank (${sentences.length} sentences)...`);
  const rankedSentences = sentenceTextRank(sentences, weights);

  // ─── Build Index Entity ─────────────────────────────────────────

  const index = buildIndex(title, chain.chunks, rankedSentences, 15);

  // ─── Output: Full Graph Description ─────────────────────────────

  // Collect all entities
  const entities: Array<{ name: string; type: string; observations: string[] }> = [];

  // Document entity
  entities.push({ name: title, type: 'Document', observations: [] });

  // Chain chunks
  for (const chunk of chain.chunks) {
    entities.push({
      name: chunk.id,
      type: 'TextChunk',
      observations: chunk.observations.map(o => o.text),
    });
  }

  // Index entity
  entities.push({
    name: index.indexId,
    type: 'DocumentIndex',
    observations: index.observations,
  });

  // Collect all relations
  const allRelations = [...chain.relations, ...index.relations];

  console.log(`=== Graph Structure ===`);
  console.log(`  Entities: ${entities.length} (1 Document + ${chain.chunks.length} TextChunks + 1 DocumentIndex)`);
  console.log(`  Relations: ${allRelations.length} (${chain.relations.length} chain + ${index.relations.length} index)`);
  console.log();

  // Index details
  console.log(`=== Index: "${index.indexId}" ===`);
  console.log(`  Observations:`);
  for (const obs of index.observations) {
    console.log(`    "${obs}"`);
  }
  console.log(`  Highlights ${index.references.length} chunks:`);
  for (const ref of index.references) {
    const preview = ref.sentence.length > 80 ? ref.sentence.slice(0, 77) + '...' : ref.sentence;
    console.log(`    chunk[${String(ref.chunkIndex).padStart(3)}] (score: ${ref.score.toFixed(4)}) ${preview}`);
  }
  console.log();

  // Top sentences for reference
  console.log(`=== Sentence TextRank (top 15) ===`);
  for (const { sentence, score } of rankedSentences.slice(0, 15)) {
    const preview = sentence.text.length > 100 ? sentence.text.slice(0, 97) + '...' : sentence.text;
    console.log(`  (${score.toFixed(6)})  ${preview}`);
  }
  console.log();

  // Cleanup
  st.close();
}

main();
