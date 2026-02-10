/**
 * kb_load.ts — Load a plaintext document into the knowledge graph.
 *
 * Pipeline:
 *   1. Normalize text
 *   2. Split into observations (≤140 chars, word-boundary aligned)
 *   3. Group observations into chunks (≤2 per entity)
 *   4. Build chain: Document → starts_with/ends_with → chunks ↔ follows/preceded_by
 *   5. Sentence TextRank: rank sentences by TF-IDF cosine PageRank
 *   6. Build index entity: Document → has_index → Index → highlights → top chunks
 *
 * Returns arrays of entities and relations ready for createEntities/createRelations.
 */

import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { StringTable } from './stringtable.js';

// ─── Constants ──────────────────────────────────────────────────────

const MAX_OBS_LENGTH = 140;
const MAX_OBS_PER_ENTITY = 2;

const TEXTRANK_DAMPING = 0.85;
const TEXTRANK_MAX_ITER = 30000;
const TEXTRANK_CONVERGENCE = 1e-6;

const ALLOWED_EXTENSIONS = new Set([
  '.txt', '.tex', '.md', '.markdown', '.rst', '.org', '.adoc',
  '.asciidoc', '.html', '.htm', '.xml', '.json', '.yaml', '.yml',
  '.toml', '.csv', '.tsv', '.log', '.cfg', '.ini', '.conf',
  '.py', '.js', '.ts', '.c', '.h', '.cpp', '.hpp', '.java',
  '.go', '.rs', '.rb', '.pl', '.sh', '.bash', '.zsh', '.fish',
  '.el', '.lisp', '.clj', '.hs', '.ml', '.scala', '.kt',
  '.r', '.m', '.swift', '.lua', '.vim', '.sql',
  '.bib', '.sty', '.cls',
]);

// ─── Data Structures ────────────────────────────────────────────────

interface Word {
  text: string;
  normalized: string;
  start: number;
  end: number;
}

interface Observation {
  text: string;
  start: number;
  end: number;
  words: Word[];
}

interface Chunk {
  index: number;
  id: string;
  observations: Observation[];
}

interface Sentence {
  index: number;
  text: string;
  start: number;
  words: string[];
}

/** What we return to the server for insertion. */
export interface KbLoadResult {
  entities: Array<{ name: string; entityType: string; observations: string[] }>;
  relations: Array<{ from: string; to: string; relationType: string }>;
  stats: {
    chars: number;
    words: number;
    uniqueWords: number;
    chunks: number;
    sentences: number;
    indexHighlights: number;
  };
}

// ─── Text Processing ────────────────────────────────────────────────

function normalize(text: string): string {
  text = text.replace(/\r\n/g, '\n');
  text = text.replace(/[ \t]+/g, ' ');
  text = text.replace(/\n{3,}/g, '\n\n');
  text = text.trim();
  return text.split(/\s+/).join(' ');
}

function labelWords(text: string, offset: number): Word[] {
  const words: Word[] = [];
  let i = 0;
  const n = text.length;
  while (i < n) {
    while (i < n && text[i] === ' ') i++;
    if (i >= n) break;
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

function splitIntoObservations(text: string): Observation[] {
  const observations: Observation[] = [];
  let pos = 0;

  while (pos < text.length) {
    const remaining = text.slice(pos);

    if (remaining.length <= MAX_OBS_LENGTH) {
      observations.push({
        text: remaining,
        start: pos,
        end: pos + remaining.length,
        words: labelWords(remaining, pos),
      });
      break;
    }

    let splitAt = 0;
    for (let i = 0; i < remaining.length; i++) {
      if (remaining[i] === ' ') {
        if (remaining.slice(0, i).length <= MAX_OBS_LENGTH) {
          splitAt = i;
        } else {
          break;
        }
      }
    }

    if (splitAt === 0) {
      // No space fits — hard split
      let jsLen = 0;
      for (let i = 0; i < remaining.length; i++) {
        const charLen = remaining.codePointAt(i)! > 0xFFFF ? 2 : 1;
        if (jsLen + charLen > MAX_OBS_LENGTH) { splitAt = i; break; }
        jsLen += charLen;
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
    while (pos < text.length && text[pos] === ' ') pos++;
  }

  return observations;
}

function chunkObservations(observations: Observation[]): Chunk[] {
  const chunks: Chunk[] = [];
  for (let i = 0; i < observations.length; i += MAX_OBS_PER_ENTITY) {
    chunks.push({
      index: chunks.length,
      id: crypto.randomBytes(12).toString('hex'),
      observations: observations.slice(i, i + MAX_OBS_PER_ENTITY),
    });
  }
  return chunks;
}

function chunkText(chunk: Chunk): string {
  return chunk.observations.map(o => o.text).join(' ');
}

function chunkWordKeys(chunk: Chunk): Set<string> {
  const keys = new Set<string>();
  for (const obs of chunk.observations) {
    for (const w of obs.words) keys.add(w.normalized);
  }
  return keys;
}

// ─── Sentence Splitting ─────────────────────────────────────────────

function splitSentences(normalizedText: string): Sentence[] {
  const sentences: Sentence[] = [];
  const re = /(?<=[.?!])\s+/g;
  let pos = 0;
  let match: RegExpExecArray | null;
  while ((match = re.exec(normalizedText)) !== null) {
    const text = normalizedText.slice(pos, match.index + 1).trim();
    if (text.length > 0) {
      const words = text.toLowerCase().split(/\s+/).filter(w => w.length > 0);
      if (words.length >= 3) {
        sentences.push({ index: sentences.length, text, start: pos, words });
      }
    }
    pos = match.index + match[0].length;
  }
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

// ─── TF-IDF ─────────────────────────────────────────────────────────

function buildWeightVector(
  allWords: Word[],
  idf: Map<string, number>,
): Map<string, number> {
  const rawCounts = new Map<string, number>();
  for (const w of allWords) {
    rawCounts.set(w.normalized, (rawCounts.get(w.normalized) ?? 0) + 1);
  }
  const weights = new Map<string, number>();
  for (const [word, raw] of rawCounts) {
    weights.set(word, raw * (idf.get(word) ?? 0));
  }
  return weights;
}

function deriveCorpusDocFreqs(st: StringTable): { df: Map<string, number>; corpusSize: number } {
  const df = new Map<string, number>();
  let corpusSize = 0;
  for (const entry of st.entries()) {
    corpusSize += entry.refcount;
    const uniqueWords = new Set(
      entry.text.toLowerCase().split(/\s+/).filter(w => w.length > 0)
    );
    for (const word of uniqueWords) {
      df.set(word, (df.get(word) ?? 0) + entry.refcount);
    }
  }
  return { df, corpusSize };
}

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

// ─── Cosine Similarity ──────────────────────────────────────────────

function cosineSimilarity(
  weights: Map<string, number>,
  keysA: Set<string>,
  keysB: Set<string>,
): number {
  let dot = 0;
  for (const word of keysA) {
    if (keysB.has(word)) {
      const w = weights.get(word) ?? 0;
      dot += w * w;
    }
  }
  let normA = 0;
  for (const word of keysA) { const w = weights.get(word) ?? 0; normA += w * w; }
  let normB = 0;
  for (const word of keysB) { const w = weights.get(word) ?? 0; normB += w * w; }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  return denom === 0 ? 0 : dot / denom;
}

// ─── PageRank ───────────────────────────────────────────────────────

function pageRank(matrix: number[][]): number[] {
  const n = matrix.length;
  if (n === 0) return [];
  const rowSums = matrix.map(row => row.reduce((a, b) => a + b, 0));
  let scores = new Array(n).fill(1 / n);

  for (let iter = 0; iter < TEXTRANK_MAX_ITER; iter++) {
    const next = new Array(n).fill(0);
    for (let i = 0; i < n; i++) {
      let sum = 0;
      for (let j = 0; j < n; j++) {
        if (j !== i && rowSums[j] > 0) {
          sum += (matrix[j][i] / rowSums[j]) * scores[j];
        }
      }
      next[i] = (1 - TEXTRANK_DAMPING) / n + TEXTRANK_DAMPING * sum;
    }
    let delta = 0;
    for (let i = 0; i < n; i++) delta += Math.abs(next[i] - scores[i]);
    scores = next;
    if (delta < TEXTRANK_CONVERGENCE) break;
  }
  return scores;
}

// ─── Sentence TextRank ──────────────────────────────────────────────

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

// ─── Sentence → Chunk mapping ───────────────────────────────────────

function sentenceToChunk(sentence: Sentence, chunks: Chunk[]): Chunk | null {
  const target = sentence.start;
  for (const chunk of chunks) {
    const first = chunk.observations[0];
    const last = chunk.observations[chunk.observations.length - 1];
    if (target >= first.start && target < last.end) return chunk;
  }
  return null;
}

// ─── Public API ─────────────────────────────────────────────────────

/**
 * Validate that a file path has a plaintext extension.
 * Returns the extension if valid, throws if not.
 */
export function validateExtension(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  if (!ext) {
    throw new Error(`File has no extension: ${filePath}. Only plaintext files are accepted.`);
  }
  if (!ALLOWED_EXTENSIONS.has(ext)) {
    throw new Error(
      `Unsupported file extension "${ext}". Only plaintext formats are accepted ` +
      `(${[...ALLOWED_EXTENSIONS].slice(0, 10).join(', ')}, ...). ` +
      `For PDFs, use pdftotext first. For other binary formats, convert to text.`
    );
  }
  return ext;
}

/**
 * Load a plaintext document into the knowledge graph.
 *
 * @param text       Raw document text
 * @param title      Document entity name (e.g. filename without extension)
 * @param st         StringTable for IDF corpus frequencies
 * @param topK       Number of sentences to highlight in the index (default: 15)
 * @returns Entities and relations ready for createEntities/createRelations
 */
export function loadDocument(
  text: string,
  title: string,
  st: StringTable,
  topK = 15,
): KbLoadResult {
  // 1. Normalize and chunk
  const normalizedText = normalize(text);
  const observations = splitIntoObservations(normalizedText);
  const chunks = chunkObservations(observations);

  // Collect all words
  const allWords: Word[] = [];
  for (const chunk of chunks) {
    for (const obs of chunk.observations) allWords.push(...obs.words);
  }
  const vocab = new Set(allWords.map(w => w.normalized));

  // 2. IDF from corpus
  const { df, corpusSize } = deriveCorpusDocFreqs(st);
  const idf = buildIdfVector(vocab, df, corpusSize);

  // 3. TF-IDF weight vector
  const weights = buildWeightVector(allWords, idf);

  // 4. Sentence TextRank
  const sentences = splitSentences(normalizedText);
  const rankedSentences = sentenceTextRank(sentences, weights);

  // 5. Map top sentences to chunks (deduplicate)
  const topSents = rankedSentences.slice(0, topK);
  const highlights: Array<{ chunk: Chunk; sentence: Sentence; score: number }> = [];
  const seenChunks = new Set<string>();
  for (const { sentence, score } of topSents) {
    const chunk = sentenceToChunk(sentence, chunks);
    if (!chunk || seenChunks.has(chunk.id)) continue;
    seenChunks.add(chunk.id);
    highlights.push({ chunk, sentence, score });
  }

  // 6. Build index observations (compressed sentence previews)
  const indexId = `${title}__index`;
  const indexObs: string[] = [];
  let current = '';
  for (const { sentence } of highlights) {
    const preview = sentence.text.length > 60
      ? sentence.text.slice(0, 57) + '...'
      : sentence.text;
    const candidate = current ? current + ' | ' + preview : preview;
    if (candidate.length <= MAX_OBS_LENGTH) {
      current = candidate;
    } else {
      if (current) indexObs.push(current);
      if (indexObs.length >= MAX_OBS_PER_ENTITY) break;
      current = preview.length <= MAX_OBS_LENGTH ? preview : preview.slice(0, MAX_OBS_LENGTH);
    }
  }
  if (current && indexObs.length < MAX_OBS_PER_ENTITY) indexObs.push(current);

  // ─── Assemble entities ──────────────────────────────────────────

  const entities: KbLoadResult['entities'] = [];
  const relations: KbLoadResult['relations'] = [];

  // Document entity (no observations — it's a pointer node)
  entities.push({ name: title, entityType: 'Document', observations: [] });

  // Chunk entities
  for (const chunk of chunks) {
    entities.push({
      name: chunk.id,
      entityType: 'TextChunk',
      observations: chunk.observations.map(o => o.text),
    });
  }

  // Index entity
  entities.push({
    name: indexId,
    entityType: 'DocumentIndex',
    observations: indexObs,
  });

  // ─── Assemble relations ─────────────────────────────────────────

  // Document → chain endpoints
  if (chunks.length > 0) {
    relations.push({ from: title, to: chunks[0].id, relationType: 'starts_with' });
    relations.push({ from: chunks[0].id, to: title, relationType: 'belongs_to' });
    if (chunks.length > 1) {
      relations.push({ from: title, to: chunks[chunks.length - 1].id, relationType: 'ends_with' });
      relations.push({ from: chunks[chunks.length - 1].id, to: title, relationType: 'belongs_to' });
    }
  }

  // Chain: follows/preceded_by
  for (let i = 0; i < chunks.length - 1; i++) {
    relations.push({ from: chunks[i].id, to: chunks[i + 1].id, relationType: 'follows' });
    relations.push({ from: chunks[i + 1].id, to: chunks[i].id, relationType: 'preceded_by' });
  }

  // Document → index
  relations.push({ from: title, to: indexId, relationType: 'has_index' });
  relations.push({ from: indexId, to: title, relationType: 'indexes' });

  // Index → highlighted chunks
  for (const { chunk } of highlights) {
    relations.push({ from: indexId, to: chunk.id, relationType: 'highlights' });
    relations.push({ from: chunk.id, to: indexId, relationType: 'highlighted_by' });
  }

  return {
    entities,
    relations,
    stats: {
      chars: text.length,
      words: allWords.length,
      uniqueWords: vocab.size,
      chunks: chunks.length,
      sentences: sentences.length,
      indexHighlights: highlights.length,
    },
  };
}
