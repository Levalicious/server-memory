/**
 * Trigram inverted index for regex pre-filtering.
 *
 * Maps each character trigram (3-byte lowercase substring) to the set of
 * string-IDs whose payload contains that trigram. Queries are expressed as
 * a boolean expression over trigrams ({@link TrigramQuery}) — produced by
 * the regex-AST analysis in `src/regex_query.ts` (Cox's algorithm) — and
 * evaluated against the postings via {@link TrigramIndex.evaluateQuery}.
 *
 * Storage: process memory only. The class supports incremental add/remove,
 * so `intern` / `release` / `addObservation` etc. can keep it in sync within
 * a single process. Multi-process invalidation is the caller's problem;
 * `rebuild` reconstructs from a `(stringId, text)[]` iterator.
 *
 * Build cost: roughly O(total chars indexed). On the active 42K-entity KB
 * that's ~7s (the bulk-sort path here, vs. ~minutes with a naive
 * `Map<trigram, Set<id>>` builder).
 */

// =============================================================================
// Trigram packing — byte-level (UTF-8)
// =============================================================================

/**
 * Reusable encoder. `TextEncoder.encode` produces a fresh `Uint8Array`, so
 * we don't share buffers — only the encoder instance.
 */
const UTF8 = new TextEncoder();

/**
 * Encode a string to its UTF-8 byte sequence. Multi-byte chars (non-ASCII)
 * become 2-4 bytes each, so each non-ASCII char contributes its own internal
 * trigrams to the index — much better selectivity than the prior 24-bit
 * UTF-16-byte-truncated packing (which collapsed all chars > 0xff into the
 * 256-byte alphabet via bitwise mask).
 */
export function encodeUtf8(s: string): Uint8Array {
  return UTF8.encode(s);
}

/**
 * Pack 3 consecutive bytes into a 24-bit integer. Bytes are taken verbatim
 * — no case folding here; that's handled upstream via `String#toLowerCase`
 * before encoding (Unicode-aware). Same packing used on both index and
 * regex-query sides.
 */
export function packTrigramAt(bytes: Uint8Array, i: number): number {
  return bytes[i] | (bytes[i + 1] << 8) | (bytes[i + 2] << 16);
}

/**
 * Collect the distinct trigrams of `bytes` into `out`. We dedupe per-string
 * because the index granularity is "string-ID contains this trigram" — a
 * string having `foo` ten times doesn't change the candidate set.
 */
function collectTrigrams(bytes: Uint8Array, out: Set<number>): void {
  if (bytes.length < 3) return;
  for (let i = 0; i + 3 <= bytes.length; i++) {
    out.add(packTrigramAt(bytes, i));
  }
}

// =============================================================================
// TrigramQuery — boolean expression over trigrams
// =============================================================================

/**
 * A boolean expression over trigrams, produced by Cox's algorithm from a
 * regex AST and evaluated against a {@link TrigramIndex}.
 *
 * Variants:
 *   - `all`: anything matches; the index can't filter (caller must scan).
 *   - `none`: nothing matches (zero candidates — sound short-circuit).
 *   - `tri`: a single required trigram (a 24-bit packed int, see
 *     {@link packTrigramAt}).
 *   - `and`: every child must hold (intersection of postings).
 *   - `or`:  some child must hold (union of postings).
 *
 * Construction is normalized via {@link tqAnd} / {@link tqOr} which flatten
 * nested same-tag nodes and short-circuit `all`/`none` operands; consumers
 * never need to handle pathological tree shapes.
 */
export type TrigramQuery =
  | { tag: 'all' }
  | { tag: 'none' }
  | { tag: 'tri'; t: number }
  | { tag: 'and'; ks: TrigramQuery[] }
  | { tag: 'or';  ks: TrigramQuery[] };

export function allMatch(): TrigramQuery { return { tag: 'all' }; }
export function noMatch():  TrigramQuery { return { tag: 'none' }; }
export function tqTri(t: number): TrigramQuery { return { tag: 'tri', t }; }

/**
 * Build an AND query, simplifying:
 *   - drop `all` operands (no constraint)
 *   - any `none` operand → result is `none`
 *   - flatten nested `and`
 *   - dedupe `tri` leaves
 *   - 0 surviving operands → `all`; 1 → that operand
 */
export function tqAnd(qs: TrigramQuery[]): TrigramQuery {
  const flat: TrigramQuery[] = [];
  for (const q of qs) {
    if (q.tag === 'all') continue;
    if (q.tag === 'none') return noMatch();
    if (q.tag === 'and') flat.push(...q.ks);
    else flat.push(q);
  }
  if (flat.length === 0) return allMatch();
  // Dedupe trigram leaves (the same trigram appearing twice doesn't tighten).
  const seenTris = new Set<number>();
  const out: TrigramQuery[] = [];
  for (const q of flat) {
    if (q.tag === 'tri') {
      if (seenTris.has(q.t)) continue;
      seenTris.add(q.t);
    }
    out.push(q);
  }
  if (out.length === 1) return out[0];
  return { tag: 'and', ks: out };
}

/**
 * Build an OR query, simplifying:
 *   - any `all` operand → result is `all`
 *   - drop `none` operands
 *   - flatten nested `or`
 *   - dedupe `tri` leaves
 *   - 0 surviving operands → `none`; 1 → that operand
 */
export function tqOr(qs: TrigramQuery[]): TrigramQuery {
  const flat: TrigramQuery[] = [];
  for (const q of qs) {
    if (q.tag === 'all') return allMatch();
    if (q.tag === 'none') continue;
    if (q.tag === 'or') flat.push(...q.ks);
    else flat.push(q);
  }
  if (flat.length === 0) return noMatch();
  const seenTris = new Set<number>();
  const out: TrigramQuery[] = [];
  for (const q of flat) {
    if (q.tag === 'tri') {
      if (seenTris.has(q.t)) continue;
      seenTris.add(q.t);
    }
    out.push(q);
  }
  if (out.length === 1) return out[0];
  return { tag: 'or', ks: out };
}

// =============================================================================
// Posting-list helpers (internal)
// =============================================================================

/** Sorted-array intersection of two ascending lists. Returns a fresh array. */
function intersectSorted(a: bigint[], b: bigint[]): bigint[] {
  const out: bigint[] = [];
  let i = 0, j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) { out.push(a[i]); i++; j++; }
    else if (a[i] < b[j]) i++;
    else j++;
  }
  return out;
}

/** Sorted-array union of k ascending lists. Returns a fresh deduped array. */
function unionSorted(arrays: bigint[][]): bigint[] {
  if (arrays.length === 0) return [];
  if (arrays.length === 1) return arrays[0].slice();
  const seen = new Set<bigint>();
  const out: bigint[] = [];
  for (const arr of arrays) {
    for (const v of arr) {
      if (!seen.has(v)) { seen.add(v); out.push(v); }
    }
  }
  out.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return out;
}

// =============================================================================
// TrigramIndex
// =============================================================================

export class TrigramIndex {
  /** trigram (24-bit packed int) → sorted ascending list of string-IDs. */
  private postings: Map<number, bigint[]>;

  /**
   * Per string-ID, the set of trigrams it contributed. Needed so `remove`
   * can reverse the contribution without rescanning the string.
   */
  private trigramsByString: Map<bigint, Set<number>>;

  /** Number of distinct strings currently indexed. */
  private _size: number;

  constructor() {
    this.postings = new Map();
    this.trigramsByString = new Map();
    this._size = 0;
  }

  get size(): number { return this._size; }
  get distinctTrigrams(): number { return this.postings.size; }

  /**
   * Insert a string into the index. Idempotent for the same `(id, text)`
   * pair. If `id` was previously indexed under a different `text`, the
   * caller must `remove(id)` first — the class doesn't store the original
   * text to detect this.
   */
  add(id: bigint, text: string): void {
    if (this.trigramsByString.has(id)) return;
    const tris = new Set<number>();
    // Lowercase first (Unicode-aware), then UTF-8 encode. Same path the
    // regex side uses, so query trigrams agree with index trigrams.
    collectTrigrams(encodeUtf8(text.toLowerCase()), tris);
    if (tris.size === 0) {
      this.trigramsByString.set(id, tris);
      this._size++;
      return;
    }
    for (const t of tris) {
      let list = this.postings.get(t);
      if (!list) { list = []; this.postings.set(t, list); }
      // Tail-append fast path (entity offsets monotonically increase as new
      // entities are interned). Otherwise, binary-search insertion.
      if (list.length === 0 || list[list.length - 1] < id) {
        list.push(id);
      } else {
        let lo = 0, hi = list.length;
        while (lo < hi) {
          const mid = (lo + hi) >>> 1;
          if (list[mid] < id) lo = mid + 1; else hi = mid;
        }
        list.splice(lo, 0, id);
      }
    }
    this.trigramsByString.set(id, tris);
    this._size++;
  }

  /** Remove a string from the index. No-op if the id wasn't indexed. */
  remove(id: bigint): void {
    const tris = this.trigramsByString.get(id);
    if (!tris) return;
    for (const t of tris) {
      const list = this.postings.get(t);
      if (!list) continue;
      let lo = 0, hi = list.length;
      while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (list[mid] < id) lo = mid + 1; else hi = mid;
      }
      if (lo < list.length && list[lo] === id) {
        list.splice(lo, 1);
        if (list.length === 0) this.postings.delete(t);
      }
    }
    this.trigramsByString.delete(id);
    this._size--;
  }

  /** Wipe everything. */
  clear(): void {
    this.postings.clear();
    this.trigramsByString.clear();
    this._size = 0;
  }

  /**
   * Bulk rebuild from an iterator of (id, text). Faster than calling `add()`
   * N times because we sort once at the end instead of inserting into N
   * already-sorted lists.
   */
  rebuild(items: Iterable<readonly [bigint, string]>): void {
    this.clear();
    const pairs: { t: number; id: bigint }[] = [];
    for (const [id, text] of items) {
      const tris = new Set<number>();
      collectTrigrams(encodeUtf8(text.toLowerCase()), tris);
      this.trigramsByString.set(id, tris);
      this._size++;
      for (const t of tris) pairs.push({ t, id });
    }
    pairs.sort((a, b) => {
      if (a.t !== b.t) return a.t - b.t;
      return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
    });
    let curTri = -1;
    let curList: bigint[] | undefined;
    for (const p of pairs) {
      if (p.t !== curTri) {
        curTri = p.t;
        curList = [];
        this.postings.set(p.t, curList);
      }
      curList!.push(p.id);
    }
  }

  /**
   * Evaluate a boolean trigram query against the index, returning a sorted
   * ascending list of candidate string-IDs.
   *
   * Return values:
   *   - `null`  — the query is `all` (no constraint); caller must full-scan.
   *   - `[]`    — the query is `none` or short-circuited to empty (a required
   *               trigram is missing from the index). Sound — no real string
   *               can match. Caller can skip the post-filter entirely.
   *   - `bigint[]` — sorted ascending list of candidate string-IDs.
   */
  evaluateQuery(q: TrigramQuery): bigint[] | null {
    switch (q.tag) {
      case 'all':  return null;
      case 'none': return [];
      case 'tri': {
        const list = this.postings.get(q.t);
        return list ? list.slice() : [];
      }
      case 'and': {
        const lists: bigint[][] = [];
        for (const child of q.ks) {
          const r = this.evaluateQuery(child);
          if (r === null) continue;          // 'all' contributes no constraint
          if (r.length === 0) return [];     // any empty branch ⇒ empty AND
          lists.push(r);
        }
        if (lists.length === 0) return null; // every child was 'all'
        lists.sort((a, b) => a.length - b.length);
        let acc = lists[0];
        for (let k = 1; k < lists.length && acc.length > 0; k++) {
          acc = intersectSorted(acc, lists[k]);
        }
        return acc;
      }
      case 'or': {
        const lists: bigint[][] = [];
        for (const child of q.ks) {
          const r = this.evaluateQuery(child);
          if (r === null) return null;       // OR with unfilterable ⇒ unfilterable
          lists.push(r);
        }
        return unionSorted(lists);
      }
    }
  }
}
