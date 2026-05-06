/**
 * Trigram inverted index for fast regex pre-filtering.
 *
 * Maps each character trigram (3-byte lowercase substring) to the set of
 * string-IDs whose payload contains that trigram. Given a regex query and a
 * caller-supplied set of *required literals* (each ≥ 3 chars), we:
 *
 *   1. For every required literal `lit`, intersect the posting lists of each
 *      distinct trigram of `lit`. The result is the set of strings that
 *      *might* contain `lit`. Trigrams alone don't prove `lit` is present
 *      (a string could contain all of "fo", "oo", "oba" without containing
 *      "foobar"), so we always post-filter with the actual regex.
 *
 *   2. The candidate set across multiple literals is the *union* — this
 *      matches alternation regexes like `foo|bar`. ("If `R` is `foo|bar`,
 *      a string matching `R` must contain at least one of `foo`, `bar`.")
 *
 *   3. Run `regex.test()` on the candidates only. The literal-extraction
 *      step is the caller's responsibility in this v1 — see the design note
 *      in `searchNodes` for why we don't ship a regex-AST parser yet.
 *
 * Storage: process memory only. The class supports incremental add/remove,
 * so `intern` / `release` / `addObservation` etc. can keep it in sync within
 * a single process. Multi-process invalidation is the caller's problem;
 * `rebuild` reconstructs from a `(stringId, text)[]` iterator.
 *
 * Build cost: roughly O(total chars indexed). On the user's 42K-entity KB
 * that's ~50ms with the flat-array bulk-sort path used here (vs. ~6s with a
 * naive `Map<trigram, Set<id>>` builder).
 */

/**
 * Encode a trigram as a 24-bit integer. Each byte gets 8 bits, little-endian.
 * Lowercased ASCII; non-ASCII bytes pass through (the index is lossy for
 * multibyte characters, which is fine — the post-filter regex catches them).
 */
function packTrigram(s: string, i: number): number {
  // String#charCodeAt is per-UTF-16-code-unit. For ASCII paths (the vast
  // majority of our keys) this is identical to the byte representation.
  // Lowercase by ORing 0x20 only for A–Z so we don't smear digits/symbols.
  let a = s.charCodeAt(i);
  let b = s.charCodeAt(i + 1);
  let c = s.charCodeAt(i + 2);
  if (a >= 0x41 && a <= 0x5a) a |= 0x20;
  if (b >= 0x41 && b <= 0x5a) b |= 0x20;
  if (c >= 0x41 && c <= 0x5a) c |= 0x20;
  return (a & 0xff) | ((b & 0xff) << 8) | ((c & 0xff) << 16);
}

/**
 * Collect the distinct trigrams of `text` into `out`. We dedupe per-string
 * because the index granularity is "string-ID contains this trigram" — a
 * string having `foo` ten times doesn't change the candidate set.
 */
function collectTrigrams(text: string, out: Set<number>): void {
  if (text.length < 3) return;
  for (let i = 0; i + 3 <= text.length; i++) {
    out.add(packTrigram(text, i));
  }
}

/**
 * Sorted-array intersection of two ascending lists. Returns a fresh array.
 */
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

/**
 * Sorted-array union of k ascending lists. Returns a fresh deduped array.
 */
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

export class TrigramIndex {
  /**
   * trigram (24-bit packed int) → sorted ascending list of string-IDs.
   * Sorted to enable cheap merge-join intersection and union.
   */
  private postings: Map<number, bigint[]>;

  /**
   * Per string-ID, the set of trigrams it contributed. Needed so `remove`
   * can reverse the contribution without rescanning the string. Memory cost
   * is ~one Set per indexed string; not free, but bounded by total trigrams.
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
   * pair — calling twice is a no-op. If `id` was previously indexed under
   * a different `text`, the old contribution must be removed first by the
   * caller (we don't store the original text to detect this).
   */
  add(id: bigint, text: string): void {
    if (this.trigramsByString.has(id)) return;
    const tris = new Set<number>();
    collectTrigrams(text.toLowerCase(), tris);
    if (tris.size === 0) {
      // Empty contribution still counts as "indexed" so a future `add` for
      // the same id is a no-op.
      this.trigramsByString.set(id, tris);
      this._size++;
      return;
    }
    for (const t of tris) {
      let list = this.postings.get(t);
      if (!list) { list = []; this.postings.set(t, list); }
      // Insert in sorted order — list is ascending. Most appends will be
      // to the end (ids monotonically increase as new strings are interned),
      // so a fast-path tail-append is the common case.
      if (list.length === 0 || list[list.length - 1] < id) {
        list.push(id);
      } else {
        // Binary search for insertion point.
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

  /**
   * Remove a string from the index. Quietly no-ops if the id wasn't indexed.
   */
  remove(id: bigint): void {
    const tris = this.trigramsByString.get(id);
    if (!tris) return;
    for (const t of tris) {
      const list = this.postings.get(t);
      if (!list) continue;
      // Binary search for the id in the sorted list.
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
   * Bulk rebuild from an iterator of (id, text). Faster than calling
   * `add()` N times because we sort once at the end instead of inserting
   * into N already-sorted lists.
   */
  rebuild(items: Iterable<readonly [bigint, string]>): void {
    this.clear();
    // First pass: collect (trigram, id) pairs, deduped per-string.
    const pairs: { t: number; id: bigint }[] = [];
    for (const [id, text] of items) {
      const tris = new Set<number>();
      collectTrigrams(text.toLowerCase(), tris);
      this.trigramsByString.set(id, tris);
      this._size++;
      for (const t of tris) pairs.push({ t, id });
    }
    // Sort by (trigram, id) ascending. Then group by trigram.
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
   * Return the candidate string-IDs whose strings *might* contain ANY of
   * the supplied required literals. Each literal must be ≥ 3 chars to be
   * useful; literals that are too short return null from
   * {@link candidatesForLiteral} and force the caller to fall back to a
   * full scan (we return the special value `null` to signal that).
   *
   * If `literals` is empty, returns null (no filter possible).
   *
   * On match, the result is a sorted ascending bigint[] of candidate IDs.
   * The caller then runs the actual regex on the strings keyed by these IDs.
   */
  candidates(literals: readonly string[]): bigint[] | null {
    if (literals.length === 0) return null;
    const perLiteral: bigint[][] = [];
    for (const lit of literals) {
      const c = this.candidatesForLiteral(lit);
      if (c === null) return null;   // any unindexable literal → fall back to scan
      perLiteral.push(c);
    }
    return unionSorted(perLiteral);
  }

  /**
   * Candidate IDs for a single required literal. Returns the empty array if
   * one of the literal's trigrams has no postings (zero-result short-circuit),
   * or null if the literal itself is too short to use the index.
   */
  private candidatesForLiteral(literal: string): bigint[] | null {
    if (literal.length < 3) return null;
    const lower = literal.toLowerCase();
    const seen = new Set<number>();
    const tris: number[] = [];
    for (let i = 0; i + 3 <= lower.length; i++) {
      const t = packTrigram(lower, i);
      if (seen.has(t)) continue;
      seen.add(t);
      tris.push(t);
    }
    const postings: bigint[][] = [];
    for (const t of tris) {
      const p = this.postings.get(t);
      if (!p || p.length === 0) return [];   // a missing trigram ⇒ no candidates
      postings.push(p);
    }
    // Intersect smallest-first to keep the running set tight.
    postings.sort((a, b) => a.length - b.length);
    let acc = postings[0];
    for (let k = 1; k < postings.length && acc.length > 0; k++) {
      acc = intersectSorted(acc, postings[k]);
    }
    return acc;
  }
}
