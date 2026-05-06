/**
 * Cox-style trigram query extraction from a JavaScript regex source.
 *
 * Implements the algorithm from Russ Cox, "Regular Expression Matching with
 * a Trigram Index" (2012), adapted to JS regex syntax.
 *
 * Pipeline:
 *
 *   regex source string
 *      │
 *      ▼  (parseRegex)
 *   regex AST (`Re`)
 *      │
 *      ▼  (computeInfo, recursive bottom-up)
 *   per-node Info { emptyable, exact, prefix, suffix, match }
 *      │
 *      ▼  (queryFromInfo)
 *   trigram query (`TQ`) — boolean expression over trigrams
 *
 * The Info struct at each node tracks four things plus the match query:
 *
 *   - `emptyable`: subtree can match the empty string.
 *   - `exact`:     finite set of complete matches (string[]) or null if
 *                  infinite/unbounded.
 *   - `prefix`:    set of strings; every match begins with one of them.
 *                  null when unbounded.
 *   - `suffix`:    symmetric to prefix.
 *   - `match`:     a boolean trigram expression that must hold for any
 *                  string matching the regex.
 *
 * The string sets are bounded (`SS_LIMIT`); when a combinator would produce
 * a set bigger than the cap, we collapse it to null and lose some precision
 * in exchange for predictable runtime.
 *
 * Concatenation x · y propagates trigrams ACROSS the boundary: if x ends
 * with one of `xSuffix` and y begins with one of `yPrefix`, every match
 * contains some s+p[0..2] window, yielding `OR over (s,p) of AND of those
 * trigrams`. This is the rule that recovers `foo.*bar → AND(foo, bar)` and
 * `(foo|bar)baz → OR(AND(foobaz trigrams), AND(barbaz trigrams))` — the
 * shapes our v1 string-walker punted on.
 */

import { type TrigramQuery, packTrigramAt, allMatch, tqAnd, tqOr, tqTri, encodeUtf8 } from './trigram.js';

// =============================================================================
// AST
// =============================================================================

export type Re =
  | { tag: 'empty' }
  | { tag: 'literal'; ch: string }                                 // single character
  | { tag: 'dot' }                                                  // .
  | { tag: 'class'; chars: string[] | null }                        // [abc] (chars=enumerated) or [^…]/\d/\w/\s (chars=null, "open")
  | { tag: 'anchor' }                                               // ^ $ \b \B and lookarounds (zero-width)
  | { tag: 'concat'; children: Re[] }
  | { tag: 'alt';    children: Re[] }
  | { tag: 'repeat'; child: Re; min: number; max: number };         // max = -1 for unlimited

// =============================================================================
// Parser  (recursive descent over JS regex syntax)
// =============================================================================

class RegexParser {
  private src: string;
  private pos: number;

  constructor(src: string) { this.src = src; this.pos = 0; }

  parse(): Re {
    const re = this.parseAlt();
    if (this.pos !== this.src.length) {
      throw new Error(`regex parse: trailing input at ${this.pos}: ${this.src.slice(this.pos)}`);
    }
    return re;
  }

  private parseAlt(): Re {
    const branches: Re[] = [this.parseConcat()];
    while (this.peek() === '|') {
      this.pos++;
      branches.push(this.parseConcat());
    }
    if (branches.length === 1) return branches[0];
    return { tag: 'alt', children: branches };
  }

  private parseConcat(): Re {
    const parts: Re[] = [];
    while (this.pos < this.src.length) {
      const c = this.peek();
      if (c === '|' || c === ')') break;
      parts.push(this.parseRepeat());
    }
    if (parts.length === 0) return { tag: 'empty' };
    if (parts.length === 1) return parts[0];
    return { tag: 'concat', children: parts };
  }

  private parseRepeat(): Re {
    const atom = this.parseAtom();
    const c = this.peek();
    if (c === '*') { this.pos++; this.consumeLazy(); return { tag: 'repeat', child: atom, min: 0, max: -1 }; }
    if (c === '+') { this.pos++; this.consumeLazy(); return { tag: 'repeat', child: atom, min: 1, max: -1 }; }
    if (c === '?') { this.pos++; this.consumeLazy(); return { tag: 'repeat', child: atom, min: 0, max:  1 }; }
    if (c === '{') {
      // {n}, {n,}, {n,m} — but only treat as quantifier if it parses cleanly,
      // because {abc} and similar should be a literal `{`.
      const start = this.pos;
      this.pos++;
      const numStart = this.pos;
      while (this.pos < this.src.length && /\d/.test(this.src[this.pos])) this.pos++;
      if (this.pos === numStart) { this.pos = start; return atom; }
      const min = Number(this.src.slice(numStart, this.pos));
      let max = min;
      if (this.peek() === ',') {
        this.pos++;
        const m2 = this.pos;
        while (this.pos < this.src.length && /\d/.test(this.src[this.pos])) this.pos++;
        max = m2 === this.pos ? -1 : Number(this.src.slice(m2, this.pos));
      }
      if (this.peek() !== '}') { this.pos = start; return atom; }
      this.pos++;
      this.consumeLazy();
      return { tag: 'repeat', child: atom, min, max };
    }
    return atom;
  }

  private consumeLazy(): void {
    if (this.peek() === '?') this.pos++;  // ??, *?, +?: lazy markers, ignored
  }

  private parseAtom(): Re {
    const c = this.peek();
    if (c === undefined) return { tag: 'empty' };
    if (c === '^' || c === '$') { this.pos++; return { tag: 'anchor' }; }
    if (c === '.') { this.pos++; return { tag: 'dot' }; }
    if (c === '\\') return this.parseEscape();
    if (c === '[') return this.parseClass();
    if (c === '(') return this.parseGroup();
    // Anything else, including ) or |, would have terminated parseConcat already.
    this.pos++;
    return { tag: 'literal', ch: c };
  }

  /**
   * Parse a backslash escape outside a character class. Common cases:
   *  - `\b` `\B`   word-boundary anchors
   *  - `\d`        digits, enumerated (10 chars)
   *  - `\w`        word chars, enumerated (63 chars: A-Za-z0-9_)
   *  - `\s`        whitespace, enumerated (6 chars)
   *  - `\D \W \S`  negations of the above — too big to enumerate, treated as
   *    open (chars=null)
   *  - `\n \t \r \0`        control-character literals
   *  - `\xHH \uHHHH`        hex/unicode literals
   *  - `\1`..`\9`           backreferences (treat as open class)
   *  - everything else      taken as a literal character (so `\.` `\*` `\\`
   *    etc. all become the corresponding literal char)
   */
  private parseEscape(): Re {
    this.pos++; // skip \
    const c = this.src[this.pos++];
    if (c === undefined) throw new Error('regex parse: dangling backslash');
    if (c === 'b' || c === 'B') return { tag: 'anchor' };
    if (c === 'd') return { tag: 'class', chars: ENUM_DIGIT };
    if (c === 'w') return { tag: 'class', chars: ENUM_WORD };
    if (c === 's') return { tag: 'class', chars: ENUM_WHITESPACE };
    if (c === 'D' || c === 'W' || c === 'S') return { tag: 'class', chars: null };
    if (c === 'n') return { tag: 'literal', ch: '\n' };
    if (c === 't') return { tag: 'literal', ch: '\t' };
    if (c === 'r') return { tag: 'literal', ch: '\r' };
    if (c === '0') return { tag: 'literal', ch: '\0' };
    if (c === 'x') {
      const hex = this.src.slice(this.pos, this.pos + 2);
      this.pos += 2;
      return { tag: 'literal', ch: String.fromCharCode(parseInt(hex, 16) || 0) };
    }
    if (c === 'u') {
      // Either \uHHHH or \u{HHHH...}; we accept the simple form.
      if (this.src[this.pos] === '{') {
        const close = this.src.indexOf('}', this.pos);
        if (close < 0) return { tag: 'literal', ch: c };
        const hex = this.src.slice(this.pos + 1, close);
        this.pos = close + 1;
        return { tag: 'literal', ch: String.fromCodePoint(parseInt(hex, 16) || 0) };
      }
      const hex = this.src.slice(this.pos, this.pos + 4);
      this.pos += 4;
      return { tag: 'literal', ch: String.fromCharCode(parseInt(hex, 16) || 0) };
    }
    if (/[1-9]/.test(c)) return { tag: 'class', chars: null };  // backreference — give up
    return { tag: 'literal', ch: c };
  }

  /**
   * Parse a [...] class. Range syntax (`a-z`) expands; escapes inside the
   * class behave like outside the class but with `]` as a delimiter. A
   * negated class (`[^…]`) collapses to "open" (no enumeration), because
   * enumerating the complement would require committing to an alphabet.
   */
  private parseClass(): Re {
    this.pos++; // skip [
    let negated = false;
    if (this.src[this.pos] === '^') { negated = true; this.pos++; }
    const chars = new Set<string>();
    let openClass = false;
    while (this.pos < this.src.length && this.src[this.pos] !== ']') {
      let c: string;
      if (this.src[this.pos] === '\\') {
        this.pos++;
        const e = this.src[this.pos++];
        if (e === undefined) throw new Error('regex parse: dangling backslash in class');
        if (e === 'd' || e === 'D' || e === 'w' || e === 'W' || e === 's' || e === 'S') {
          // Open sub-class inside the class — collapse.
          openClass = true;
          c = '\0';
        } else if (e === 'n') c = '\n';
        else if (e === 't') c = '\t';
        else if (e === 'r') c = '\r';
        else if (e === '0') c = '\0';
        else if (e === 'x') {
          c = String.fromCharCode(parseInt(this.src.slice(this.pos, this.pos + 2), 16) || 0);
          this.pos += 2;
        } else if (e === 'u') {
          c = String.fromCharCode(parseInt(this.src.slice(this.pos, this.pos + 4), 16) || 0);
          this.pos += 4;
        } else c = e;
      } else {
        c = this.src[this.pos++];
      }
      // Range a-b ?
      if (this.src[this.pos] === '-' && this.src[this.pos + 1] !== ']' && this.src[this.pos + 1] !== undefined) {
        this.pos++; // skip -
        let endCh: string;
        if (this.src[this.pos] === '\\') {
          this.pos++;
          endCh = this.src[this.pos++] ?? c;
        } else {
          endCh = this.src[this.pos++];
        }
        const lo = c.charCodeAt(0);
        const hi = endCh.charCodeAt(0);
        if (hi < lo || hi - lo > 256) {
          openClass = true;
        } else {
          for (let cc = lo; cc <= hi; cc++) chars.add(String.fromCharCode(cc));
        }
      } else {
        if (!openClass) chars.add(c);
      }
    }
    if (this.pos < this.src.length) this.pos++;  // skip ]
    if (openClass || negated) return { tag: 'class', chars: null };
    return { tag: 'class', chars: [...chars] };
  }

  /**
   * Parse a (…) group. Recognizes JS group prefixes:
   *   (?:…)        non-capturing
   *   (?=…) (?!…)  lookahead / negative lookahead     → zero-width (anchor)
   *   (?<=…) (?<!…) lookbehind / negative lookbehind  → zero-width (anchor)
   *   (?<name>…)   named capture
   *   (…)          plain capture
   */
  private parseGroup(): Re {
    this.pos++;
    if (this.src[this.pos] === '?') {
      this.pos++;
      const c = this.src[this.pos];
      if (c === ':') {
        this.pos++;
      } else if (c === '=' || c === '!') {
        this.pos++;
        this.parseAlt();
        if (this.src[this.pos] === ')') this.pos++;
        return { tag: 'anchor' };
      } else if (c === '<') {
        const next = this.src[this.pos + 1];
        if (next === '=' || next === '!') {
          this.pos += 2;
          this.parseAlt();
          if (this.src[this.pos] === ')') this.pos++;
          return { tag: 'anchor' };
        }
        // Named capture (?<name>...) — skip the name.
        while (this.pos < this.src.length && this.src[this.pos] !== '>') this.pos++;
        if (this.src[this.pos] === '>') this.pos++;
      }
    }
    const inner = this.parseAlt();
    if (this.src[this.pos] === ')') this.pos++;
    return inner;
  }

  private peek(): string | undefined { return this.src[this.pos]; }
}

export function parseRegex(source: string): Re {
  return new RegexParser(source).parse();
}

// =============================================================================
// Info propagation (Cox's algorithm)
// =============================================================================

/**
 * Bounded set of byte sequences; null means "open" (unbounded or unknown).
 *
 * The byte representation is the canonical form throughout the regex →
 * trigram pipeline. Every literal char from the regex source is encoded to
 * UTF-8 at info creation; all downstream cross / union / boundary
 * operations work on byte arrays. This is the same model `codesearch` /
 * `livegrep` / `zoekt` use, and it gives multi-byte Unicode chars (Greek
 * `α`, math `≤`, CJK, etc.) full per-byte selectivity instead of folding
 * everything above 0x7F into a single 256-byte alphabet.
 */
type SS = Uint8Array[] | null;

interface Info {
  emptyable: boolean;
  exact:  SS;
  prefix: SS;
  suffix: SS;
  match:  TrigramQuery;
}

/**
 * Cap for exact / prefix / suffix sets — past this, collapse to null.
 *
 * Sized large enough that practical concatenations of small char-classes
 * (`\d{4}` = 10⁴ = 10 K is over; `\d{3}` = 1000 is also over but the
 * single-element cross of a 3-digit literal sequence stays linear) keep
 * useful info. Multi-wildcard regexes blow past this and degrade to the
 * non-exact path via the cross-fallback in {@link infoConcat}.
 */
const SS_LIMIT = 256;

const EMPTY_BYTES = new Uint8Array(0);

const ENUM_DIGIT: string[] = '0123456789'.split('');
/**
 * `\w` = [A-Za-z0-9_]. Lowercased + deduped to [a-z0-9_] — same set under
 * case-insensitive matching, smaller (37 chars vs 63).
 */
const ENUM_WORD:  string[] = (() => {
  const out: string[] = [];
  for (let c = 0x30; c <= 0x39; c++) out.push(String.fromCharCode(c));     // 0-9
  for (let c = 0x61; c <= 0x7a; c++) out.push(String.fromCharCode(c));     // a-z (covers A-Z under case-fold)
  out.push('_');
  return out;
})();
const ENUM_WHITESPACE: string[] = [' ', '\t', '\n', '\r', '\f', '\v'];

/** Stable string key for a byte sequence — usable as a Set/Map key. */
function bytesKey(b: Uint8Array): string {
  // String of code units in 0..255. 1-1 mapping with bytes; same byte
  // sequence ⇒ same string. We DON'T use this for trigram packing; only as
  // a dedupe key.
  let s = '';
  for (let i = 0; i < b.length; i++) s += String.fromCharCode(b[i]);
  return s;
}

function bytesConcat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function ssOk(set: Uint8Array[] | null): SS {
  return set !== null && set.length <= SS_LIMIT ? set : null;
}

function ssCross(a: Uint8Array[], b: Uint8Array[]): SS {
  if (a.length * b.length > SS_LIMIT) return null;
  const seen = new Set<string>();
  const out: Uint8Array[] = [];
  for (const x of a) for (const y of b) {
    const c = bytesConcat(x, y);
    const k = bytesKey(c);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(c);
    if (out.length > SS_LIMIT) return null;
  }
  return out;
}

function ssUnion(a: Uint8Array[], b: Uint8Array[]): SS {
  const seen = new Set<string>();
  const out: Uint8Array[] = [];
  for (const arr of [a, b]) for (const x of arr) {
    const k = bytesKey(x);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(x);
  }
  if (out.length > SS_LIMIT) return null;
  return out;
}

function infoEmpty(): Info {
  return { emptyable: true, exact: [EMPTY_BYTES], prefix: [EMPTY_BYTES], suffix: [EMPTY_BYTES], match: allMatch() };
}

function infoLiteral(ch: string): Info {
  // Unicode-aware case fold, then UTF-8 encode. Same path the index takes
  // on insert (`text.toLowerCase()` then `encodeUtf8`), so the resulting
  // byte sequences agree with the index's. Multi-byte chars become 2-4
  // bytes each — every byte contributes to trigram selectivity.
  const bytes = encodeUtf8(ch.toLowerCase());
  return { emptyable: false, exact: [bytes], prefix: [bytes], suffix: [bytes], match: allMatch() };
}

function infoOpen(): Info {  // unsupported / unenumerated class (negated, \D, \W, \S, etc.) AND dot.
  return { emptyable: false, exact: null, prefix: null, suffix: null, match: allMatch() };
}

/**
 * In a byte-level model, JS regex `.` matches one code unit which is 1-2
 * bytes for BMP code points (and 2 code units for surrogate pairs ⇒ 4
 * bytes total). Enumerating "any single byte" is unsound — the trigram
 * filter would miss matches where `.` corresponds to a multi-byte char.
 * Enumerating all valid UTF-8 byte sequences is too large (~1.1 M for
 * 4-byte code points). So `.` collapses to open: `a.c`-style queries fall
 * back to the linear scan, but the speedup on every other query shape
 * (literals, classes, anchors, alternation, quantifiers over enumerable
 * atoms) is fully preserved.
 */
function infoDot(): Info {
  return infoOpen();
}

function infoAnchor(): Info {
  return { emptyable: true, exact: [EMPTY_BYTES], prefix: [EMPTY_BYTES], suffix: [EMPTY_BYTES], match: allMatch() };
}

function infoClass(chars: string[] | null): Info {
  if (chars === null || chars.length === 0) return infoOpen();
  // Lowercase + UTF-8 encode + dedupe. Each entry is one byte-sequence
  // (multi-byte chars expand here, just like literals).
  const seen = new Set<string>();
  const folded: Uint8Array[] = [];
  for (const c of chars) {
    const b = encodeUtf8(c.toLowerCase());
    const k = bytesKey(b);
    if (seen.has(k)) continue;
    seen.add(k);
    folded.push(b);
  }
  if (folded.length === 0) return infoOpen();
  return { emptyable: false, exact: ssOk(folded), prefix: ssOk(folded), suffix: ssOk(folded), match: allMatch() };
}

/**
 * Produce the trigram query for a boundary between `xSuffix` and `yPrefix`.
 * For every (s, p) pair, the trigrams that span the boundary in s+p must
 * appear. With multiple (s, p) pairs we OR over them.
 */
function boundaryQuery(xSuffix: SS, yPrefix: SS): TrigramQuery {
  if (xSuffix === null || yPrefix === null) return allMatch();
  const branches: TrigramQuery[] = [];
  for (const s of xSuffix) {
    for (const p of yPrefix) {
      const combined = bytesConcat(s, p);
      // Trigrams strictly spanning the boundary: byte positions i in
      //   max(0, |s|-2) .. min(|combined|-3, |s|-1)
      const lo = Math.max(0, s.length - 2);
      const hi = Math.min(combined.length - 3, s.length - 1);
      const tris: TrigramQuery[] = [];
      for (let i = lo; i <= hi; i++) tris.push(tqTri(packTrigramAt(combined, i)));
      branches.push(tqAnd(tris));
    }
  }
  return tqOr(branches);
}

function infoConcat(x: Info, y: Info): Info {
  const emptyable = x.emptyable && y.emptyable;

  // exact: cross product, only when both sides are finite.
  let exact: SS = null;
  if (x.exact !== null && y.exact !== null) exact = ssCross(x.exact, y.exact);

  // prefix:
  //   - If x is finite (x.exact non-null), every match's prefix is one of
  //     x.exact's strings extended by y's prefix (or just x.exact when y is
  //     unbounded). The cross handles the empty-x-string member implicitly
  //     (it concatenates with y.prefix), so no separate emptyable adjustment.
  //     If the cross product would exceed SS_LIMIT we fall back to x.prefix
  //     (still valid: every match starts with one of x.prefix's strings) —
  //     less precise but never wrong.
  //   - If x is unbounded but x.prefix is known, every match's prefix is at
  //     least x.prefix. We can union y.prefix into it ONLY when x is also
  //     emptyable (empty x ⇒ match starts at y).
  //   - Crucial: union with `null` (open) must collapse to `null`, never to
  //     the other operand.
  let prefix: SS;
  if (x.exact !== null) {
    if (y.prefix !== null) {
      prefix = ssCross(x.exact, y.prefix);
      if (prefix === null) prefix = x.prefix;     // cross exceeded limit; less-precise fallback
    } else {
      prefix = x.prefix;
    }
  } else {
    prefix = x.prefix;
    if (x.emptyable && prefix !== null && y.prefix !== null) {
      prefix = ssUnion(prefix, y.prefix);
    }
  }

  // suffix: symmetric.
  let suffix: SS;
  if (y.exact !== null) {
    if (x.suffix !== null) {
      suffix = ssCross(x.suffix, y.exact);
      if (suffix === null) suffix = y.suffix;
    } else {
      suffix = y.suffix;
    }
  } else {
    suffix = y.suffix;
    if (y.emptyable && suffix !== null && x.suffix !== null) {
      suffix = ssUnion(suffix, x.suffix);
    }
  }

  const match = tqAnd([x.match, y.match, boundaryQuery(x.suffix, y.prefix)]);
  return { emptyable, exact, prefix, suffix, match };
}

function infoAlt(x: Info, y: Info): Info {
  const emptyable = x.emptyable || y.emptyable;
  let exact: SS = null;
  if (x.exact !== null && y.exact !== null) exact = ssUnion(x.exact, y.exact);
  let prefix: SS = null;
  if (x.prefix !== null && y.prefix !== null) prefix = ssUnion(x.prefix, y.prefix);
  let suffix: SS = null;
  if (x.suffix !== null && y.suffix !== null) suffix = ssUnion(x.suffix, y.suffix);
  const match = tqOr([x.match, y.match]);
  return { emptyable, exact, prefix, suffix, match };
}

function infoStar(_x: Info): Info {
  // x* matches the empty string, so we cannot require any content.
  return { emptyable: true, exact: null, prefix: null, suffix: null, match: allMatch() };
}

function infoPlus(x: Info): Info {
  // x+ = x followed by x*. Every match contains at least one x, so x.match
  // is required and x.prefix/x.suffix bound the start/end of any match.
  return { emptyable: x.emptyable, exact: null, prefix: x.prefix, suffix: x.suffix, match: x.match };
}

function infoQuestion(x: Info): Info {
  return infoAlt(x, infoEmpty());
}

function infoRepeat(x: Info, min: number, max: number): Info {
  if (min === 0 && max === 1)  return infoQuestion(x);
  if (min === 0 && max === -1) return infoStar(x);
  if (min === 1 && max === -1) return infoPlus(x);
  if (min >= 1 && max === min) {
    // x{n}: concat x with itself n times.
    let acc = x;
    for (let i = 1; i < min; i++) acc = infoConcat(acc, x);
    return acc;
  }
  if (min >= 1) {
    // x{min,max} where max > min (or unlimited): require x{min}, then ANY tail.
    let acc = x;
    for (let i = 1; i < min; i++) acc = infoConcat(acc, x);
    return infoConcat(acc, infoStar(x));
  }
  // min == 0, max > 0 — like x* (matches empty)
  return infoStar(x);
}

function computeInfo(re: Re): Info {
  switch (re.tag) {
    case 'empty':  return infoEmpty();
    case 'literal': return infoLiteral(re.ch);
    case 'dot':    return infoDot();
    case 'class':  return infoClass(re.chars);
    case 'anchor': return infoAnchor();
    case 'concat': {
      let acc = infoEmpty();
      for (const c of re.children) acc = infoConcat(acc, computeInfo(c));
      return acc;
    }
    case 'alt': {
      let acc = computeInfo(re.children[0]);
      for (let i = 1; i < re.children.length; i++) acc = infoAlt(acc, computeInfo(re.children[i]));
      return acc;
    }
    case 'repeat':
      return infoRepeat(computeInfo(re.child), re.min, re.max);
  }
}

// =============================================================================
// Top-level: regex source → trigram query
// =============================================================================

/**
 * Build the trigram query for every distinct member of `exact`. Each member
 * contributes AND of its own trigrams; the result is the OR of those branches.
 *
 * Members shorter than 3 chars contribute no trigrams — and any match that
 * could be that member would slip through the index. So if any member is
 * too short, the exact-derived path is unsound; return null and let the
 * caller fall back to `info.match`.
 */
function tqFromExact(exact: Uint8Array[]): TrigramQuery | null {
  // `exact` byte sequences are already lowercased + UTF-8 encoded —
  // infoLiteral / infoClass fold + encode at info-creation time, so each
  // member is in the index's canonical byte form. We measure trigram
  // boundaries in BYTES, not code units, so multi-byte chars produce
  // multiple internal trigrams.
  const branches: TrigramQuery[] = [];
  for (const e of exact) {
    if (e.length < 3) return null;
    const tris: TrigramQuery[] = [];
    for (let i = 0; i + 3 <= e.length; i++) tris.push(tqTri(packTrigramAt(e, i)));
    branches.push(tqAnd(tris));
  }
  return tqOr(branches);
}

/**
 * Lowercase every trigram leaf in a query so that case-insensitive regex
 * lookups (which the index has lowercased going in) line up. We do this as
 * a final pass so the upstream Info computation works on the regex source
 * as authored.
 */
function lowercaseLeaves(q: TrigramQuery): TrigramQuery {
  // Nothing to do — packTrigramAt already lowercases ASCII A–Z. The Info
  // computation feeds `combined` (a substring of the source / source's
  // exact set) directly to `packTrigramAt`, which lowercases. So we're
  // already case-insensitive at the trigram level. Function kept for
  // documentation / future Unicode normalization.
  return q;
}

/**
 * Public entry point: regex source → trigram query, or null if the regex
 * doesn't yield any useful filter.
 *
 * Returns:
 *   - `null` when no filter can be extracted (e.g., `.*`, `\\D+`, regexes
 *     where Info collapses to AnyMatch). The caller must full-scan.
 *   - A `TrigramQuery` to be evaluated against the index. `none` is also
 *     possible — it means "regex matches no string in the index" and can
 *     short-circuit the post-filter.
 */
export function buildTrigramQuery(source: string): TrigramQuery | null {
  let re: Re;
  try {
    re = parseRegex(source);
  } catch {
    return null;
  }
  const info = computeInfo(re);

  // Prefer exact-derived query when available — most selective.
  if (info.exact !== null && info.exact.length > 0) {
    const q = tqFromExact(info.exact);
    if (q !== null && q.tag !== 'all') return lowercaseLeaves(q);
  }

  // Fall back to the recursive match query.
  if (info.match.tag === 'all') return null;
  return lowercaseLeaves(info.match);
}
