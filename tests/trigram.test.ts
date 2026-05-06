/**
 * Unit tests for the trigram inverted index and Cox-style regex → trigram
 * query construction.
 *
 * Two surfaces under test:
 *   - {@link TrigramIndex} (`add` / `remove` / `rebuild` / `evaluateQuery`)
 *   - {@link buildTrigramQuery} — regex AST → boolean trigram expression
 *
 * The integration soundness invariant we lean on everywhere: false positives
 * are fine (the post-filter regex eliminates them), false negatives are NOT
 * (they cause `searchNodes` to silently miss matches). So every test that
 * asserts on candidates() output is checking the *superset* property — the
 * candidate set must include every actually-matching string.
 */

import { describe, it, expect } from '@jest/globals';
import {
  TrigramIndex,
  packTrigramAt,
  encodeUtf8,
  tqAnd,
  tqOr,
  tqTri,
  allMatch,
  noMatch,
  type TrigramQuery,
} from '../src/trigram.js';
import { buildTrigramQuery, parseRegex } from '../src/regex_query.js';

/**
 * Helper: build a `tri` leaf from a 3-byte ASCII string. Byte-level
 * indexing means we test in the same canonical form the index uses.
 */
const tri = (s: string): TrigramQuery => tqTri(packTrigramAt(encodeUtf8(s.toLowerCase()), 0));

describe('TrigramIndex', () => {
  describe('add / remove / size', () => {
    it('starts empty', () => {
      const idx = new TrigramIndex();
      expect(idx.size).toBe(0);
      expect(idx.distinctTrigrams).toBe(0);
    });

    it('add increments size and indexes trigrams', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar');
      expect(idx.size).toBe(1);
      // 'foobar' has trigrams: foo, oob, oba, bar (4 distinct)
      expect(idx.distinctTrigrams).toBe(4);
    });

    it('add is idempotent for the same id', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar');
      idx.add(1n, 'foobar');
      idx.add(1n, 'completely-different-text-which-should-be-ignored');
      expect(idx.size).toBe(1);
      expect(idx.distinctTrigrams).toBe(4);
    });

    it('remove undoes add', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar');
      idx.add(2n, 'bazqux');
      idx.remove(1n);
      expect(idx.size).toBe(1);
      expect(idx.distinctTrigrams).toBe(4);
    });

    it('remove no-ops on unknown id', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar');
      expect(() => idx.remove(99n)).not.toThrow();
      expect(idx.size).toBe(1);
    });

    it('handles strings shorter than 3 chars', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'ab');
      expect(idx.size).toBe(1);
      expect(idx.distinctTrigrams).toBe(0);
      idx.add(1n, 'cd');
      expect(idx.size).toBe(1);
    });

    it('clear wipes everything', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar');
      idx.add(2n, 'bazqux');
      idx.clear();
      expect(idx.size).toBe(0);
      expect(idx.distinctTrigrams).toBe(0);
    });
  });

  describe('rebuild', () => {
    it('produces the same state as a sequence of adds', () => {
      const items: [bigint, string][] = [
        [1n, 'foobar'],
        [2n, 'bazqux'],
        [3n, 'foobaz'],
      ];

      const seq = new TrigramIndex();
      for (const [id, text] of items) seq.add(id, text);

      const bulk = new TrigramIndex();
      bulk.rebuild(items);

      expect(bulk.size).toBe(seq.size);
      expect(bulk.distinctTrigrams).toBe(seq.distinctTrigrams);
      expect(bulk.evaluateQuery(tri('foo'))).toEqual(seq.evaluateQuery(tri('foo')));
      expect(bulk.evaluateQuery(tri('baz'))).toEqual(seq.evaluateQuery(tri('baz')));
    });
  });

  describe('evaluateQuery', () => {
    const idx = new TrigramIndex();
    idx.add(10n, 'memory file');
    idx.add(20n, 'graphfile');
    idx.add(30n, 'String Table');
    idx.add(40n, 'concurrent memory access');
    idx.add(50n, 'lockholder');
    idx.add(60n, 'foo');                     // exactly 3 chars
    idx.add(70n, 'no-relevant-substrings');

    it('returns null for an `all` query', () => {
      expect(idx.evaluateQuery(allMatch())).toBeNull();
    });

    it('returns empty array for a `none` query', () => {
      expect(idx.evaluateQuery(noMatch())).toEqual([]);
    });

    it('returns empty array when a required trigram has no postings', () => {
      // 'zzz' — no document contains it
      expect(idx.evaluateQuery(tri('zzz'))).toEqual([]);
    });

    it('returns sorted candidate IDs for a single trigram', () => {
      // 'mem' appears in "memory file" and "concurrent memory access"
      expect(idx.evaluateQuery(tri('mem'))).toEqual([10n, 40n]);
    });

    it('matches case-insensitively', () => {
      // 'String Table' contains 'str' (lowercased)
      expect(idx.evaluateQuery(tri('STR'))).toContain(30n);
    });

    it('AND: intersection of trigrams', () => {
      // String containing both `mem` AND `con` — only id 40
      const q = tqAnd([tri('mem'), tri('con')]);
      expect(idx.evaluateQuery(q)).toEqual([40n]);
    });

    it('OR: union of trigrams', () => {
      // String containing `mem` OR `gra` — ids 10, 20, 40
      const q = tqOr([tri('mem'), tri('gra')]);
      const got = idx.evaluateQuery(q)!;
      expect(got.sort()).toEqual([10n, 20n, 40n].sort());
    });

    it('AND with `all` operand drops the operand (no constraint)', () => {
      // {mem AND all} should equal {mem}
      const q = tqAnd([tri('mem'), allMatch()]);
      expect(idx.evaluateQuery(q)).toEqual(idx.evaluateQuery(tri('mem')));
    });

    it('AND with `none` operand short-circuits to []', () => {
      const q = tqAnd([tri('mem'), noMatch()]);
      expect(idx.evaluateQuery(q)).toEqual([]);
    });

    it('OR with `all` operand short-circuits to null (must scan)', () => {
      const q = tqOr([tri('mem'), allMatch()]);
      expect(idx.evaluateQuery(q)).toBeNull();
    });

    it('OR with `none` operand drops the operand', () => {
      const q = tqOr([tri('mem'), noMatch()]);
      expect(idx.evaluateQuery(q)).toEqual(idx.evaluateQuery(tri('mem')));
    });
  });

  describe('add → remove → search invariants', () => {
    it('removed strings do not appear in candidates', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar contains foo');
      idx.add(2n, 'another foo here');
      expect(idx.evaluateQuery(tri('foo'))).toEqual([1n, 2n]);
      idx.remove(1n);
      expect(idx.evaluateQuery(tri('foo'))).toEqual([2n]);
      idx.remove(2n);
      expect(idx.evaluateQuery(tri('foo'))).toEqual([]);
    });
  });
});

// =============================================================================
// Regex AST + Cox's algorithm
// =============================================================================

describe('parseRegex', () => {
  it('parses literals', () => {
    expect(parseRegex('foo')).toMatchObject({
      tag: 'concat',
      children: [
        { tag: 'literal', ch: 'f' },
        { tag: 'literal', ch: 'o' },
        { tag: 'literal', ch: 'o' },
      ],
    });
  });

  it('parses dot', () => {
    expect(parseRegex('.')).toMatchObject({ tag: 'dot' });
  });

  it('parses anchors', () => {
    expect(parseRegex('^foo$')).toMatchObject({
      tag: 'concat',
      children: [
        { tag: 'anchor' },
        { tag: 'literal', ch: 'f' },
        { tag: 'literal', ch: 'o' },
        { tag: 'literal', ch: 'o' },
        { tag: 'anchor' },
      ],
    });
  });

  it('parses alternation', () => {
    const re = parseRegex('foo|bar');
    expect(re.tag).toBe('alt');
  });

  it('parses character classes with ranges', () => {
    const re = parseRegex('[a-c]');
    expect(re).toMatchObject({ tag: 'class' });
    if (re.tag === 'class' && re.chars !== null) {
      expect(re.chars.sort()).toEqual(['a', 'b', 'c'].sort());
    }
  });

  it('treats negated classes as open', () => {
    const re = parseRegex('[^a]');
    expect(re).toMatchObject({ tag: 'class', chars: null });
  });

  it('enumerates predefined classes (\\d, \\w, \\s) where the alphabet is small', () => {
    const d = parseRegex('\\d');
    expect(d).toMatchObject({ tag: 'class' });
    if (d.tag === 'class' && d.chars !== null) expect(d.chars).toContain('5');

    const w = parseRegex('\\w');
    expect(w).toMatchObject({ tag: 'class' });
    if (w.tag === 'class' && w.chars !== null) {
      expect(w.chars).toContain('a');
      expect(w.chars).toContain('z');
      expect(w.chars).toContain('5');
      expect(w.chars).toContain('_');
      // Uppercase A-Z is folded to a-z at enumeration time (case-insensitive
      // matching), so we don't expect 'Z' in the set.
    }

    const s = parseRegex('\\s');
    expect(s).toMatchObject({ tag: 'class' });
    if (s.tag === 'class' && s.chars !== null) expect(s.chars).toContain(' ');
  });

  it('keeps \\D, \\W, \\S as open (negation alphabets are too big)', () => {
    expect(parseRegex('\\D')).toMatchObject({ tag: 'class', chars: null });
    expect(parseRegex('\\W')).toMatchObject({ tag: 'class', chars: null });
    expect(parseRegex('\\S')).toMatchObject({ tag: 'class', chars: null });
  });

  it('treats word boundaries as anchors', () => {
    expect(parseRegex('\\b')).toMatchObject({ tag: 'anchor' });
    expect(parseRegex('\\B')).toMatchObject({ tag: 'anchor' });
  });

  it('parses escaped literals', () => {
    expect(parseRegex('\\.')).toMatchObject({ tag: 'literal', ch: '.' });
    expect(parseRegex('\\\\')).toMatchObject({ tag: 'literal', ch: '\\' });
  });

  it('parses quantifiers', () => {
    expect(parseRegex('a*').tag).toBe('repeat');
    expect(parseRegex('a+').tag).toBe('repeat');
    expect(parseRegex('a?').tag).toBe('repeat');
    expect(parseRegex('a{3}').tag).toBe('repeat');
    expect(parseRegex('a{2,5}').tag).toBe('repeat');
  });

  it('parses groups', () => {
    const re = parseRegex('(foo|bar)');
    expect(re.tag).toBe('alt');
  });

  it('parses non-capturing groups', () => {
    const re = parseRegex('(?:foo|bar)');
    expect(re.tag).toBe('alt');
  });

  it('treats lookarounds as anchors', () => {
    const re = parseRegex('(?=foo)bar');
    if (re.tag === 'concat') {
      expect(re.children[0]).toMatchObject({ tag: 'anchor' });
    }
  });
});

describe('buildTrigramQuery — Cox-style trigram extraction', () => {
  // Helper: given a regex source and a list of strings, hand-check that the
  // trigram filter built from the source classifies each string consistently
  // with whether the regex actually matches it. Specifically, the SOUNDNESS
  // check: every string that the regex matches must be admitted by the
  // filter (no false negatives).
  function assertSound(source: string, strings: string[]): void {
    const q = buildTrigramQuery(source);
    if (q === null) return;  // Filter is "all" — no constraint, soundness is trivial.
    const re = new RegExp(source, 'i');
    const idx = new TrigramIndex();
    for (let i = 0; i < strings.length; i++) idx.add(BigInt(i), strings[i]);
    const candidates = idx.evaluateQuery(q);
    if (candidates === null) return;  // Filter collapsed to "all" against this index.
    const candidateSet = new Set(candidates.map(b => Number(b)));
    for (let i = 0; i < strings.length; i++) {
      if (re.test(strings[i])) {
        expect(candidateSet.has(i)).toBe(true);  // soundness: matchers must be in candidates
      }
    }
  }

  it('plain literal extracts the right trigrams', () => {
    const q = buildTrigramQuery('memory');
    expect(q).not.toBeNull();
    assertSound('memory', [
      'memory', 'in-memory database', 'memorial', 'unrelated', 'mem', 'mory',
    ]);
  });

  it('anchored literal: ^foo$ behaves like foo for trigram purposes', () => {
    assertSound('^memory$', ['memory', 'in-memory database', 'memorial', 'foo']);
  });

  it('top-level alternation', () => {
    assertSound('foo|bar|baz', [
      'something foo', 'bar in middle', 'ends with baz', 'qux only', 'foobarbaz',
    ]);
  });

  it('CONCAT-OVER-DOT: foo.*bar extracts AND(foo, bar)', () => {
    // This is the headline case the v1 string-walker punted on.
    const q = buildTrigramQuery('foo.*bar');
    expect(q).not.toBeNull();
    assertSound('foo.*bar', [
      'foo and bar in same line', 'foobar', 'just foo', 'just bar', 'unrelated',
    ]);
  });

  it('CONCAT-OVER-DOT: foo.bar extracts AND(foo, bar)', () => {
    assertSound('foo.bar', ['fooXbar', 'foobar', 'fooXXbar', 'foo bar but more']);
  });

  it('OPTIONAL: colou?r extracts via exact set OR(color, colour)', () => {
    assertSound('colou?r', ['my color', 'my colour', 'colourful', 'colorful', 'unrelated']);
  });

  it('PARENS: (foo|bar)baz extracts via exact set', () => {
    assertSound('(foo|bar)baz', [
      'foobaz here', 'barbaz here', 'just baz', 'foo bar baz', 'unrelated',
    ]);
  });

  it('CHAR CLASS: f[oa]o extracts via exact set OR(foo, fao)', () => {
    assertSound('f[oa]o', ['foo here', 'fao here', 'fbo here', 'unrelated']);
  });

  it('PLUS: ab+c extracts AND(abc) (smallest match)', () => {
    assertSound('ab+c', ['abc here', 'abbc here', 'abbbc here', 'ac here', 'unrelated']);
  });

  it('ESCAPED LITERAL: \\.com', () => {
    assertSound('\\.com', ['site.com here', '.com', 'comatose', 'site dot com']);
  });

  it('WORD BOUNDARIES: \\bfoo\\b are anchors only', () => {
    assertSound('\\bfoo\\b', ['my foo here', 'foobar here', 'unrelated']);
  });

  it('returns null for queries with no useful constraint', () => {
    expect(buildTrigramQuery('.*')).toBeNull();
    expect(buildTrigramQuery('.')).toBeNull();         // single char — too short
    expect(buildTrigramQuery('\\d+')).toBeNull();       // emptyable repeat over a class
    // `a.c` returns null in the byte-level model. JS regex `.` matches one
    // code unit which is 1-2 bytes for BMP chars (or a 4-byte surrogate
    // pair) — enumerating "any single byte" would produce false negatives
    // on multi-byte content. We treat `.` as open and fall back to scan.
    // The soundness check below still guards correctness.
    expect(buildTrigramQuery('a.c')).toBeNull();
  });

  it('DOT-CONTAINING SHORT REGEXES: a.c falls back to scan, soundly', () => {
    // `a.c` has only 1-byte literals around a single open `.`, so no
    // byte-trigram boundary can be formed. The extractor returns null and
    // the linear scan handles correctness.
    assertSound('a.c', ['abc here', 'aXc somewhere', 'aαc unicode', 'ac short', 'unrelated']);
  });

  it('case-insensitive at the trigram level', () => {
    // The index lowercases on insert; the regex side uses the same packTrigramAt
    // which also lowercases. So buildTrigramQuery('FOO') should match 'foo'.
    const q = buildTrigramQuery('FOO');
    const idx = new TrigramIndex();
    idx.add(1n, 'foo here');
    expect(idx.evaluateQuery(q!)).toContain(1n);
  });

  it('UTF-8 BYTE-LEVEL: multi-byte literals produce internal trigrams', () => {
    // `αβγ` is 6 UTF-8 bytes (each Greek letter is 2 bytes: 0xCE-0xCF
    // lead). With byte-level trigrams, this is 4 trigrams of length 3
    // bytes each, vs the old "char" model where 3 code units gave only 1
    // trigram (and 3 collisions onto bytes 0xB1, 0xB2, 0xB3).
    //
    // We verify by building a small index of Greek strings, querying for a
    // multi-byte literal, and asserting selectivity > what 1-char-per-
    // trigram would give. The strict check is that the candidate set
    // includes only strings actually containing the literal.
    const idx = new TrigramIndex();
    idx.add(1n, 'the equation αβγ holds');
    idx.add(2n, 'no greek here at all');
    idx.add(3n, 'αβ but no γ');
    idx.add(4n, 'just γ on its own');
    idx.add(5n, 'mixed αβγδ epsilon');

    const q = buildTrigramQuery('αβγ');
    expect(q).not.toBeNull();
    const candidates = idx.evaluateQuery(q!);
    expect(candidates).not.toBeNull();
    // Strings 1 and 5 contain "αβγ"; 2/3/4 don't.
    expect(new Set(candidates!.map(b => Number(b)))).toEqual(new Set([1, 5]));
  });

  it('UTF-8 BYTE-LEVEL: ASCII-only queries unchanged', () => {
    // Soundness on a representative ASCII query — same as before, just
    // routed through the byte path now.
    assertSound('memory', ['memory leak', 'no match', 'memorial']);
  });

  it('case-folds non-ASCII characters consistently between regex source and index', () => {
    // Regression test for the boundary-trigram case-fold bug. The index
    // runs Unicode-aware toLowerCase on insert (so `Ä` becomes `ä` = 0xE4),
    // but packTrigramAt only does ASCII-A–Z bitwise lowercasing — `Ä`
    // (0xC4) wasn't being folded on the regex side. Trigrams in the
    // boundary region of a concat (e.g. `bcÄ.+`) used to mismatch.
    //
    // The fix: lowercase chars at info-creation time (infoLiteral /
    // infoClass) so every downstream set is already in the index's
    // canonical case-folded form.
    assertSound('Äbc', ['Äbc here', 'äbc here', 'unrelated']);
    assertSound('αβγ', ['αβγ in greek', 'ΑΒΓ uppercase greek', 'unrelated']);
    // Boundary case: non-ASCII char near a concat suffix.
    assertSound('xyzÄ.foo', ['xyzÄqfoo', 'xyzäqfoo', 'unrelated']);
  });

  it('LaTeX / math content: ASCII-only commands work as expected', () => {
    // The common LaTeX shape: `\sum`, `\alpha`, etc. is pure ASCII, so
    // selectivity is high regardless of Unicode handling.
    assertSound('\\\\sum', ['\\sum_{i=1}^{n} x_i', 'unrelated', 'no sum here']);
    assertSound('\\\\alpha\\\\beta', ['\\alpha\\beta cdot', 'no greek', 'just \\alpha']);
  });

  it('end-to-end: every shape from the bench passes soundness', () => {
    // Same query shapes the bench drives. Run soundness on each against a
    // small synthetic corpus.
    const corpus = [
      'memory leak',
      'graph traversal',
      'pagerank algorithm',
      'StringTable internals',
      'lockholder pid',
      'observation log',
      'concurrent memory access',
      'rebuildNameIndex called',
      'Self entity',
      'Lev',
      'Claude',
      'foo bar baz qux',
      'unrelated text',
    ];
    const queries = [
      'memory', 'graph', 'pagerank', 'StringTable', 'lock', 'observation',
      'concurrent', 'rebuildNameIndex', '^Self$', '^Lev$', '^Claude$',
      'memory|graph', 'foo|bar|baz|qux',
    ];
    for (const q of queries) assertSound(q, corpus);
  });
});
