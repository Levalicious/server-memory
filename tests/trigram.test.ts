/**
 * Unit tests for the trigram inverted index.
 *
 * Covers the API contract that `searchNodes` will rely on:
 *   - add / remove are inverses
 *   - rebuild is equivalent to a sequence of adds
 *   - candidates() never returns a false negative (every string that
 *     contains a literal must appear in the candidate set)
 *   - candidates() handles the documented edge cases: empty literal list,
 *     short literal (returns null), missing trigram (empty result)
 *
 * False *positives* are allowed by design — that's why search_nodes still
 * post-filters with a real regex. The test set is sized so we can verify the
 * post-filter eliminates the false positives by counting actual containment.
 */

import { describe, it, expect } from '@jest/globals';
import { TrigramIndex } from '../src/trigram.js';

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
      // Only bazqux's trigrams remain: baz, azq, zqu, qux
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
      // size still increments — the entry is "indexed" but contributes no trigrams
      expect(idx.size).toBe(1);
      expect(idx.distinctTrigrams).toBe(0);
      // Adding the same id again is still a no-op
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
      // candidates() should agree on a representative query
      expect(bulk.candidates(['foo'])).toEqual(seq.candidates(['foo']));
      expect(bulk.candidates(['baz'])).toEqual(seq.candidates(['baz']));
    });
  });

  describe('candidates()', () => {
    const idx = new TrigramIndex();
    idx.add(10n, 'memory file');
    idx.add(20n, 'graphfile');
    idx.add(30n, 'String Table');
    idx.add(40n, 'concurrent memory access');
    idx.add(50n, 'lockholder');
    idx.add(60n, 'foo');                     // exactly 3 chars
    idx.add(70n, 'no-relevant-substrings');

    it('returns null for empty literal list', () => {
      expect(idx.candidates([])).toBeNull();
    });

    it('returns null when a literal is too short', () => {
      expect(idx.candidates(['ab'])).toBeNull();
      expect(idx.candidates(['memory', 'ab'])).toBeNull();
    });

    it('returns the empty array when a trigram has no postings', () => {
      // 'zzzzzz' — no document contains the trigram 'zzz'
      expect(idx.candidates(['zzzzzz'])).toEqual([]);
    });

    it('returns sorted candidate IDs for a literal that exists', () => {
      const c = idx.candidates(['memory'])!;
      expect(c).toEqual([10n, 40n]);   // both contain "memory"
    });

    it('matches case-insensitively', () => {
      // 'String Table' contains 'string' (lowercased)
      const c = idx.candidates(['STRING'])!;
      expect(c).toContain(30n);
    });

    it('alternation: union of per-literal candidates', () => {
      const c = idx.candidates(['memory', 'graph'])!;
      expect(c.sort()).toEqual([10n, 20n, 40n].sort());
    });

    it('exact 3-char literal works', () => {
      const c = idx.candidates(['foo'])!;
      expect(c).toEqual([60n]);
    });

    it('false-positive rate: candidates is a SUPERSET of true matches', () => {
      // Test the soundness contract: every string that ACTUALLY contains the
      // literal must be in the candidate set. (The reverse is not required —
      // candidates can contain extras, that's the role of the post-filter.)
      const literals = ['memory', 'lock', 'graph', 'concurrent'];
      const all: [bigint, string][] = [
        [10n, 'memory file'],
        [20n, 'graphfile'],
        [30n, 'String Table'],
        [40n, 'concurrent memory access'],
        [50n, 'lockholder'],
        [60n, 'foo'],
        [70n, 'no-relevant-substrings'],
      ];
      for (const lit of literals) {
        const trueMatches = new Set(
          all.filter(([, t]) => t.toLowerCase().includes(lit.toLowerCase())).map(([id]) => id),
        );
        const candidates = new Set(idx.candidates([lit]) ?? []);
        for (const m of trueMatches) {
          expect(candidates.has(m)).toBe(true);
        }
      }
    });
  });

  describe('add → remove → search invariants', () => {
    it('removed strings do not appear in candidates', () => {
      const idx = new TrigramIndex();
      idx.add(1n, 'foobar contains foo');
      idx.add(2n, 'another foo here');
      expect(idx.candidates(['foo'])).toEqual([1n, 2n]);
      idx.remove(1n);
      expect(idx.candidates(['foo'])).toEqual([2n]);
      idx.remove(2n);
      expect(idx.candidates(['foo'])).toEqual([]);
    });
  });
});
