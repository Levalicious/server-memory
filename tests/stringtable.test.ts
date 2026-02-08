import { StringTable } from '../src/stringtable.js';
import { unlinkSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

const TEST_PATH = join(tmpdir(), `strtab-test-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(TEST_PATH); } catch {}
}

describe('StringTable', () => {
  afterEach(cleanup);

  test('intern and get round-trip', () => {
    const st = new StringTable(TEST_PATH);
    const id = st.intern('hello');
    expect(id).not.toBe(0n);
    expect(st.get(id)).toBe('hello');
    st.close();
  });

  test('intern deduplicates', () => {
    const st = new StringTable(TEST_PATH);
    const id1 = st.intern('duplicate');
    const id2 = st.intern('duplicate');
    expect(id1).toBe(id2);
    expect(st.refcount(id1)).toBe(2);
    expect(st.count).toBe(1);
    st.close();
  });

  test('distinct strings get distinct IDs', () => {
    const st = new StringTable(TEST_PATH);
    const id1 = st.intern('alpha');
    const id2 = st.intern('beta');
    const id3 = st.intern('gamma');
    expect(id1).not.toBe(id2);
    expect(id2).not.toBe(id3);
    expect(st.get(id1)).toBe('alpha');
    expect(st.get(id2)).toBe('beta');
    expect(st.get(id3)).toBe('gamma');
    expect(st.count).toBe(3);
    st.close();
  });

  test('release decrements refcount', () => {
    const st = new StringTable(TEST_PATH);
    const id = st.intern('reftest');
    st.intern('reftest');  // refcount = 2
    expect(st.refcount(id)).toBe(2);

    st.release(id);
    expect(st.refcount(id)).toBe(1);
    expect(st.count).toBe(1);

    st.release(id);
    // refcount hit 0 — entry freed
    expect(st.count).toBe(0);
    st.close();
  });

  test('released string can be re-interned', () => {
    const st = new StringTable(TEST_PATH);
    const id1 = st.intern('ephemeral');
    st.release(id1);
    expect(st.count).toBe(0);

    // Re-intern the same string — should get a new allocation
    const id2 = st.intern('ephemeral');
    expect(st.get(id2)).toBe('ephemeral');
    expect(st.refcount(id2)).toBe(1);
    expect(st.count).toBe(1);
    st.close();
  });

  test('addRef bumps refcount without re-interning', () => {
    const st = new StringTable(TEST_PATH);
    const id = st.intern('shared');
    expect(st.refcount(id)).toBe(1);

    st.addRef(id);
    expect(st.refcount(id)).toBe(2);

    st.release(id);
    expect(st.refcount(id)).toBe(1);
    st.close();
  });

  test('handles UTF-8 multi-byte strings', () => {
    const st = new StringTable(TEST_PATH);
    const str = '日本語テスト 🚀';
    const id = st.intern(str);
    expect(st.get(id)).toBe(str);
    st.close();
  });

  test('handles empty string', () => {
    const st = new StringTable(TEST_PATH);
    const id = st.intern('');
    expect(st.get(id)).toBe('');
    expect(st.refcount(id)).toBe(1);
    st.close();
  });

  test('many strings trigger rehash', () => {
    const st = new StringTable(TEST_PATH);
    const ids: bigint[] = [];

    // Insert enough strings to trigger at least one rehash
    // Initial bucket count is 4096, load factor 0.7 → rehash at ~2867
    for (let i = 0; i < 3000; i++) {
      ids.push(st.intern(`string_${i}`));
    }

    expect(st.count).toBe(3000);

    // Verify all are retrievable
    for (let i = 0; i < 3000; i++) {
      expect(st.get(ids[i])).toBe(`string_${i}`);
    }

    st.close();
  });

  test('persists across close and reopen', () => {
    const st = new StringTable(TEST_PATH);
    const id = st.intern('persistent');
    st.close();

    const st2 = new StringTable(TEST_PATH);
    expect(st2.get(id)).toBe('persistent');
    expect(st2.refcount(id)).toBe(1);

    // Interning again should find existing entry
    const id2 = st2.intern('persistent');
    expect(id2).toBe(id);
    expect(st2.refcount(id)).toBe(2);
    st2.close();
  });

  test('hash collisions resolved correctly', () => {
    const st = new StringTable(TEST_PATH);
    // Insert many strings — statistical certainty of collisions with 4096 buckets
    const strs = Array.from({ length: 100 }, (_, i) => `collision_test_${i}`);
    const ids = strs.map(s => st.intern(s));

    // All should be retrievable
    for (let i = 0; i < strs.length; i++) {
      expect(st.get(ids[i])).toBe(strs[i]);
    }

    // All should be distinct
    expect(new Set(ids.map(String)).size).toBe(100);
    st.close();
  });

  test('removal fixup maintains probe chain integrity', () => {
    const st = new StringTable(TEST_PATH);

    // Insert three strings, then remove the first, verify the others survive
    const id1 = st.intern('first');
    const id2 = st.intern('second');
    const id3 = st.intern('third');

    st.release(id1);
    expect(st.count).toBe(2);

    // second and third should still be findable
    const id2b = st.intern('second');
    const id3b = st.intern('third');
    expect(id2b).toBe(id2);
    expect(id3b).toBe(id3);

    st.close();
  });
});
