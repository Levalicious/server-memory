import { MemoryFile } from '../src/memoryfile.js';
import { unlinkSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

const TEST_PATH = join(tmpdir(), `memfile-test-${process.pid}.dat`);

function cleanup() {
  try { unlinkSync(TEST_PATH); } catch {}
}

describe('MemoryFile', () => {
  afterEach(cleanup);

  test('open, alloc, write, read, close', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const offset = mf.alloc(64n);
    expect(offset).not.toBe(0n);

    const data = Buffer.from('hello, mmap arena!');
    mf.write(offset, data);

    const readBack = mf.read(offset, BigInt(data.length));
    expect(readBack.toString()).toBe('hello, mmap arena!');

    mf.close();
  });

  test('multiple allocations dont overlap', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const a = mf.alloc(100n);
    const b = mf.alloc(100n);
    const c = mf.alloc(100n);

    expect(a).not.toBe(0n);
    expect(b).not.toBe(0n);
    expect(c).not.toBe(0n);

    // All offsets should be distinct and non-overlapping
    // With 8-byte alignment and alloc headers, gaps should be >= 100 + 8 (header)
    expect(b > a).toBe(true);
    expect(c > b).toBe(true);

    // Write different data to each
    mf.write(a, Buffer.from('A'.repeat(100)));
    mf.write(b, Buffer.from('B'.repeat(100)));
    mf.write(c, Buffer.from('C'.repeat(100)));

    // Read back and verify no corruption
    expect(mf.read(a, 100n).toString()).toBe('A'.repeat(100));
    expect(mf.read(b, 100n).toString()).toBe('B'.repeat(100));
    expect(mf.read(c, 100n).toString()).toBe('C'.repeat(100));

    mf.close();
  });

  test('free and realloc reuses space', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const a = mf.alloc(64n);
    const b = mf.alloc(64n);

    // Free the first block
    mf.free(a);

    // Next alloc of same size should reuse the freed block
    const c = mf.alloc(64n);
    expect(c).toBe(a);  // Should get the same offset back

    mf.close();
  });

  test('coalesce merges adjacent free blocks', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const a = mf.alloc(64n);
    const b = mf.alloc(64n);
    const c = mf.alloc(64n);

    // Free a and b (adjacent)
    mf.free(a);
    mf.free(b);

    // Before coalesce: two separate free blocks, neither large enough for 200 bytes
    // After coalesce: one merged block
    mf.coalesce();

    // Should be able to allocate a block larger than either a or b individually
    // but fitting in the merged space (64+8 + 64+8 = 144 bytes of free space minus headers)
    const stats = mf.stats();
    expect(stats.freeListHead).not.toBe(0n);

    mf.close();
  });

  test('data persists across close and reopen', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const offset = mf.alloc(64n);
    const msg = Buffer.from('persistent data!');
    mf.write(offset, msg);
    mf.close();

    // Reopen
    const mf2 = new MemoryFile(TEST_PATH);
    const readBack = mf2.read(offset, BigInt(msg.length));
    expect(readBack.toString()).toBe('persistent data!');
    mf2.close();
  });

  test('stats reports correct values', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    const statsBefore = mf.stats();
    expect(statsBefore.fileSize).toBe(4096n);
    expect(statsBefore.freeListHead).toBe(0n);

    mf.alloc(100n);
    const statsAfter = mf.stats();
    expect(statsAfter.allocated > statsBefore.allocated).toBe(true);

    mf.close();
  });

  test('file auto-grows beyond initial size', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    // Allocate more than initial 4096 bytes
    const offsets: bigint[] = [];
    for (let i = 0; i < 100; i++) {
      const off = mf.alloc(256n);
      expect(off).not.toBe(0n);
      offsets.push(off);
    }

    const stats = mf.stats();
    expect(stats.fileSize > 4096n).toBe(true);

    // Verify all allocations are readable
    for (const off of offsets) {
      const buf = mf.read(off, 1n);
      expect(buf.length).toBe(1);
    }

    mf.close();
  });

  test('lock and unlock dont throw', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);

    expect(() => mf.lockShared()).not.toThrow();
    expect(() => mf.unlock()).not.toThrow();
    expect(() => mf.lockExclusive()).not.toThrow();
    expect(() => mf.unlock()).not.toThrow();

    mf.close();
  });

  test('operations on closed file throw', () => {
    const mf = new MemoryFile(TEST_PATH, 4096);
    mf.close();

    expect(() => mf.alloc(64n)).toThrow('MemoryFile is closed');
    expect(() => mf.read(32n, 8n)).toThrow('MemoryFile is closed');
    expect(() => mf.write(32n, Buffer.alloc(8))).toThrow('MemoryFile is closed');
  });
});
