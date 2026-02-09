/**
 * MemoryFile - TypeScript wrapper around the native mmap arena allocator.
 *
 * All offsets are BigInt (u64 on the C side).
 * Buffers passed to/from the native layer are Node Buffers.
 */

import { createRequire } from 'module';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

interface NativeMemoryFile {
  open(path: string, initialSize: number): unknown;
  close(handle: unknown): void;
  sync(handle: unknown): void;
  alloc(handle: unknown, size: bigint): bigint;
  free(handle: unknown, offset: bigint): void;
  coalesce(handle: unknown): void;
  read(handle: unknown, offset: bigint, length: bigint): Buffer;
  write(handle: unknown, offset: bigint, data: Buffer): void;
  lockShared(handle: unknown): void;
  lockExclusive(handle: unknown): void;
  unlock(handle: unknown): void;
  stats(handle: unknown): { fileSize: bigint; allocated: bigint; freeListHead: bigint };
}

// The .node binary is in build/Release/ relative to project root
const native: NativeMemoryFile = require(join(__dirname, '..', 'build', 'Release', 'memoryfile.node'));

export class MemoryFile {
  private handle: unknown;
  private closed = false;

  constructor(path: string, initialSize: number = 4096) {
    this.handle = native.open(path, initialSize);
  }

  /**
   * Allocate a block of the given size.
   * Returns the offset to the usable region (after the alloc header).
   * Returns 0n on failure.
   */
  alloc(size: bigint): bigint {
    this.assertOpen();
    return native.alloc(this.handle, size);
  }

  /**
   * Free a previously allocated block by its offset.
   * The block goes onto the free list for reuse.
   */
  free(offset: bigint): void {
    this.assertOpen();
    native.free(this.handle, offset);
  }

  /**
   * Merge adjacent free blocks to reduce fragmentation.
   */
  coalesce(): void {
    this.assertOpen();
    native.coalesce(this.handle);
  }

  /**
   * Read `length` bytes starting at `offset`.
   * Returns a Buffer with the data.
   */
  read(offset: bigint, length: bigint): Buffer {
    this.assertOpen();
    return native.read(this.handle, offset, length);
  }

  /**
   * Write a Buffer at the given offset.
   */
  write(offset: bigint, data: Buffer): void {
    this.assertOpen();
    native.write(this.handle, offset, data);
  }

  /**
   * Acquire a shared (read) lock on the file.
   * Blocks until the lock is acquired.
   */
  lockShared(): void {
    this.assertOpen();
    native.lockShared(this.handle);
  }

  /**
   * Acquire an exclusive (write) lock on the file.
   * Blocks until the lock is acquired.
   */
  lockExclusive(): void {
    this.assertOpen();
    native.lockExclusive(this.handle);
  }

  /**
   * Release the lock on the file.
   */
  unlock(): void {
    this.assertOpen();
    native.unlock(this.handle);
  }

  /**
   * Flush all changes to disk.
   */
  sync(): void {
    this.assertOpen();
    native.sync(this.handle);
  }

  /**
   * Get arena statistics.
   */
  stats(): { fileSize: bigint; allocated: bigint; freeListHead: bigint } {
    this.assertOpen();
    return native.stats(this.handle);
  }

  /**
   * Close the memory file. Syncs and unmaps.
   * The instance is unusable after this.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    native.close(this.handle);
    this.handle = null;
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error('MemoryFile is closed');
    }
  }
}
