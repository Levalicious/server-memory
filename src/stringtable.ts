/**
 * StringTable - Interned, refcounted string storage backed by a MemoryFile.
 *
 * Layout of each string entry (allocated via memfile_alloc):
 *   [u32 refcount] [u32 hash] [u16 len] [u8 data[len]]
 *
 * String ID = the memfile offset to the entry (the offset returned by alloc,
 * pointing past the memfile_alloc_t header, directly at refcount).
 *
 * The hash index is a separate allocated block:
 *   [u32 bucket_count] [u32 _pad] [u64 offsets[bucket_count]]
 *   Each bucket is the offset of the first entry in that bucket (0 = empty).
 *   Collisions are resolved by linear probing.
 *
 * File header (after memfile header):
 *   offset 32: [u64 hash_index_offset]
 *   offset 40: [u32 entry_count]     total live strings
 *   offset 44: [u32 _pad]
 */

import { MemoryFile } from './memoryfile.js';

// We store our own header in an allocated block so it doesn't collide with
// the memfile header region. The offset of this header block is stored at
// a fixed position right after the memfile header.
//
// Our header block layout:
//   [u64 hash_index_offset] [u32 entry_count] [u32 _pad]
//
// The pointer TO our header block is at memfile offset 32 (right after the
// 32-byte memfile header). We write it there during init.
//
// BUT: offsets 32+ belong to the allocator. So we allocate a 16-byte block
// for our header, and store its offset as the very first allocation.

// Position in file where we store the offset to our header block.
// This is a "well-known" location. We use the first allocation's offset.
const OUR_HEADER_SIZE = 16; // u64 hash_index_offset + u32 entry_count + u32 pad

// Offsets within our header block
const HDR_HASH_INDEX_OFFSET = 0;   // u64
const HDR_ENTRY_COUNT = 8;          // u32

// String entry field offsets (relative to entry start)
const ENT_REFCOUNT = 0;   // u32
const ENT_HASH = 4;       // u32
const ENT_LEN = 8;        // u16
const ENT_DATA = 10;      // u8[len]
const ENT_HEADER_SIZE = 10;

// Hash index field offsets (relative to index block start)
const IDX_BUCKET_COUNT = 0;  // u32
const IDX_BUCKETS = 8;       // u64[bucket_count]

const INITIAL_BUCKETS = 4096;
const LOAD_FACTOR_THRESHOLD = 0.7;

// FNV-1a 32-bit hash
function fnv1a(data: Buffer): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < data.length; i++) {
    hash ^= data[i];
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;  // unsigned
}

export class StringTable {
  private mf: MemoryFile;
  private headerOffset: bigint;  // offset to our header block

  constructor(path: string, initialSize: number = 65536) {
    this.mf = new MemoryFile(path, initialSize);

    const stats = this.mf.stats();
    // A fresh memfile has allocated = 32 (just the memfile header).
    // If anything has been allocated, the file was previously initialized.
    if (stats.allocated <= 32n) {
      this.headerOffset = this.initHeader();
    } else {
      // The first allocation in the file is always our header block.
      // It's at the first allocatable position after the memfile header.
      // memfile_alloc returns the offset past the alloc_t header (8 bytes),
      // so the first allocation is at offset 32 (memfile header) + 8 (alloc_t) = 40.
      this.headerOffset = 40n;
    }
  }

  private initHeader(): bigint {
    // Allocate our header block (first alloc in the file)
    const hdrOffset = this.mf.alloc(BigInt(OUR_HEADER_SIZE));
    if (hdrOffset === 0n) throw new Error('StringTable: failed to allocate header');

    // Allocate hash index block
    const bucketBytes = INITIAL_BUCKETS * 8;
    const indexSize = 4 + 4 + bucketBytes;  // bucket_count + pad + buckets
    const indexOffset = this.mf.alloc(BigInt(indexSize));
    if (indexOffset === 0n) throw new Error('StringTable: failed to allocate hash index');

    // Write bucket_count to index block
    const idxHeader = Buffer.alloc(8);
    idxHeader.writeUInt32LE(INITIAL_BUCKETS, 0);
    idxHeader.writeUInt32LE(0, 4);  // pad
    this.mf.write(indexOffset, idxHeader);

    // Zero all buckets
    const zeroBuckets = Buffer.alloc(bucketBytes);
    this.mf.write(indexOffset + 8n, zeroBuckets);

    // Write our header: hash_index_offset and entry_count
    const hdr = Buffer.alloc(OUR_HEADER_SIZE);
    hdr.writeBigUInt64LE(indexOffset, HDR_HASH_INDEX_OFFSET);
    hdr.writeUInt32LE(0, HDR_ENTRY_COUNT);   // entry_count = 0
    hdr.writeUInt32LE(0, 12);                // pad
    this.mf.write(hdrOffset, hdr);

    return hdrOffset;
  }

  // --- Header access ---

  private getHashIndexOffset(): bigint {
    const buf = this.mf.read(this.headerOffset + BigInt(HDR_HASH_INDEX_OFFSET), 8n);
    return buf.readBigUInt64LE(0);
  }

  private getEntryCount(): number {
    const buf = this.mf.read(this.headerOffset + BigInt(HDR_ENTRY_COUNT), 4n);
    return buf.readUInt32LE(0);
  }

  private setEntryCount(count: number): void {
    const buf = Buffer.alloc(4);
    buf.writeUInt32LE(count, 0);
    this.mf.write(this.headerOffset + BigInt(HDR_ENTRY_COUNT), buf);
  }

  // --- Hash index access ---

  private getBucketCount(): number {
    const indexOffset = this.getHashIndexOffset();
    const buf = this.mf.read(indexOffset, 4n);
    return buf.readUInt32LE(0);
  }

  private getBucket(index: number): bigint {
    const indexOffset = this.getHashIndexOffset();
    const pos = indexOffset + BigInt(IDX_BUCKETS + index * 8);
    const buf = this.mf.read(pos, 8n);
    return buf.readBigUInt64LE(0);
  }

  private setBucket(index: number, offset: bigint): void {
    const indexOffset = this.getHashIndexOffset();
    const pos = indexOffset + BigInt(IDX_BUCKETS + index * 8);
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64LE(offset, 0);
    this.mf.write(pos, buf);
  }

  // --- String entry access ---

  private readEntry(offset: bigint): { refcount: number; hash: number; len: number; data: Buffer } {
    const header = this.mf.read(offset, BigInt(ENT_HEADER_SIZE));
    const refcount = header.readUInt32LE(ENT_REFCOUNT);
    const hash = header.readUInt32LE(ENT_HASH);
    const len = header.readUInt16LE(ENT_LEN);
    const data = len > 0 ? this.mf.read(offset + BigInt(ENT_DATA), BigInt(len)) : Buffer.alloc(0);
    return { refcount, hash, len, data };
  }

  private writeRefcount(offset: bigint, refcount: number): void {
    const buf = Buffer.alloc(4);
    buf.writeUInt32LE(refcount, 0);
    this.mf.write(offset, buf);
  }

  // --- Public API ---

  /**
   * Intern a string. Returns its ID (offset).
   * If the string already exists, bumps refcount and returns existing ID.
   * If new, allocates an entry and inserts into the hash index.
   */
  intern(str: string): bigint {
    const data = Buffer.from(str, 'utf-8');
    const hash = fnv1a(data);
    const bucketCount = this.getBucketCount();
    let bucket = hash % bucketCount;

    // Linear probe to find existing or empty slot
    for (let i = 0; i < bucketCount; i++) {
      const slotIdx = (bucket + i) % bucketCount;
      const entryOffset = this.getBucket(slotIdx);

      if (entryOffset === 0n) {
        // Empty slot — string not found, allocate new entry
        const entrySize = ENT_HEADER_SIZE + data.length;
        const newOffset = this.mf.alloc(BigInt(entrySize));
        if (newOffset === 0n) throw new Error('StringTable: alloc failed');

        // Write entry: refcount=1, hash, len, data
        const entryBuf = Buffer.alloc(ENT_HEADER_SIZE);
        entryBuf.writeUInt32LE(1, ENT_REFCOUNT);
        entryBuf.writeUInt32LE(hash, ENT_HASH);
        entryBuf.writeUInt16LE(data.length, ENT_LEN);
        this.mf.write(newOffset, entryBuf);
        if (data.length > 0) {
          this.mf.write(newOffset + BigInt(ENT_DATA), data);
        }

        // Insert into hash index
        this.setBucket(slotIdx, newOffset);
        const count = this.getEntryCount() + 1;
        this.setEntryCount(count);

        // Check load factor and rehash if needed
        if (count > bucketCount * LOAD_FACTOR_THRESHOLD) {
          this.rehash(bucketCount * 2);
        }

        return newOffset;
      }

      // Slot occupied — check if it matches
      const entry = this.readEntry(entryOffset);
      if (entry.hash === hash && entry.len === data.length && entry.data.equals(data)) {
        // Found — bump refcount
        this.writeRefcount(entryOffset, entry.refcount + 1);
        return entryOffset;
      }
      // Collision — continue probing
    }

    throw new Error('StringTable: hash index full (should not happen with rehashing)');
  }

  /**
   * Get the string for an ID. Returns the UTF-8 string.
   */
  get(id: bigint): string {
    const entry = this.readEntry(id);
    return entry.data.toString('utf-8');
  }

  /**
   * Look up a string without interning or bumping refcount.
   * Returns the ID (offset) if found, or null if not present.
   */
  find(str: string): bigint | null {
    const data = Buffer.from(str, 'utf-8');
    const hash = fnv1a(data);
    const bucketCount = this.getBucketCount();
    let bucket = hash % bucketCount;

    for (let i = 0; i < bucketCount; i++) {
      const slotIdx = (bucket + i) % bucketCount;
      const entryOffset = this.getBucket(slotIdx);

      if (entryOffset === 0n) return null; // Empty slot — not found

      const entry = this.readEntry(entryOffset);
      if (entry.hash === hash && entry.len === data.length && entry.data.equals(data)) {
        return entryOffset;
      }
    }

    return null;
  }

  /**
   * Decrement refcount. If it reaches 0, free the entry and remove from hash index.
   */
  release(id: bigint): void {
    const entry = this.readEntry(id);
    if (entry.refcount <= 1) {
      // Remove from hash index
      this.removeFromIndex(id, entry.hash);
      // Free the allocation
      this.mf.free(id);
      this.setEntryCount(this.getEntryCount() - 1);
    } else {
      this.writeRefcount(id, entry.refcount - 1);
    }
  }

  /**
   * Bump refcount without interning (for when you already have the ID).
   */
  addRef(id: bigint): void {
    const entry = this.readEntry(id);
    this.writeRefcount(id, entry.refcount + 1);
  }

  /**
   * Get current refcount for an entry.
   */
  refcount(id: bigint): number {
    const buf = this.mf.read(id, 4n);
    return buf.readUInt32LE(0);
  }

  /**
   * Number of live strings in the table.
   */
  get count(): number {
    return this.getEntryCount();
  }

  /**
   * Iterate over all live strings in the table.
   * Yields { id, text, refcount } for each entry.
   */
  *entries(): Generator<{ id: bigint; text: string; refcount: number }> {
    const bucketCount = this.getBucketCount();
    for (let i = 0; i < bucketCount; i++) {
      const entryOffset = this.getBucket(i);
      if (entryOffset === 0n) continue;
      const entry = this.readEntry(entryOffset);
      yield {
        id: entryOffset,
        text: entry.data.toString('utf-8'),
        refcount: entry.refcount,
      };
    }
  }

  // --- Hash index management ---

  private removeFromIndex(offset: bigint, hash: number): void {
    const bucketCount = this.getBucketCount();
    let bucket = hash % bucketCount;

    // Find the entry in the index
    for (let i = 0; i < bucketCount; i++) {
      const slotIdx = (bucket + i) % bucketCount;
      const entryOffset = this.getBucket(slotIdx);

      if (entryOffset === 0n) return;  // Not found (shouldn't happen)
      if (entryOffset === offset) {
        // Found — remove and fix up the linear probe chain
        this.setBucket(slotIdx, 0n);
        this.fixupAfterRemoval(slotIdx, bucketCount);
        return;
      }
    }
  }

  /**
   * After removing an entry at slotIdx, re-insert any entries that were
   * displaced past the removed slot by linear probing.
   */
  private fixupAfterRemoval(removedSlot: number, bucketCount: number): void {
    let slot = (removedSlot + 1) % bucketCount;
    while (true) {
      const entryOffset = this.getBucket(slot);
      if (entryOffset === 0n) break;  // End of cluster

      // Read this entry's natural bucket
      const entry = this.readEntry(entryOffset);
      const naturalBucket = entry.hash % bucketCount;

      // Check if this entry needs to move (it was displaced past removedSlot)
      if (this.needsRelocation(naturalBucket, removedSlot, slot, bucketCount)) {
        this.setBucket(removedSlot, entryOffset);
        this.setBucket(slot, 0n);
        // Continue fixup from the newly emptied slot
        removedSlot = slot;
      }

      slot = (slot + 1) % bucketCount;
    }
  }

  private needsRelocation(natural: number, empty: number, current: number, size: number): boolean {
    // Is 'empty' between 'natural' and 'current' in the circular probe sequence?
    if (natural <= current) {
      return natural <= empty && empty < current;
    } else {
      // Wraps around
      return natural <= empty || empty < current;
    }
  }

  private rehash(newBucketCount: number): void {
    const oldIndexOffset = this.getHashIndexOffset();
    const oldBucketCount = this.getBucketCount();

    // Allocate new index
    const newIndexSize = 4 + 4 + newBucketCount * 8;
    const newIndexOffset = this.mf.alloc(BigInt(newIndexSize));
    if (newIndexOffset === 0n) throw new Error('StringTable: rehash alloc failed');

    // Write new bucket count
    const header = Buffer.alloc(8);
    header.writeUInt32LE(newBucketCount, 0);
    this.mf.write(newIndexOffset, header);

    // Zero new buckets
    const zeroBuckets = Buffer.alloc(newBucketCount * 8);
    this.mf.write(newIndexOffset + 8n, zeroBuckets);

    // Update header to point to new index
    const hdr = Buffer.alloc(8);
    hdr.writeBigUInt64LE(newIndexOffset, 0);
    this.mf.write(this.headerOffset + BigInt(HDR_HASH_INDEX_OFFSET), hdr);

    // Re-insert all entries from old index
    for (let i = 0; i < oldBucketCount; i++) {
      const pos = oldIndexOffset + BigInt(IDX_BUCKETS + i * 8);
      const buf = this.mf.read(pos, 8n);
      const entryOffset = buf.readBigUInt64LE(0);
      if (entryOffset === 0n) continue;

      // Read hash and insert into new index
      const entry = this.readEntry(entryOffset);
      let bucket = entry.hash % newBucketCount;
      for (let j = 0; j < newBucketCount; j++) {
        const slotIdx = (bucket + j) % newBucketCount;
        const slotPos = newIndexOffset + BigInt(IDX_BUCKETS + slotIdx * 8);
        const slotBuf = this.mf.read(slotPos, 8n);
        if (slotBuf.readBigUInt64LE(0) === 0n) {
          const writeBuf = Buffer.alloc(8);
          writeBuf.writeBigUInt64LE(entryOffset, 0);
          this.mf.write(slotPos, writeBuf);
          break;
        }
      }
    }

    // Free old index block
    this.mf.free(oldIndexOffset);
  }

  // --- Lifecycle ---

  sync(): void {
    this.mf.sync();
  }

  /** Refresh the mmap if the file was grown by another process. */
  refresh(): void {
    this.mf.refresh();
  }

  close(): void {
    this.mf.close();
  }
}
