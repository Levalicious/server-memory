/**
 * errors.ts — neutral storage-layer error facts.
 *
 * Deliberately NOT tool-policy vocabulary: policy codes (LIMIT_EXCEEDED,
 * ENTITY_NOT_FOUND, …) are selected at the surface (server.ts), keeping the
 * failure->code mapping in exactly one layer (docs/api-error-policy.md R1/R3).
 * Adapters throw these facts; the manager converts them into ToolErrors.
 */

export class StoreRecordError extends Error {
  constructor(
    public field: 'name' | 'type' | 'record',
    public bytes: number,
  ) {
    super(`${field} of ${bytes} bytes exceeds the store's record capacity`);
    this.name = 'StoreRecordError';
  }
}
