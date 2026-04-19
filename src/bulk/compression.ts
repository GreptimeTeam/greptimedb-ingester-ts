/** Body compression for Arrow IPC record batches within Flight DoPut. */
export const BulkCompression = {
  None: 'none',
  Lz4: 'lz4',
  Zstd: 'zstd',
} as const;

export type BulkCompression = (typeof BulkCompression)[keyof typeof BulkCompression];
