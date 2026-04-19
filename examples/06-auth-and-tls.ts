/*
 * Example 06 — basic auth + TLS configuration.
 *
 * Shows the two auth paths (unary/streaming via proto RequestHeader, bulk via gRPC
 * metadata) being configured from the same `withBasicAuth(...)`. TLS can be provided via
 * system trust store, PEM strings, or file paths.
 *
 * Run: pnpm example 06-auth-and-tls
 *   (requires a GreptimeDB configured with auth, otherwise the default localhost:4001
 *    container will accept any credentials.)
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const username = process.env.GREPTIMEDB_USER ?? 'admin';
  const password = process.env.GREPTIMEDB_PASSWORD ?? 'admin';

  const builder = Client.create(endpoint)
    .withDatabase('public')
    .withBasicAuth(username, password);

  // Enable TLS via env flag. Any of the three modes works.
  if (process.env.GREPTIMEDB_TLS === 'system') {
    builder.withTls({ kind: 'system' });
  } else if (process.env.GREPTIMEDB_TLS_CERT_PATH !== undefined) {
    builder.withTls({
      kind: 'file',
      caPath: process.env.GREPTIMEDB_TLS_CA_PATH,
      certPath: process.env.GREPTIMEDB_TLS_CERT_PATH,
      keyPath: process.env.GREPTIMEDB_TLS_KEY_PATH,
    });
  }

  const client = new Client(builder.build());

  try {
    const table = Table.new('auth_demo')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('n', DataType.Int64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['auth-host-1', 42n, Date.now()]);

    const res = await client.write(table);
    console.log(`authenticated insert: ${res.value} rows`);
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('auth/tls example failed:', err);
  process.exit(1);
});
