// Diagnostic: how fast can the TS SDK encode bulk batches (Arrow + IPC), with no
// network at all? This isolates client CPU cost from server/network.

import { performance } from 'node:perf_hooks';
import { precomputeArrowSchema, rowsToArrowTable } from '../src/bulk/arrow-encoder.js';
import { encodeTableForFlight } from '../src/bulk/flight-codec.js';
import { buildSchemaTable, generateBatch } from './log-data-provider.js';

async function main(): Promise<void> {
  const totalRows = 2_000_000;
  const batchSize = 5_000;
  const numBatches = Math.floor(totalRows / batchSize);

  const schema = buildSchemaTable().schema();
  const precomp = precomputeArrowSchema(schema);

  // Pre-gen all rows outside the timer.
  const batches: unknown[][][] = new Array(numBatches);
  for (let i = 0; i < numBatches; i++) batches[i] = generateBatch(batchSize, i * batchSize);

  let arrowNs = 0n;
  let ipcNs = 0n;
  let totalBytes = 0;

  // Warmup JIT.
  for (let i = 0; i < 3; i++) {
    const t = rowsToArrowTable(schema, batches[i]!, precomp);
    await encodeTableForFlight(t);
  }

  const start = performance.now();
  for (let i = 0; i < numBatches; i++) {
    const t0 = process.hrtime.bigint();
    const table = rowsToArrowTable(schema, batches[i]!, precomp);
    const t1 = process.hrtime.bigint();
    const { batchMessages } = await encodeTableForFlight(table);
    const t2 = process.hrtime.bigint();
    arrowNs += t1 - t0;
    ipcNs += t2 - t1;
    for (const m of batchMessages) totalBytes += m.metadata.byteLength + m.body.byteLength;
  }
  const elapsed = performance.now() - start;

  const rowsPerSec = (numBatches * batchSize) / (elapsed / 1000);
  const mb = totalBytes / 1024 / 1024;
  console.log(
    `[encode-only] rows=${numBatches * batchSize} elapsed=${elapsed.toFixed(0)}ms ` +
      `rows/s=${rowsPerSec.toFixed(0)} ` +
      `arrow=${(Number(arrowNs) / 1e6).toFixed(0)}ms ` +
      `ipc=${(Number(ipcNs) / 1e6).toFixed(0)}ms ` +
      `bytes=${mb.toFixed(1)}MB`,
  );
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
