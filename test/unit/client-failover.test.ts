import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the unary write so we drive attempt outcomes without a live server. The Client's
// channel pool still constructs (lazy) gRPC clients, but no RPC is ever issued.
vi.mock('../../src/write/unary.js', () => ({
  performUnaryWrite: vi.fn(),
}));

import { performUnaryWrite } from '../../src/write/unary.js';
import {
  Client,
  ConfigBuilder,
  DataType,
  GreptimeStatusCode,
  Precision,
  ServerError,
  Table,
  TimeoutError,
  TransportError,
  type EndpointSelector,
  type SelectContext,
} from '../../src/index.js';

const mockedWrite = vi.mocked(performUnaryWrite);

// Records every select() and its exclude snapshot, plus health-hook calls. Returns the first
// non-excluded endpoint (falling open to the first endpoint), so exclusion is observable.
class RecordingSelector implements EndpointSelector {
  public readonly selects: { peer: string; exclude: string[] }[] = [];
  public readonly successes: string[] = [];
  public readonly failures: string[] = [];

  public select(endpoints: readonly string[], ctx?: SelectContext): string {
    const exclude = ctx?.exclude ?? new Set<string>();
    const peer = endpoints.find((ep) => !exclude.has(ep)) ?? endpoints[0]!;
    this.selects.push({ peer, exclude: [...exclude] });
    return peer;
  }

  public reportSuccess(endpoint: string): void {
    this.successes.push(endpoint);
  }

  public reportFailure(endpoint: string): void {
    this.failures.push(endpoint);
  }
}

function clientWith(selector: EndpointSelector): Client {
  const cfg = ConfigBuilder.createWithEndpoints('h1:4001', 'h2:4001')
    .withDatabase('public')
    .withEndpointSelector(selector)
    .withRetry({ maxAttempts: 3, initialBackoffMs: 1, maxBackoffMs: 2 })
    .build();
  return new Client(cfg);
}

function sampleTable(): Table {
  return Table.new('failover_demo')
    .addTagColumn('host', DataType.String)
    .addFieldColumn('v', DataType.Int64)
    .addTimestampColumn('ts', Precision.Millisecond)
    .addRow(['h', 1n, 0]);
}

describe('Client unary failover', () => {
  beforeEach(() => {
    mockedWrite.mockReset();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('excludes already-failed peers on subsequent retry attempts', async () => {
    mockedWrite
      .mockRejectedValueOnce(new TransportError('down', 14))
      .mockRejectedValueOnce(new TransportError('down', 14))
      .mockResolvedValueOnce({ value: 7 });

    const selector = new RecordingSelector();
    const client = clientWith(selector);
    try {
      const res = await client.write(sampleTable());
      expect(res.value).toBe(7);
    } finally {
      await client.close();
    }

    expect(selector.selects).toHaveLength(3);
    expect(selector.selects[0]!.exclude).toEqual([]);
    expect(selector.selects[1]!.exclude).toContain('h1:4001');
    expect(selector.selects[2]!.exclude).toEqual(expect.arrayContaining(['h1:4001', 'h2:4001']));
  });

  it('reports endpoint failures to the selector but not server business errors', async () => {
    // First attempt: a transient transport failure → endpoint failure.
    // Second attempt: a retriable server status → endpoint is alive, must NOT be ejected.
    // Third attempt: success.
    mockedWrite
      .mockRejectedValueOnce(new TransportError('down', 14))
      .mockRejectedValueOnce(new ServerError('region busy', GreptimeStatusCode.RegionBusy))
      .mockResolvedValueOnce({ value: 1 });

    const selector = new RecordingSelector();
    const client = clientWith(selector);
    try {
      await client.write(sampleTable());
    } finally {
      await client.close();
    }

    // Only the transport failure ejects; the RegionBusy peer and the final success report success.
    expect(selector.failures).toEqual(['h1:4001']);
    expect(selector.successes).toEqual(expect.arrayContaining(['h2:4001']));
  });

  it('does not retry a non-retriable server error', async () => {
    mockedWrite.mockRejectedValueOnce(
      new ServerError('bad request', GreptimeStatusCode.InvalidArguments),
    );

    const selector = new RecordingSelector();
    const client = clientWith(selector);
    try {
      await expect(client.write(sampleTable())).rejects.toBeInstanceOf(ServerError);
    } finally {
      await client.close();
    }

    expect(selector.selects).toHaveLength(1);
    // A business error proves the endpoint is alive — reported as success, not failure.
    expect(selector.failures).toEqual([]);
    expect(selector.successes).toEqual(['h1:4001']);
  });

  it('emits no health signal for a client-side error (endpoint not involved)', async () => {
    // A local encode/validation failure says nothing about the endpoint — it must neither eject
    // the peer nor reset its failure streak.
    mockedWrite.mockRejectedValueOnce(new Error('encode failed'));

    const selector = new RecordingSelector();
    const client = clientWith(selector);
    try {
      await expect(client.write(sampleTable())).rejects.toThrow('encode failed');
    } finally {
      await client.close();
    }

    expect(selector.selects).toHaveLength(1);
    expect(selector.failures).toEqual([]);
    expect(selector.successes).toEqual([]);
  });

  it('does not retry or report endpoint health for client-side timeout', async () => {
    mockedWrite.mockRejectedValueOnce(new TimeoutError('deadline exceeded'));

    const selector = new RecordingSelector();
    const client = clientWith(selector);
    try {
      await expect(client.write(sampleTable())).rejects.toBeInstanceOf(TimeoutError);
    } finally {
      await client.close();
    }

    expect(mockedWrite).toHaveBeenCalledTimes(1);
    expect(selector.selects).toHaveLength(1);
    expect(selector.failures).toEqual([]);
    expect(selector.successes).toEqual([]);
  });
});
