import { Client, Table, Precision, type RetryPolicy } from '@greptime/ingester/web';

const retry: Partial<RetryPolicy> = { maxAttempts: 1 };
const client = new Client({ endpoint: 'https://example.com', retry });
void client.write(Table.new('metrics').addTimestampColumn('ts', Precision.Millisecond).addRow([0]));
