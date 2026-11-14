import { Client, DataType, IngesterError, Precision, Table } from '@greptime/ingester/web';

const form = document.querySelector<HTMLFormElement>('form')!;
const button = document.querySelector<HTMLButtonElement>('button')!;
const output = document.querySelector<HTMLPreElement>('pre')!;

async function insert(): Promise<void> {
  button.disabled = true;
  output.textContent = 'Writing…';
  let client: Client | undefined;
  try {
    const username = form.querySelector<HTMLInputElement>('[name="username"]')!.value;
    client = new Client({
      endpoint: form.querySelector<HTMLInputElement>('[name="endpoint"]')!.value,
      database: form.querySelector<HTMLInputElement>('[name="database"]')!.value,
      timeoutMs: 10_000,
      ...(username && {
        auth: {
          kind: 'basic',
          username,
          password: form.querySelector<HTMLInputElement>('[name="password"]')!.value,
        },
      }),
    });
    const table = Table.new('browser_metrics_demo')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('usage', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['browser-01', 75.3, Date.now()])
      .addRow(['browser-02', 82.1, Date.now()]);

    const { value } = await client.write(table);
    output.textContent = `Inserted ${value} rows into browser_metrics_demo.`;
  } catch (err) {
    output.textContent = err instanceof IngesterError ? `${err.kind}: ${err.message}` : String(err);
  } finally {
    await client?.close();
    button.disabled = false;
  }
}

form.addEventListener('submit', (event) => {
  event.preventDefault();
  void insert();
});
