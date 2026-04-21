// Bench router: `pnpm bench <name> [--args]`

import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';

const here = path.dirname(fileURLToPath(import.meta.url));
const arg = process.argv[2];
const forward = process.argv.slice(3);

if (arg === undefined) {
  console.error(
    'Usage: pnpm bench <name> [--rows=N --batch-size=N --parallelism=N --endpoint=host:port]',
  );
  console.error('Available: regular-api, stream-api, bulk-api, cpu-bulk-api');
  process.exit(2);
}

const file = path.join(here, `${arg}.ts`);
if (!existsSync(file)) {
  console.error(`bench not found: ${file}`);
  process.exit(2);
}

const tsx = path.resolve(here, '..', 'node_modules', '.bin', 'tsx');
const result = spawnSync(tsx, [file, ...forward], { stdio: 'inherit' });
process.exit(result.status ?? 1);
