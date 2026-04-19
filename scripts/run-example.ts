import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const examplesDir = path.resolve(__dirname, '..', 'examples');

const arg = process.argv[2];
if (!arg) {
  console.error('Usage: pnpm example <name>');
  console.error('Available:');
  // Lazily list examples on demand — kept intentionally minimal.
  process.exit(2);
}

const candidate = arg.endsWith('.ts') ? arg : `${arg}.ts`;
const filePath = path.join(examplesDir, candidate);
if (!existsSync(filePath)) {
  console.error(`example not found: ${filePath}`);
  process.exit(2);
}

const tsx = path.resolve(__dirname, '..', 'node_modules', '.bin', 'tsx');
const result = spawnSync(tsx, [filePath], { stdio: 'inherit' });
process.exit(result.status ?? 1);
