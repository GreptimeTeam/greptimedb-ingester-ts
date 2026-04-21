// Lazy, cached loaders for the optional native compression codecs. Both libs are
// declared in `package.json` `optionalDependencies`; we only require() them the first
// time the user asks for compression, and raise a clear `ConfigError` if the package
// isn't installed (e.g. user ran `pnpm install --no-optional`).

import type * as Lz4Module from 'lz4-napi';
import type * as ZstdModule from '@mongodb-js/zstd';

import { ConfigError } from '../errors.js';

export type Compressor = (input: Uint8Array) => Promise<Uint8Array>;

let lz4Cache: Compressor | undefined;
let zstdCache: Compressor | undefined;

/** Load `lz4-napi` and return an async frame-compressor matching arrow-rs LZ4_FRAME. */
export async function loadLz4(): Promise<Compressor> {
  if (lz4Cache !== undefined) return lz4Cache;
  let mod: typeof Lz4Module;
  try {
    mod = await import('lz4-napi');
  } catch (err) {
    throw new ConfigError(
      'lz4-napi is not installed; install it to enable LZ4_FRAME bulk compression ' +
        '(pnpm add lz4-napi). It is declared in optionalDependencies, so a ' +
        'no-optional install or unsupported platform can leave it missing.',
      err,
    );
  }
  // `compressFrame` accepts Buffer | string. Coerce our Uint8Array so Node Buffer sees
  // the same memory without copying.
  lz4Cache = async (input: Uint8Array): Promise<Uint8Array> => {
    const buf = Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    return await mod.compressFrame(buf);
  };
  return lz4Cache;
}

/** Load `@mongodb-js/zstd` and return an async compressor at default level (3). */
export async function loadZstd(): Promise<Compressor> {
  if (zstdCache !== undefined) return zstdCache;
  let mod: typeof ZstdModule;
  try {
    mod = await import('@mongodb-js/zstd');
  } catch (err) {
    throw new ConfigError(
      '@mongodb-js/zstd is not installed; install it to enable ZSTD bulk compression ' +
        '(pnpm add @mongodb-js/zstd). It is declared in optionalDependencies, so a ' +
        'no-optional install or unsupported platform can leave it missing.',
      err,
    );
  }
  zstdCache = async (input: Uint8Array): Promise<Uint8Array> => {
    const buf = Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    return await mod.compress(buf);
  };
  return zstdCache;
}

/** Test-only: drop cached compressor references so re-import paths can be exercised. */
export function _resetCodecCacheForTest(): void {
  lz4Cache = undefined;
  zstdCache = undefined;
}
