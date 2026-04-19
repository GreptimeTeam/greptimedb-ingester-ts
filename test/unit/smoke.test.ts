import { describe, expect, it } from 'vitest';
import { VERSION } from '../../src/index.js';

describe('smoke', () => {
  it('exposes a semver-ish version string', () => {
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+/);
  });
});
