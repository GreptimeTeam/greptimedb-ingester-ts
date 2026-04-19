import { defineConfig } from 'vitest/config';

const runIntegration = process.env.INTEGRATION === '1';

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: [
      'test/unit/**/*.test.ts',
      'test/compat/**/*.test.ts',
      ...(runIntegration ? ['test/integration/**/*.test.ts'] : []),
    ],
    exclude: ['node_modules', 'dist', '.tshy', '.tshy-build'],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov', 'html'],
      include: ['src/**/*.ts'],
      exclude: ['src/generated/**', 'src/**/*.d.ts'],
    },
  },
});
