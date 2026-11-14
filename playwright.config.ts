import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './test/browser',
  workers: 1,
  timeout: 180_000,
  use: { browserName: 'chromium', headless: true },
});
