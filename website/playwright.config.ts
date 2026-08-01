import { defineConfig, devices } from '@playwright/test'

const port = 1313
const baseURL = `http://127.0.0.1:${port}/object-log/`

export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL,
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1280, height: 800 },
      },
    },
    {
      // Content/link checks only; screenshots are desktop-chromium to avoid
      // 1px full-page height flakes that blocked Pages deploys.
      name: 'mobile',
      use: {
        ...devices['Pixel 7'],
        viewport: { width: 390, height: 844 },
      },
    },
  ],
  webServer: {
    // Hugo 0.159+ serves from disk by default and would overwrite website/public
    // with a 127.0.0.1 / livereload build — which Pages then deployed.
    // Always render to memory so the production artifact stays intact.
    command: process.env.CI
      ? `hugo server --renderToMemory --bind 127.0.0.1 --port ${port} --baseURL ${baseURL} --disableFastRender --environment production`
      : `hugo server --renderToMemory --bind 127.0.0.1 --port ${port} --baseURL ${baseURL} --disableFastRender`,
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
})
