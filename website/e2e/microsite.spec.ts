import { test, expect, type Page } from '@playwright/test'

const article = (page: Page) => page.locator('article').first()

/** Collect internal same-origin links from the page and assert they return 200. */
async function assertNoDeadInternalLinks(page: Page) {
  const hrefs = await page.locator('a[href]').evaluateAll((anchors) => {
    const origin = window.location.origin
    const base = window.location.pathname.split('/').slice(0, 2).join('/') // /object-log
    const out = new Set<string>()
    for (const a of anchors) {
      const href = (a as HTMLAnchorElement).getAttribute('href')
      if (!href || href.startsWith('#') || href.startsWith('mailto:')) continue
      let url: URL
      try {
        url = new URL(href, window.location.href)
      } catch {
        continue
      }
      if (url.origin !== origin) continue
      // Only site pages under base path
      if (!url.pathname.startsWith(base) && base !== '') continue
      // Strip hash
      url.hash = ''
      out.add(url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`.replace(/\/\/$/, '/'))
    }
    return [...out]
  })

  const failures: string[] = []
  for (const path of hrefs) {
    const res = await page.request.get(path)
    if (!res.ok()) {
      failures.push(`${path} → ${res.status()}`)
    }
  }
  expect(failures, `dead internal links:\n${failures.join('\n')}`).toEqual([])
}

test.describe('Homepage', () => {
  test('hero, paths, and screenshot', async ({ page }, testInfo) => {
    await page.goto('./')
    await expect(page.getByRole('heading', { name: /Many writes/i })).toBeVisible()
    await expect(page.getByText(/group-commits opaque batches/i).first()).toBeVisible()
    await expect(page.getByRole('link', { name: /Get started/i }).first()).toBeVisible()
    await expect(page.getByRole('link', { name: /Why this exists/i }).first()).toBeVisible()
    await expect(page.getByRole('heading', { name: /How a produce resolves/i })).toBeVisible()
    await expect(page.locator('.olog-seal')).toBeVisible()

    await expect(page).toHaveScreenshot(`homepage-${testInfo.project.name}.png`, {
      fullPage: true,
      maxDiffPixelRatio: 0.04,
    })
  })

  test('no dead internal links on home', async ({ page }) => {
    await page.goto('./')
    await assertNoDeadInternalLinks(page)
  })
})

test.describe('Why', () => {
  test('section loads with thesis', async ({ page }, testInfo) => {
    await page.goto('./why/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    await expect(article(page).getByText(/One PUT per produce/i)).toBeVisible()
    await expect(page).toHaveScreenshot(`why-${testInfo.project.name}.png`, {
      fullPage: true,
      maxDiffPixelRatio: 0.04,
    })
  })

  test('no dead internal links', async ({ page }) => {
    await page.goto('./why/')
    await assertNoDeadInternalLinks(page)
  })
})

test.describe('Get Started', () => {
  test('install and sample code', async ({ page }, testInfo) => {
    await page.goto('./get-started/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    await expect(article(page).getByText(/object-log = "0.3"/)).toBeVisible()
    await expect(article(page).getByText(/LogEngine::new/)).toBeVisible()
    await expect(page).toHaveScreenshot(`get-started-${testInfo.project.name}.png`, {
      fullPage: true,
      maxDiffPixelRatio: 0.04,
    })
  })
})

test.describe('Concepts', () => {
  test('landing cards', async ({ page }, testInfo) => {
    await page.goto('./concepts/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    // Content cards (hextra-card) — use text match; sidebar also has these names.
    await expect(article(page).locator('.hextra-card', { hasText: 'BlobStore' })).toBeVisible()
    await expect(article(page).locator('.hextra-card', { hasText: 'LogEngine' })).toBeVisible()
    await expect(article(page).locator('.hextra-card', { hasText: 'Sequencer' })).toBeVisible()
    await expect(page).toHaveScreenshot(`concepts-${testInfo.project.name}.png`, {
      fullPage: true,
      maxDiffPixelRatio: 0.04,
    })
  })

  test('blob-store leaf', async ({ page }) => {
    await page.goto('./concepts/blob-store/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    await expect(article(page).getByText(/LocalBlobStore/)).toBeVisible()
  })

  test('no dead internal links on concepts', async ({ page }) => {
    await page.goto('./concepts/')
    await assertNoDeadInternalLinks(page)
  })
})

test.describe('Reference', () => {
  test('api and cli pages', async ({ page }, testInfo) => {
    await page.goto('./reference/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    await page.goto('./reference/cli/')
    await expect(article(page).getByRole('heading', { level: 1 }).first()).toBeVisible()
    await expect(article(page).getByText(/produce/).first()).toBeVisible()
    await expect(page).toHaveScreenshot(`reference-cli-${testInfo.project.name}.png`, {
      fullPage: true,
      maxDiffPixelRatio: 0.04,
    })
  })

  test('no dead internal links on reference', async ({ page }) => {
    await page.goto('./reference/')
    await assertNoDeadInternalLinks(page)
  })
})

test.describe('Site-wide dead links', () => {
  const routes = [
    './',
    './why/',
    './get-started/',
    './concepts/',
    './concepts/blob-store/',
    './concepts/log-engine/',
    './concepts/sequencer/',
    './concepts/durability/',
    './reference/',
    './reference/api/',
    './reference/cli/',
  ]

  for (const route of routes) {
    test(`route ${route} returns 200`, async ({ page }) => {
      const res = await page.goto(route)
      expect(res?.ok(), `${route} status ${res?.status()}`).toBeTruthy()
    })
  }
})
