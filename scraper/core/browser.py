from contextlib import asynccontextmanager

from playwright.async_api import Browser, async_playwright


@asynccontextmanager
async def managed_browser() -> Browser:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            yield browser
        finally:
            await browser.close()
