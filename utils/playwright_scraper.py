"""
utils/playwright_scraper.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Playwright browser automation para portales JS-rendered.

Complementa firecrawl_client para casos que requieren:
  - Login / sesión autenticada
  - Paginación interactiva (click en "Next")
  - Formularios de búsqueda con submit
  - Portales legacy que bloquean scrapers HTTP directos

Instalar browsers la primera vez:
    pip install playwright
    playwright install chromium

Uso en agentes:
    from utils.playwright_scraper import scrape_page, scrape_table, is_available

    # Contenido HTML de una página JS-heavy
    html = scrape_page("https://portal.city.gov/permits", wait_selector="table.results")

    # Tabla completa como lista de dicts
    rows = scrape_table(
        "https://permits.sanjose.gov/search",
        table_selector="table#permitResults",
    )
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

_env_int = lambda k, d: int(os.getenv(k, str(d)).split("#")[0].strip()) if \
    os.getenv(k, str(d)).split("#")[0].strip().isdigit() else d

PLAYWRIGHT_TIMEOUT_MS = _env_int("PLAYWRIGHT_TIMEOUT_MS", 30000)
PLAYWRIGHT_HEADLESS   = os.getenv("PLAYWRIGHT_HEADLESS", "true").split("#")[0].strip().lower() \
                        not in ("false", "0")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    _pw_available = True
except ImportError:
    _pw_available = False
    logger.debug(
        "playwright not installed — "
        "pip install playwright && playwright install chromium"
    )


def _check():
    if not _pw_available:
        raise ImportError(
            "playwright not installed: "
            "pip install playwright && playwright install chromium"
        )


def scrape_page(
    url: str,
    wait_selector: Optional[str] = None,
    js_script: Optional[str] = None,
    timeout_ms: Optional[int] = None,
) -> str:
    """
    Load a URL in a headless browser and return the full page HTML.

    Args:
        url:           URL to load
        wait_selector: CSS selector to wait for before capturing (ensures JS renders)
        js_script:     Optional JS to evaluate; its return value replaces HTML output
        timeout_ms:    Override default timeout

    Returns:
        HTML string or JS result string, empty string on error.
    """
    _check()
    timeout = timeout_ms or PLAYWRIGHT_TIMEOUT_MS
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
            page    = browser.new_page()
            page.goto(url, timeout=timeout)
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout)
            if js_script:
                result = page.evaluate(js_script)
                browser.close()
                return str(result)
            content = page.content()
            browser.close()
            return content
    except Exception as e:
        logger.warning(f"[playwright] scrape_page({url!r}) failed: {e}")
        return ""


def scrape_table(
    url: str,
    table_selector: str,
    wait_selector: Optional[str] = None,
    timeout_ms: Optional[int] = None,
) -> list[dict]:
    """
    Navigate to a URL, find a table by CSS selector, and return rows as dicts
    keyed by column header text.

    Args:
        url:            URL to load
        table_selector: CSS selector for the target <table>
        wait_selector:  CSS selector to wait for (defaults to table_selector)
        timeout_ms:     Override default timeout

    Returns:
        List of row dicts, empty list on error.
    """
    _check()
    timeout = timeout_ms or PLAYWRIGHT_TIMEOUT_MS
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
            page    = browser.new_page()
            page.goto(url, timeout=timeout)
            page.wait_for_selector(wait_selector or table_selector, timeout=timeout)

            table   = page.locator(table_selector).first
            headers = [h.inner_text().strip() for h in table.locator("th").all()]
            if not headers:
                browser.close()
                return []

            rows_out = []
            for row in table.locator("tr").all():
                cells = row.locator("td").all()
                if not cells:
                    continue
                row_dict = {
                    (headers[i] if i < len(headers) else f"col_{i}"): cell.inner_text().strip()
                    for i, cell in enumerate(cells)
                }
                rows_out.append(row_dict)

            browser.close()
            return rows_out
    except Exception as e:
        logger.warning(f"[playwright] scrape_table({url!r}) failed: {e}")
        return []


def scrape_paginated(
    url: str,
    row_selector: str,
    next_selector: str,
    extract_fn: Callable[["Page"], list[dict]],  # type: ignore[name-defined]
    max_pages: int = 10,
    timeout_ms: Optional[int] = None,
) -> list[dict]:
    """
    Scrape a JS-paginated list across multiple pages.

    On each page, extract_fn(page) is called and its results accumulated.
    Clicks next_selector to advance until it disappears or max_pages reached.

    Args:
        url:            Starting URL
        row_selector:   CSS selector confirming the content has loaded
        next_selector:  CSS selector for the "Next page" button
        extract_fn:     Callable receiving a Playwright Page, returning list[dict]
        max_pages:      Safety cap on page iterations
        timeout_ms:     Override default timeout

    Returns:
        Accumulated list of all extracted rows.
    """
    _check()
    timeout = timeout_ms or PLAYWRIGHT_TIMEOUT_MS
    results: list[dict] = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
            page    = browser.new_page()
            page.goto(url, timeout=timeout)
            for _ in range(max_pages):
                page.wait_for_selector(row_selector, timeout=timeout)
                results.extend(extract_fn(page))
                next_btn = page.locator(next_selector)
                if not next_btn.count() or not next_btn.is_enabled():
                    break
                next_btn.click()
                page.wait_for_load_state("networkidle", timeout=timeout)
            browser.close()
    except Exception as e:
        logger.warning(f"[playwright] scrape_paginated({url!r}) failed: {e}")
    return results


def fill_and_submit(
    url: str,
    fields: dict[str, str],
    submit_selector: str,
    wait_selector: Optional[str] = None,
    timeout_ms: Optional[int] = None,
) -> str:
    """
    Fill a form and return the resulting page HTML.
    Useful for permit search portals with date/city filters.

    Args:
        url:             URL of the form page
        fields:          {css_selector: value} pairs to fill
        submit_selector: CSS selector for the submit button
        wait_selector:   CSS selector to wait for after submission
        timeout_ms:      Override default timeout

    Returns:
        HTML of the results page, empty string on error.
    """
    _check()
    timeout = timeout_ms or PLAYWRIGHT_TIMEOUT_MS
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
            page    = browser.new_page()
            page.goto(url, timeout=timeout)
            for selector, value in fields.items():
                page.fill(selector, value)
            page.click(submit_selector)
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout)
            else:
                page.wait_for_load_state("networkidle", timeout=timeout)
            content = page.content()
            browser.close()
            return content
    except Exception as e:
        logger.warning(f"[playwright] fill_and_submit({url!r}) failed: {e}")
        return ""


def is_available() -> bool:
    """Return True if playwright is installed."""
    return _pw_available
