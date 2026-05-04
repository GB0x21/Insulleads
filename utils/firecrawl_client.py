"""
utils/firecrawl_client.py
━━━━━━━━━━━━━━━━━━━━━━━━━
Firecrawl wrapper para agentes Insulleads.

Firecrawl convierte cualquier URL en markdown/JSON estructurado manejando
JS rendering, paginación y bloqueos anti-bot. Complementa los engines
socrata/ckan/seeclickfix para portales sin API pública.

Uso en agentes:
    from utils.firecrawl_client import scrape_url, search_web, is_available

    # Scrape estructurado con schema
    data = scrape_url(
        "https://permits.city.gov/search",
        extract_schema={
            "type": "object",
            "properties": {
                "permits": {"type": "array", "items": {"type": "object",
                    "properties": {"address": {"type": "string"},
                                   "contractor": {"type": "string"},
                                   "value": {"type": "number"}}}}
            }
        }
    )

    # Buscar en la web y obtener contenido completo
    results = search_web("building permits Oakland contractor 2024", limit=5)
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
FIRECRAWL_TIMEOUT = int(os.getenv("FIRECRAWL_TIMEOUT_S", "30").split("#")[0].strip())

try:
    from firecrawl import FirecrawlApp
    _fc_available = True
except ImportError:
    _fc_available = False
    logger.debug("firecrawl-py not installed — pip install firecrawl-py")


def _client() -> "FirecrawlApp":
    if not _fc_available:
        raise ImportError("firecrawl-py not installed: pip install firecrawl-py")
    if not FIRECRAWL_API_KEY:
        raise ValueError("FIRECRAWL_API_KEY not set in environment")
    return FirecrawlApp(api_key=FIRECRAWL_API_KEY)


def scrape_url(
    url: str,
    formats: Optional[list[str]] = None,
    extract_schema: Optional[dict] = None,
) -> dict:
    """
    Scrape a single URL and return structured content.

    Args:
        url: URL to scrape
        formats: list of formats — ["markdown"] | ["extract"] | ["html", "markdown"]
        extract_schema: JSON Schema dict for structured extraction (LLM-powered)

    Returns:
        dict with 'markdown', 'html', or 'extract' keys, or {} on error.
    """
    if formats is None:
        formats = ["markdown"]
    try:
        fc = _client()
        params: dict[str, Any] = {"formats": formats}
        if extract_schema:
            params["extract"] = {"schema": extract_schema}
        result = fc.scrape_url(url, params=params)
        return result if isinstance(result, dict) else {}
    except Exception as e:
        logger.warning(f"[firecrawl] scrape_url({url!r}) failed: {e}")
        return {}


def batch_scrape(
    urls: list[str],
    formats: Optional[list[str]] = None,
    extract_schema: Optional[dict] = None,
) -> list[dict]:
    """
    Scrape multiple URLs asynchronously (Firecrawl manages the queue).
    Returns list of result dicts in the same order as urls.
    """
    if not urls:
        return []
    if formats is None:
        formats = ["markdown"]
    try:
        fc = _client()
        params: dict[str, Any] = {"formats": formats}
        if extract_schema:
            params["extract"] = {"schema": extract_schema}
        batch_result = fc.async_batch_scrape_urls(urls, params=params)
        batch_id = batch_result.get("id") if isinstance(batch_result, dict) else None
        if not batch_id:
            return []
        completed = fc.check_batch_scrape_status(batch_id)
        return completed.get("data", []) if isinstance(completed, dict) else []
    except Exception as e:
        logger.warning(f"[firecrawl] batch_scrape({len(urls)} urls) failed: {e}")
        return []


def search_web(
    query: str,
    limit: int = 5,
    formats: Optional[list[str]] = None,
) -> list[dict]:
    """
    Web search returning full-page content for each result.

    Useful for enriching leads: finding contractor contact info, verifying
    project details, or discovering permit portals not yet in SOURCES.

    Returns list of dicts with 'url', 'title', 'markdown' keys.
    """
    if formats is None:
        formats = ["markdown"]
    try:
        fc = _client()
        results = fc.search(query, params={"limit": limit, "formats": formats})
        if isinstance(results, dict):
            return results.get("data", [])
        return results or []
    except Exception as e:
        logger.warning(f"[firecrawl] search_web({query!r}) failed: {e}")
        return []


def crawl_site(
    url: str,
    max_pages: int = 20,
    include_paths: Optional[list[str]] = None,
    exclude_paths: Optional[list[str]] = None,
) -> list[dict]:
    """
    Crawl an entire site starting from url, up to max_pages pages.
    Use for city portals where leads are spread across multiple pages.

    Returns list of page dicts with 'url' and 'markdown' keys.
    """
    try:
        fc = _client()
        params: dict[str, Any] = {
            "limit": max_pages,
            "scrapeOptions": {"formats": ["markdown"]},
        }
        if include_paths:
            params["includePaths"] = include_paths
        if exclude_paths:
            params["excludePaths"] = exclude_paths
        result = fc.crawl_url(url, params=params)
        if isinstance(result, dict):
            return result.get("data", [])
        return result or []
    except Exception as e:
        logger.warning(f"[firecrawl] crawl_site({url!r}) failed: {e}")
        return []


def is_available() -> bool:
    """Return True if firecrawl-py is installed and FIRECRAWL_API_KEY is set."""
    return _fc_available and bool(FIRECRAWL_API_KEY)
