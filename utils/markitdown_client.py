"""
utils/markitdown_client.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
MarkItDown wrapper para agentes Insulleads.

Microsoft MarkItDown convierte documentos (PDF, Word, Excel, imágenes, etc.)
a Markdown estructurado. Más eficiente que firecrawl para documentos estáticos
que municipios publican en sus portales (permisos PDF, inspecciones Excel, etc.).

Arquitectura (orden de preferencia de engines):
  socrata > ckan > markitdown > firecrawl > playwright
  (API estructurada > documentos > web scraping)

Uso en agentes:
    from utils.markitdown_client import convert_document, convert_batch, is_available

    # Convertir un PDF de permiso a markdown estructurado
    md = convert_document("https://permits.city.gov/docs/permit-123.pdf")
    # md es un dict: {"text_content": "...", "metadata": {...}, "success": True}

    # Convertir múltiples documentos (Excel con inspecciones, Word con normas)
    docs = convert_batch([
        "file:///path/to/inspections.xlsx",
        "file:///path/to/code.pdf"
    ])

    # OCR de una imagen de permiso escaneado
    md = convert_document("permit-scan.jpg")
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

MARKITDOWN_TIMEOUT_S = int(os.getenv("MARKITDOWN_TIMEOUT_S", "30").split("#")[0].strip())
MARKITDOWN_MAX_SIZE_MB = int(os.getenv("MARKITDOWN_MAX_SIZE_MB", "100").split("#")[0].strip())

try:
    from markitdown import MarkItDown
    _md_available = True
except ImportError:
    _md_available = False
    logger.debug("markitdown not installed — pip install markitdown")


def _check():
    if not _md_available:
        raise ImportError("markitdown not installed: pip install markitdown")


def convert_document(
    source: str,
    is_url: bool | None = None,
    timeout_s: Optional[int] = None,
) -> dict[str, Any]:
    """
    Convert a document (PDF, Word, Excel, image, HTML, etc.) to Markdown.

    Args:
        source: File path (str or Path) or URL (http(s)://)
        is_url: If None, auto-detect from source prefix. Set explicitly if needed.
        timeout_s: Timeout for HTTP requests (if source is a URL)

    Returns:
        Dict with keys:
          - "text_content": Markdown string
          - "metadata": Dict with document info
          - "success": bool
          - "error": str (if success=False)
    """
    _check()

    # Auto-detect if source is a URL
    if is_url is None:
        is_url = isinstance(source, str) and (
            source.startswith("http://") or source.startswith("https://")
        )

    try:
        md = MarkItDown()

        if is_url:
            # URL: firecrawl handles web pages better, but markitdown can fetch PDFs/docs directly
            result = md.convert(source)
        else:
            # Local file
            file_path = Path(source)
            if not file_path.exists():
                return {
                    "text_content": "",
                    "metadata": {},
                    "success": False,
                    "error": f"File not found: {source}",
                }

            # Check file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            if file_size_mb > MARKITDOWN_MAX_SIZE_MB:
                return {
                    "text_content": "",
                    "metadata": {},
                    "success": False,
                    "error": f"File too large: {file_size_mb:.1f}MB > {MARKITDOWN_MAX_SIZE_MB}MB",
                }

            result = md.convert_local(str(file_path))

        return {
            "text_content": result.text_content,
            "metadata": result.metadata or {},
            "success": True,
        }
    except Exception as e:
        logger.warning(f"[markitdown] convert({source!r}) failed: {e}")
        return {
            "text_content": "",
            "metadata": {},
            "success": False,
            "error": str(e),
        }


def convert_batch(
    sources: list[str],
    stop_on_error: bool = False,
) -> list[dict[str, Any]]:
    """
    Convert multiple documents. Returns list of result dicts in source order.

    Args:
        sources: List of file paths or URLs
        stop_on_error: If True, stop processing after first error and return partial results

    Returns:
        List of conversion result dicts (one per source, in order)
    """
    results = []
    for source in sources:
        try:
            result = convert_document(source)
            results.append(result)
            if not result["success"] and stop_on_error:
                break
        except Exception as e:
            logger.warning(f"[markitdown] convert_batch error on {source!r}: {e}")
            results.append({
                "text_content": "",
                "metadata": {},
                "success": False,
                "error": str(e),
            })
            if stop_on_error:
                break
    return results


def extract_tables(
    source: str,
    is_url: bool | None = None,
) -> list[dict]:
    """
    Extract tables from a document and return as list of dicts.

    Useful for permit data in Excel or inspection schedules in PDFs.

    Args:
        source: File path or URL
        is_url: Auto-detect if None

    Returns:
        List of tables (each table is a list of row dicts)
    """
    result = convert_document(source, is_url=is_url)
    if not result["success"]:
        return []

    # Simple heuristic: split by markdown table markers (|)
    markdown = result["text_content"]
    tables = []
    current_table_lines = []

    for line in markdown.split("\n"):
        if "|" in line:
            current_table_lines.append(line)
        elif current_table_lines:
            # End of table
            if len(current_table_lines) > 2:  # header + separator + at least 1 row
                tables.append(_parse_markdown_table(current_table_lines))
            current_table_lines = []

    if current_table_lines and len(current_table_lines) > 2:
        tables.append(_parse_markdown_table(current_table_lines))

    return tables


def _parse_markdown_table(lines: list[str]) -> list[dict]:
    """Parse markdown table lines into list of row dicts."""
    if len(lines) < 3:
        return []

    # First line: headers
    headers = [h.strip() for h in lines[0].split("|")[1:-1]]

    # Skip separator line (second line)

    # Remaining lines: data rows
    rows = []
    for line in lines[2:]:
        values = [v.strip() for v in line.split("|")[1:-1]]
        if len(values) == len(headers):
            rows.append(dict(zip(headers, values)))

    return rows


def is_available() -> bool:
    """Return True if markitdown is installed."""
    return _md_available
