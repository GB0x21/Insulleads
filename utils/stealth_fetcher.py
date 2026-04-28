"""
utils/stealth_fetcher.py
━━━━━━━━━━━━━━━━━━━━━━━
Fetcher HTTP con fingerprint de navegador real (Scrapling / curl_cffi).

Usado como fallback cuando requests estándar recibe 403/WAF en endpoints
de datos públicos que requieren TLS fingerprint de Chrome.

Requiere: scrapling + curl_cffi + browserforge
  pip install scrapling curl_cffi browserforge
"""
from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

try:
    import warnings as _warnings
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        from scrapling import Fetcher as _ScraplingFetcher
    _SCRAPLING_OK = True
except Exception:
    _SCRAPLING_OK = False


def _make_fetcher():
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        return _ScraplingFetcher()


def fetch_json(
    url: str,
    params: dict | None = None,
    timeout: int = 30,
    impersonate: str = "chrome124",
) -> list | dict | None:
    """
    GET a JSON API endpoint usando TLS fingerprint de Chrome.

    Retorna el JSON parseado (list o dict) o None si falla.
    """
    if not _SCRAPLING_OK:
        logger.debug("[StealthFetch] scrapling no instalado — usando requests")
        return _fallback_requests(url, params, timeout)

    full_url = f"{url}?{urlencode(params)}" if params else url
    try:
        fetcher = _make_fetcher()
        resp = fetcher.get(
            full_url,
            timeout=timeout,
            stealthy_headers=True,
            impersonate=impersonate,
        )
        if resp.status == 200:
            return json.loads(resp.body)
        logger.debug("[StealthFetch] HTTP %s para %s", resp.status, url)
        return None
    except Exception as e:
        logger.debug("[StealthFetch] %s: %s", url, e)
        return None


def post_form(
    url: str,
    data: dict,
    cookies: dict | None = None,
    extra_headers: dict | None = None,
    timeout: int = 30,
    impersonate: str = "chrome124",
) -> bytes | None:
    """
    POST a un formulario HTML y devuelve el body como bytes.
    Mantiene cookies de sesión (útil para portales ASP.NET).
    """
    if not _SCRAPLING_OK:
        return None

    kw: dict[str, Any] = {
        "timeout": timeout,
        "stealthy_headers": True,
        "impersonate": impersonate,
    }
    if cookies:
        kw["cookies"] = cookies
    if extra_headers:
        kw["extra_headers"] = extra_headers

    try:
        fetcher = _make_fetcher()
        resp = fetcher.post(url, data=data, **kw)
        if resp.status == 200:
            return resp.body
        logger.debug("[StealthFetch] POST HTTP %s para %s", resp.status, url)
        return None
    except Exception as e:
        logger.debug("[StealthFetch] POST %s: %s", url, e)
        return None


def get_raw(
    url: str,
    params: dict | None = None,
    cookies: dict | None = None,
    extra_headers: dict | None = None,
    timeout: int = 30,
    impersonate: str = "chrome124",
) -> tuple[int, bytes, dict]:
    """
    GET raw: retorna (status_code, body_bytes, cookies_dict).
    """
    if not _SCRAPLING_OK:
        return 0, b"", {}

    full_url = f"{url}?{urlencode(params)}" if params else url
    kw: dict[str, Any] = {
        "timeout": timeout,
        "stealthy_headers": True,
        "impersonate": impersonate,
    }
    if cookies:
        kw["cookies"] = cookies
    if extra_headers:
        kw["extra_headers"] = extra_headers

    try:
        fetcher = _make_fetcher()
        resp = fetcher.get(full_url, **kw)
        return resp.status, resp.body, dict(resp.cookies)
    except Exception as e:
        logger.debug("[StealthFetch] GET %s: %s", url, e)
        return 0, b"", {}


def _fallback_requests(url: str, params: dict | None, timeout: int) -> list | dict | None:
    try:
        import requests
        resp = requests.get(url, params=params, timeout=timeout,
                            headers={"Accept": "application/json"})
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None
