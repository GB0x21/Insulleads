"""
Django settings for the Insulleads outreach daemon + CRM.

Adapted from OpenOutreach's `linkedin.django_settings`. The project is a
self-hosted lead-generation stack: public-data agents (permits, solar,
rodents, ...) discover candidates, a Bayesian qualifier ranks them, and
a daemon sends outreach through Telegram / Email / WhatsApp.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SECRET_KEY = os.getenv(
    "DJANGO_SECRET_KEY",
    "insulleads-dev-secret-change-me-in-production",
)
DEBUG = os.getenv("DJANGO_DEBUG", "true").lower() in ("1", "true", "yes")
ALLOWED_HOSTS = os.getenv("DJANGO_ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Local apps
    "outreach",
    "crm",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "outreach.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "crm" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "outreach.wsgi.application"

DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DATA_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = os.getenv("TIME_ZONE", "America/Los_Angeles")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ─── Daemon / pipeline config ───────────────────────────────────────────
OUTREACH = {
    "DISCOVERY_INTERVAL_MIN": int(os.getenv("DISCOVERY_INTERVAL_MIN", "60")),
    "QUALIFY_INTERVAL_MIN": int(os.getenv("QUALIFY_INTERVAL_MIN", "15")),
    "OUTREACH_INTERVAL_MIN": int(os.getenv("OUTREACH_INTERVAL_MIN", "5")),
    "ENRICH_INTERVAL_MIN": int(os.getenv("ENRICH_INTERVAL_MIN", "30")),
    "ENRICH_RETRY_HOURS": int(os.getenv("ENRICH_RETRY_HOURS", "24")),
    "ENRICH_MAX_ATTEMPTS": int(os.getenv("ENRICH_MAX_ATTEMPTS", "3")),
    "ENRICH_INLINE_ON_OUTREACH": os.getenv(
        "ENRICH_INLINE_ON_OUTREACH", "true"
    ).lower() in ("1", "true", "yes"),
    "MAX_OUTREACH_PER_DAY": int(os.getenv("MAX_OUTREACH_PER_DAY", "200")),
    "EMAIL_PROSPECT_INTERVAL_MIN": int(os.getenv("EMAIL_PROSPECT_INTERVAL_MIN", "30")),
    "MAX_EMAIL_PROSPECT_PER_DAY": int(os.getenv("MAX_EMAIL_PROSPECT_PER_DAY", "50")),
    "SOURCES_ENABLED": [
        key for key in [
            "permits", "solar", "rodents", "flood",
            "construction", "deconstruction", "realestate",
            "energy", "places", "yelp",
        ] if os.getenv(f"AGENT_{key.upper()}", "true").lower() not in ("false", "0", "no")
    ] + [
        # New sources: opt-in by default. Flip the env var to enable.
        # All three follow the same pattern (env=true → setup_crm seeds
        # an enabled Source row).
        key for key in [
            "dins",            # Cal Fire post-wildfire rebuilds
            "hud_multifamily", # HUD / LIHTC large-ticket leads
            "shovels",         # National permits aggregator (needs SHOVELS_API_KEY)
        ] if os.getenv(f"AGENT_{key.upper()}", "false").lower() in ("true", "1", "yes")
    ],
}

# ─── LLM layer (outreach/llm/) ─────────────────────────────────────────
# Adapter backend: "anthropic" (default), "suna" (stub — see
# docs/SUNA_INTEGRATION.md) or "noop" (always safe fallback).
_LLM_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
_LLM_ADAPTER = os.getenv("LLM_ADAPTER", "anthropic").lower()
LLM = {
    "ADAPTER": _LLM_ADAPTER,
    "ENABLED": bool(_LLM_API_KEY) or _LLM_ADAPTER == "suna",
    "ANTHROPIC_API_KEY": _LLM_API_KEY,
    "MODEL": os.getenv("LLM_MODEL", "claude-opus-4-6"),
    "ENRICH_MODEL": os.getenv("LLM_ENRICH_MODEL", ""),  # defaults to MODEL
    "MAX_TOKENS": int(os.getenv("LLM_MAX_TOKENS", "1024")),
    "SUNA_BASE_URL": os.getenv("SUNA_BASE_URL", ""),
    # LlamaIndex RAG — the writer cites PG&E / Title 24 / ENERGY STAR
    # snippets from a local vector index when available. Disabled by
    # default; enable by dropping PDFs in RAG_DOCS_DIR and running
    # `python manage.py build_rag_index`.
    "RAG_ENABLED": os.getenv("LLM_RAG_ENABLED", "true").lower() not in ("false", "0", "no"),
    "RAG_DOCS_DIR": os.getenv("LLM_RAG_DOCS_DIR", str(DATA_DIR / "rag_docs")),
    "RAG_INDEX_PATH": os.getenv("LLM_RAG_INDEX_PATH", str(DATA_DIR / "rag_index")),
}

# ─── Contract discovery + analysis ──────────────────────────────────────
# Multi-agent pipeline: discovery agents find open bids/RFPs on gov +
# construction portals and via Claude web_search; each Contract row is
# then analysed by the orchestrator + sub-agents in
# `outreach/llm/contract_analyzer.py` which writes scorecard / redlines /
# recommendation back to the row.
_CONTRACT_DOCS_DIR = Path(os.getenv("CONTRACT_DOCS_DIR", str(DATA_DIR / "contracts")))
_CONTRACT_DOCS_DIR.mkdir(parents=True, exist_ok=True)

CONTRACTS = {
    "DISCOVERY_ENABLED": os.getenv("CONTRACT_DISCOVERY_ENABLED", "true").lower()
    not in ("false", "0", "no"),
    "ANALYSIS_ENABLED": os.getenv("CONTRACT_ANALYSIS_ENABLED", "true").lower()
    not in ("false", "0", "no"),
    "DOCS_DIR": str(_CONTRACT_DOCS_DIR),
    "MAX_FILE_SIZE_MB": int(os.getenv("CONTRACT_MAX_FILE_SIZE_MB", "50")),
    "DISCOVERY_INTERVAL_MIN": int(os.getenv("CONTRACT_DISCOVERY_INTERVAL_MIN", "360")),
    # Providers
    "SAM_GOV_API_KEY": os.getenv("SAM_GOV_API_KEY", ""),
    "SAM_GOV_NAICS_CODES": [
        c.strip() for c in os.getenv("SAM_GOV_NAICS_CODES", "238310,238990").split(",")
        if c.strip()
    ],
    "WEB_SEARCH_ENABLED": os.getenv(
        "WEB_SEARCH_DISCOVERY_ENABLED", "true"
    ).lower() not in ("false", "0", "no"),
    # Analyzer models
    "ORCHESTRATOR_MODEL": os.getenv(
        "CONTRACT_ORCHESTRATOR_MODEL", "claude-opus-4-6"
    ),
    "SUBAGENT_MODEL": os.getenv("CONTRACT_SUBAGENT_MODEL", "claude-haiku-4-5"),
    "SYNTHESIZER_MODEL": os.getenv(
        "CONTRACT_SYNTHESIZER_MODEL", "claude-opus-4-6"
    ),
    "SUBAGENT_PARALLELISM": int(os.getenv("CONTRACT_SUBAGENT_PARALLELISM", "4")),
    "MAX_INPUT_CHARS": int(os.getenv("CONTRACT_MAX_INPUT_CHARS", "600000")),
    "CATEGORIES": [
        c.strip()
        for c in os.getenv(
            "CONTRACT_CATEGORIES",
            "pricing,liability,termination,sla_performance,ip_confidentiality,governing_law",
        ).split(",")
        if c.strip()
    ],
}


# ─── Thermal engine (outreach/thermal/) — Phase 1 ──────────────────────
# The pipeline is offline: `python manage.py thermal_pull` hits Earth
# Engine and writes per-building anomaly deltas to data/thermal/. The
# ThermalAgent at discover time only reads that parquet, so the daemon
# never needs GEE credentials in the hot path.
THERMAL = {
    "ENABLED": bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),
    "DEFAULT_BBOX": os.getenv("THERMAL_BBOX", ""),
    "MIN_ANOMALY_K": float(os.getenv("THERMAL_MIN_ANOMALY_K", "3.0")),
    "CACHE_DIR": str(DATA_DIR / "thermal"),
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {"handlers": ["console"], "level": "INFO"},
    "loggers": {
        "django.db.backends": {"level": "WARNING"},
        "outreach": {"level": "INFO", "propagate": True},
    },
}
