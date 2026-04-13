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
    "MAX_OUTREACH_PER_DAY": int(os.getenv("MAX_OUTREACH_PER_DAY", "200")),
    "SOURCES_ENABLED": [
        key for key in [
            "permits", "solar", "rodents", "flood",
            "construction", "deconstruction", "realestate",
            "energy", "places", "yelp",
        ] if os.getenv(f"AGENT_{key.upper()}", "true").lower() not in ("false", "0", "no")
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
