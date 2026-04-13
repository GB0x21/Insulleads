#!/usr/bin/env python
"""
Django management entrypoint — adapted from OpenOutreach.

Usage:
  python manage.py rundaemon              # interactive daemon (default)
  python manage.py rundaemon --onboard config.json
  python manage.py runserver              # Django Admin / CRM at :8000
  python manage.py migrate
  python manage.py createsuperuser
  python manage.py setup_crm              # bootstrap SiteConfig + default campaign

No subcommand (or first arg is a flag) -> defaults to `rundaemon`.
"""
import os
import sys
import warnings

# Filter a harmless pydantic deprecation warning surfaced by some LLM SDKs
warnings.filterwarnings(
    "ignore",
    message=r".*PydanticDeprecatedSince20.*",
)


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "outreach.django_settings")

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Django import failed. Did you `make install` / "
            "`pip install -r requirements/local.txt` and activate the venv?"
        ) from exc

    argv = sys.argv[:]
    # No subcommand (or first arg is a flag) -> default to rundaemon
    if len(argv) == 1 or (len(argv) > 1 and argv[1].startswith("-")):
        argv.insert(1, "rundaemon")

    execute_from_command_line(argv)


if __name__ == "__main__":
    main()
