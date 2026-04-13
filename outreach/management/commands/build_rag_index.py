"""
Build (or rebuild) the LlamaIndex RAG index from PDFs / text files.

Usage:

    python manage.py build_rag_index
    python manage.py build_rag_index --rebuild
    python manage.py build_rag_index --docs-dir data/rag_docs --index-dir data/rag_index

Drop your PG&E / Title 24 / ENERGY STAR PDFs into ``data/rag_docs/``
before running. The index is persisted under ``data/rag_index/`` and
read back by :func:`outreach.llm.rag.get_default_index` at inference
time.
"""
from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from outreach.llm.rag import RagIndex, reset_default_index


class Command(BaseCommand):
    help = "Ingest docs into the LlamaIndex RAG index used by the outreach writer."

    def add_arguments(self, parser) -> None:
        llm_cfg = getattr(settings, "LLM", {}) or {}
        parser.add_argument(
            "--docs-dir",
            default=llm_cfg.get("RAG_DOCS_DIR") or "data/rag_docs",
            help="Directory of source PDFs / text files to ingest.",
        )
        parser.add_argument(
            "--index-dir",
            default=llm_cfg.get("RAG_INDEX_PATH") or "data/rag_index",
            help="Persistence directory for the built index.",
        )
        parser.add_argument(
            "--rebuild",
            action="store_true",
            help="Force a rebuild even if a persisted index already exists.",
        )

    def handle(self, *args, **opts) -> None:
        docs_dir = Path(opts["docs_dir"])
        index_dir = Path(opts["index_dir"])
        rebuild = bool(opts["rebuild"])

        self.stdout.write(
            f"[rag] docs_dir={docs_dir} index_dir={index_dir} rebuild={rebuild}"
        )
        if not docs_dir.exists():
            self.stdout.write(self.style.WARNING(
                f"[rag] {docs_dir} does not exist — create it and drop PDFs in."
            ))
            return

        index = RagIndex.load_or_build(docs_dir, index_dir, rebuild=rebuild)
        if not index.available:
            self.stdout.write(self.style.WARNING(
                "[rag] index is a no-op — check that llama_index is installed "
                "and that docs_dir is non-empty."
            ))
            return

        # Quick smoke retrieval to confirm the index answers queries.
        sample = index.retrieve(
            "insulation incentives for residential retrofits", k=2,
        )
        self.stdout.write(self.style.SUCCESS(
            f"[rag] built index — smoke retrieval returned {len(sample)} snippets."
        ))
        for snippet in sample:
            self.stdout.write(f"  - {snippet.source} (score={snippet.score:.3f})")

        reset_default_index()
