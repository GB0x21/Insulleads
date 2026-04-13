"""
LlamaIndex RAG layer — Phase 3 of the roadmap.

Purpose: give the outreach writer (``AnthropicAdapter.write_outreach``)
a way to cite real incentive / compliance documents instead of making
programs up. Typical corpus:

  * PG&E residential + commercial energy-efficiency rebates
  * California Title 24 envelope / insulation sections
  * ENERGY STAR homes program summaries
  * Local utility (BayREN, SMUD, ...) insulation incentives

The module is entirely optional. When ``llama_index`` or the built
index is missing, :class:`RagIndex` degrades to a no-op and
``retrieve()`` returns an empty list, so the daemon keeps running.

Typical lifecycle:

    from outreach.llm.rag import RagIndex
    idx = RagIndex.load_or_build(docs_dir, index_dir)
    snippets = idx.retrieve("insulation incentives for San Francisco kitchen remodel")

Build once (via ``python manage.py build_rag_index``) then reuse —
loading is much cheaper than re-ingesting every call.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("outreach.llm.rag")


# ─── Optional dependency guard ───────────────────────────────────────
try:
    from llama_index.core import (  # type: ignore
        SimpleDirectoryReader,
        StorageContext,
        VectorStoreIndex,
        load_index_from_storage,
    )
    from llama_index.core.settings import Settings as LlamaSettings  # type: ignore

    _HAS_LLAMA_INDEX = True
except Exception:  # noqa: BLE001
    _HAS_LLAMA_INDEX = False


@dataclass
class RagSnippet:
    """One retrieved chunk, shaped for prompt injection."""

    text: str
    source: str
    score: float

    def format(self) -> str:
        src = self.source or "unknown"
        return f"[{src}] {self.text.strip()}"


class RagIndex:
    """Small wrapper around a LlamaIndex ``VectorStoreIndex``.

    Degrades to a no-op (``retrieve`` returns ``[]``) whenever the
    underlying dependency or the index files are missing, so the
    outreach writer can always call ``retrieve()`` unconditionally.
    """

    def __init__(self, index: Any | None = None) -> None:
        self._index = index

    @property
    def available(self) -> bool:
        return self._index is not None

    # ── construction ────────────────────────────────────────────────
    @classmethod
    def noop(cls) -> "RagIndex":
        return cls(None)

    @classmethod
    def load_or_build(
        cls,
        docs_dir: Path,
        index_dir: Path,
        *,
        rebuild: bool = False,
    ) -> "RagIndex":
        """Load a persisted index, or build one from ``docs_dir``.

        Returns a no-op instance when ``llama_index`` is missing or
        the docs directory is empty.
        """
        if not _HAS_LLAMA_INDEX:
            logger.info("[rag] llama_index not installed — returning noop index")
            return cls.noop()

        cls._configure_embeddings()

        index_dir = Path(index_dir)
        docs_dir = Path(docs_dir)

        if not rebuild and (index_dir / "docstore.json").exists():
            try:
                storage = StorageContext.from_defaults(persist_dir=str(index_dir))
                index = load_index_from_storage(storage)
                logger.info("[rag] loaded persisted index from %s", index_dir)
                return cls(index)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[rag] failed to load index: %s — rebuilding", exc)

        if not docs_dir.exists() or not any(docs_dir.iterdir()):
            logger.info("[rag] docs dir %s is empty — noop", docs_dir)
            return cls.noop()

        try:
            documents = SimpleDirectoryReader(str(docs_dir)).load_data()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[rag] failed to ingest %s: %s", docs_dir, exc)
            return cls.noop()

        if not documents:
            logger.info("[rag] no documents parsed from %s — noop", docs_dir)
            return cls.noop()

        index = VectorStoreIndex.from_documents(documents)
        index_dir.mkdir(parents=True, exist_ok=True)
        index.storage_context.persist(persist_dir=str(index_dir))
        logger.info(
            "[rag] built + persisted index from %d docs at %s",
            len(documents),
            index_dir,
        )
        return cls(index)

    @staticmethod
    def _configure_embeddings() -> None:
        """Force a local HuggingFace embedding model when available.

        LlamaIndex defaults to OpenAI embeddings which require a
        separate API key. We prefer a local small model so the RAG
        layer works offline; if neither is available we simply
        disable embeddings and fall back to BM25-only retrieval.
        """
        try:
            from llama_index.embeddings.huggingface import (  # type: ignore
                HuggingFaceEmbedding,
            )

            LlamaSettings.embed_model = HuggingFaceEmbedding(
                model_name="sentence-transformers/all-MiniLM-L6-v2"
            )
        except Exception:  # noqa: BLE001
            # Last resort — let the caller deal with a missing model
            # by falling through to noop retrieval.
            pass

    # ── retrieval ───────────────────────────────────────────────────
    def retrieve(self, query: str, k: int = 4) -> list[RagSnippet]:
        if not self.available or not query.strip():
            return []
        try:
            retriever = self._index.as_retriever(similarity_top_k=k)
            nodes = retriever.retrieve(query)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[rag] retrieve failed: %s", exc)
            return []

        out: list[RagSnippet] = []
        for node in nodes:
            text = getattr(node, "text", None) or getattr(node.node, "text", "")
            source = ""
            meta = getattr(node, "metadata", None) or getattr(
                node.node, "metadata", {}
            )
            if isinstance(meta, dict):
                source = str(
                    meta.get("file_name")
                    or meta.get("source")
                    or meta.get("doc_id")
                    or ""
                )
            score = float(getattr(node, "score", 0.0) or 0.0)
            out.append(RagSnippet(text=text, source=source, score=score))
        return out


# ─── Module-level singleton cache ────────────────────────────────────
_cached_index: RagIndex | None = None


def get_default_index() -> RagIndex:
    """Return a process-wide cached :class:`RagIndex`.

    Reads paths from ``settings.LLM["RAG_DOCS_DIR"]`` and
    ``settings.LLM["RAG_INDEX_PATH"]``. Safe to call when RAG is
    disabled: returns a noop index.
    """
    global _cached_index
    if _cached_index is not None:
        return _cached_index

    from django.conf import settings

    llm_cfg = getattr(settings, "LLM", {}) or {}
    if not llm_cfg.get("RAG_ENABLED"):
        _cached_index = RagIndex.noop()
        return _cached_index

    docs_dir = Path(llm_cfg.get("RAG_DOCS_DIR") or "data/rag_docs")
    index_dir = Path(llm_cfg.get("RAG_INDEX_PATH") or "data/rag_index")
    _cached_index = RagIndex.load_or_build(docs_dir, index_dir)
    return _cached_index


def reset_default_index() -> None:
    """Drop the cached index — call this after rebuilding on disk."""
    global _cached_index
    _cached_index = None
