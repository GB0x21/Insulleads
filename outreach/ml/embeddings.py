"""
Lead embedding utilities.

OpenOutreach uses fastembed for 384-dim sentence embeddings. We expose the
same interface but with a graceful hash-trick fallback so the repo works
out-of-the-box without downloading ~100MB of model weights on first run.

Set `OUTREACH_EMBEDDING_BACKEND=fastembed` in `.env` to enable the real
fastembed backend (add `fastembed` to requirements/local.txt).
"""
from __future__ import annotations

import hashlib
import os
import pickle
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from outreach.models import Lead

EMBED_DIM = 384
_BACKEND = os.getenv("OUTREACH_EMBEDDING_BACKEND", "hash")
_model = None


def _text_for(lead: "Lead") -> str:
    return " | ".join(
        str(x)
        for x in (
            lead.title,
            lead.description,
            lead.city,
            lead.project_type,
            lead.contact_company,
            lead.raw.get("notes") if isinstance(lead.raw, dict) else "",
        )
        if x
    )


def _hash_embed(text: str, dim: int = EMBED_DIM) -> np.ndarray:
    """Feature-hashing fallback: deterministic, no dependencies, ~OK signal."""
    vec = np.zeros(dim, dtype=np.float32)
    for token in text.lower().split():
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 16) % 2 == 0 else -1.0
        vec[idx] += sign
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


def _fastembed_embed(text: str) -> np.ndarray:
    global _model
    if _model is None:
        from fastembed import TextEmbedding  # type: ignore

        _model = TextEmbedding()
    return np.asarray(list(_model.embed([text]))[0], dtype=np.float32)


def embed_text(text: str) -> np.ndarray:
    if _BACKEND == "fastembed":
        try:
            return _fastembed_embed(text)
        except Exception:  # noqa: BLE001
            pass
    return _hash_embed(text)


def embed_lead(lead: "Lead") -> bytes:
    """Return a pickled numpy vector, ready to be stored in a BinaryField."""
    return pickle.dumps(embed_text(_text_for(lead)))


def load_embedding(blob: bytes | None) -> np.ndarray | None:
    if blob is None:
        return None
    return pickle.loads(blob)
