"""
services/kb_backends/file_backend.py

JSONL-based retrieval backend for small-to-medium knowledge bases.

Format of each line in the .jsonl file:
    {"id": "unique-id", "text": "Full document text.", "metadata": {...}}

Retrieval algorithm:
    Token overlap scoring (TF-weighted) — dependency-free, suitable for
    FAQ-style KBs up to ~5,000 entries.  Replace with a vector store backend
    (faiss_backend.py, qdrant_backend.py) for larger corpora.

Lifecycle:
    Loaded once at first use, held in memory.  Call close() to release.
"""
import json
import logging
import math
import re
from pathlib import Path
from typing import Any

from services.kb_backends.base import KBBackend

logger = logging.getLogger(__name__)

_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "the", "is", "are", "was", "were", "in", "on", "at", "to",
    "of", "and", "or", "for", "with", "by", "as", "it", "its", "be", "been",
    "this", "that", "from", "have", "has", "had", "do", "does", "not",
    "can", "will", "i", "you", "we", "they", "he", "she", "me", "my",
    "hai", "hain", "ka", "ki", "ke", "ko", "se", "mein", "aur", "ya",
    "kya", "kaise", "main", "aap", "hum", "yeh", "woh", "ek", "bhi",
})


def _tokenise(text: str) -> list[str]:
    """Lower-case, split on non-alphanumeric, remove stopwords."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return [t for t in tokens if t not in _STOPWORDS and len(t) > 1]


def _score(query_tokens: set[str], doc_tokens: list[str]) -> float:
    """Simple TF-overlap score: sum of log(1 + tf) for each query token found."""
    score = 0.0
    doc_tf: dict[str, int] = {}
    for t in doc_tokens:
        doc_tf[t] = doc_tf.get(t, 0) + 1
    for qt in query_tokens:
        if qt in doc_tf:
            score += math.log1p(doc_tf[qt])
    return score


class FileBackend(KBBackend):
    """
    Retrieval from a local JSONL file.

    Args:
        file_path: Path to the .jsonl file (absolute or relative to project root).
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = Path(file_path)
        self._docs: list[dict[str, Any]] | None = None
        self._doc_tokens: list[list[str]] | None = None

    def _load(self) -> None:
        if self._docs is not None:
            return

        if not self._file_path.exists():
            logger.warning(
                f"FileBackend: KB file not found at '{self._file_path}'. "
                "Returning empty results."
            )
            self._docs = []
            self._doc_tokens = []
            return

        docs: list[dict[str, Any]] = []
        tokens: list[list[str]] = []

        with self._file_path.open("r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    text = record.get("text", "")
                    if not isinstance(text, str) or not text.strip():
                        logger.warning(
                            f"FileBackend: line {lineno} has no 'text' field — skipped."
                        )
                        continue
                    docs.append(record)
                    tokens.append(_tokenise(text))
                except json.JSONDecodeError as exc:
                    logger.warning(
                        f"FileBackend: JSON parse error on line {lineno}: {exc} — skipped."
                    )

        self._docs = docs
        self._doc_tokens = tokens
        logger.info(
            f"FileBackend: loaded {len(docs)} documents from '{self._file_path}'."
        )

    async def search(self, query: str, k: int = 3) -> list[str]:
        self._load()

        if not self._docs:
            return []

        query_tokens = set(_tokenise(query))
        if not query_tokens:
            return []

        scored = [
            (_score(query_tokens, dt), idx)
            for idx, dt in enumerate(self._doc_tokens)  # type: ignore
        ]
        scored.sort(key=lambda x: x[0], reverse=True)

        results: list[str] = []
        for score, idx in scored[:k]:
            if score <= 0:
                break
            results.append(self._docs[idx]["text"])  # type: ignore

        logger.debug(
            f"FileBackend: query='{query[:60]}' → {len(results)} results"
        )
        return results
