"""
services/kb_backends/faiss_backend.py

FAISS vector-search backend stub.

To activate:
    1. pip install faiss-cpu openai
    2. Build an index with scripts/build_faiss_index.py (not yet created).
    3. Set kb.backend = "faiss" and kb.file_path = "path/to/index.faiss"
       in your tenant's YAML config.
    4. Implement the body below.
"""
import logging

from services.kb_backends.base import KBBackend

logger = logging.getLogger(__name__)


class FaissBackend(KBBackend):
    """
    Dense retrieval via FAISS index.

    Args:
        index_path: Path to the .faiss index file.
        texts_path: Path to a companion .jsonl file mapping vector IDs → text.
        embedding_model: OpenAI embedding model name.
    """

    def __init__(
        self,
        index_path: str,
        texts_path: str,
        embedding_model: str = "text-embedding-3-small",
    ) -> None:
        self._index_path = index_path
        self._texts_path = texts_path
        self._embedding_model = embedding_model
        logger.warning(
            "FaissBackend is a stub — returning empty results until implemented."
        )

    async def search(self, query: str, k: int = 3) -> list[str]:
        logger.debug("FaissBackend.search() called — stub returns []")
        return []
