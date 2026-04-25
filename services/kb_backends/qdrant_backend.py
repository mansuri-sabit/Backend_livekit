"""
services/kb_backends/qdrant_backend.py

Qdrant vector-search backend stub.

To activate:
    1. pip install qdrant-client openai
    2. Create a Qdrant collection and upload embeddings.
    3. Set in tenant YAML:
         kb:
           backend: qdrant
           collection: your_collection_name
           qdrant_url: https://your-cluster.qdrant.io
           qdrant_api_key: your_key
    4. Implement below.
"""
import logging

from services.kb_backends.base import KBBackend

logger = logging.getLogger(__name__)


class QdrantBackend(KBBackend):
    """
    Dense retrieval via Qdrant.

    Args:
        collection: Qdrant collection name.
        qdrant_url: URL of the Qdrant instance.
        qdrant_api_key: API key (None for local unauthenticated instance).
        embedding_model: OpenAI embedding model name.
    """

    def __init__(
        self,
        collection: str,
        qdrant_url: str,
        qdrant_api_key: str | None = None,
        embedding_model: str = "text-embedding-3-small",
    ) -> None:
        self._collection = collection
        self._qdrant_url = qdrant_url
        self._qdrant_api_key = qdrant_api_key
        self._embedding_model = embedding_model
        logger.warning(
            "QdrantBackend is a stub — returning empty results until implemented."
        )

    async def search(self, query: str, k: int = 3) -> list[str]:
        logger.debug("QdrantBackend.search() called — stub returns []")
        return []

    async def close(self) -> None:
        pass
