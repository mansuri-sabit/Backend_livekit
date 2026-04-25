"""
services/kb_backends/none_backend.py

No-op backend for tenants without a knowledge base.
"""
from services.kb_backends.base import KBBackend


class NoneBackend(KBBackend):
    """Returns empty results for all queries."""

    async def search(self, query: str, k: int = 3) -> list[str]:
        return []
