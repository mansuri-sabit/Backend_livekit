"""
services/kb_backends/base.py

Abstract base class for all knowledge-base retrieval backends.

To add a new backend:
  1. Subclass KBBackend.
  2. Implement search() and close().
  3. Register it in services/kb_service.py _build_backend().
"""
from abc import ABC, abstractmethod


class KBBackend(ABC):
    """
    Pluggable retrieval backend.  All backends return plain strings so the
    caller never needs to know which backend is in use.
    """

    @abstractmethod
    async def search(self, query: str, k: int = 3) -> list[str]:
        """
        Return the top-k most relevant text snippets for *query*.

        Args:
            query: The user utterance or search string.
            k:     Maximum number of results to return.

        Returns:
            List of plain-text strings (no IDs, no metadata wrappers).
            May be shorter than k if fewer matches exist.
            Returns [] when nothing is relevant.
        """
        ...

    async def close(self) -> None:
        """Release any connections or file handles.  Override if needed."""
        pass
