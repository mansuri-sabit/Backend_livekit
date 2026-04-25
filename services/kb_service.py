"""
services/kb_service.py

Knowledge-base service.

Responsibilities:
  - Build the correct backend from tenant config (factory pattern).
  - Cache one backend instance per (tenant_id, campaign_id) — avoids
    reloading the JSONL file on every call turn.
  - Expose a single async search() function that the agent hook calls.

Usage:
    from services.kb_service import search_kb

    snippets = await search_kb(tenant_id, campaign_id, user_text, k=3)
"""
import logging
import os
from typing import TYPE_CHECKING

from services.kb_backends.base import KBBackend
from services.kb_backends.none_backend import NoneBackend
from services.kb_backends.file_backend import FileBackend
from services.kb_backends.faiss_backend import FaissBackend
from services.kb_backends.qdrant_backend import QdrantBackend
from services.kb_backends.mongo_backend import MongoKBBackend

if TYPE_CHECKING:
    from agents.tenant_config import KBConfig

logger = logging.getLogger(__name__)

_USE_CACHE: bool = os.environ.get("USE_CACHE", "true").lower() in ("true", "1", "yes")
_BACKEND_CACHE: dict[tuple[str, str], KBBackend] = {}


def _build_backend(kb_cfg: "KBConfig") -> KBBackend:
    """Instantiate the correct backend from a KBConfig object."""
    backend_name = (kb_cfg.backend or "none").lower()

    if backend_name == "none":
        return NoneBackend()

    if backend_name == "file":
        if not kb_cfg.file_path:
            raise ValueError(
                "kb.backend=file requires kb.file_path to be set in tenant config."
            )
        return FileBackend(file_path=kb_cfg.file_path)

    if backend_name == "faiss":
        if not kb_cfg.file_path:
            raise ValueError(
                "kb.backend=faiss requires kb.file_path (index path) in tenant config."
            )
        return FaissBackend(
            index_path=kb_cfg.file_path,
            texts_path=kb_cfg.file_path.replace(".faiss", "_texts.jsonl"),
        )

    if backend_name == "qdrant":
        if not kb_cfg.collection or not kb_cfg.qdrant_url:
            raise ValueError(
                "kb.backend=qdrant requires kb.collection and kb.qdrant_url in tenant config."
            )
        return QdrantBackend(
            collection=kb_cfg.collection,
            qdrant_url=kb_cfg.qdrant_url,
            qdrant_api_key=kb_cfg.qdrant_api_key,
        )

    if backend_name == "mongo":
        agent_id = getattr(kb_cfg, "agent_id", None)
        if not agent_id:
            logger.warning(
                "[KB] backend=mongo requires agent_id in KBConfig — falling back to NoneBackend"
            )
            return NoneBackend()
        return MongoKBBackend(
            agent_id=agent_id,
            use_direction_specific=getattr(kb_cfg, "use_direction_specific", False),
        )

    logger.error(
        f"Unknown KB backend '{backend_name}'. "
        "Falling back to NoneBackend. Check your tenant config."
    )
    return NoneBackend()


def _get_backend(tenant_id: str, campaign_id: str) -> KBBackend:
    """Return a cached backend for (tenant_id, campaign_id).

    Cache key uses the agent's MongoDB _id for mongo backends to prevent
    cross-agent KB collisions when a user has multiple agents. Both agents
    share the same tenant_id/user_id but hold different _id values, so keying
    on tenant_id alone would serve the wrong KB after a config swap.

    get_tenant_config() has its own 30s TTL cache so calling it here on every
    KB search is effectively just a dict lookup.
    """
    from agents.tenant_config import get_tenant_config

    # Load config (cheap — has its own TTL cache).
    cfg = get_tenant_config(tenant_id, campaign_id)

    # Scope cache key to specific agent _id for mongo backends.
    agent_id = getattr(cfg.kb, "agent_id", None)
    if cfg.kb.backend == "mongo" and agent_id:
        cache_key: tuple[str, str] = (agent_id, campaign_id)
    else:
        cache_key = (tenant_id, campaign_id)

    if _USE_CACHE and cache_key in _BACKEND_CACHE:
        return _BACKEND_CACHE[cache_key]

    backend = _build_backend(cfg.kb)

    if _USE_CACHE:
        _BACKEND_CACHE[cache_key] = backend
    logger.info(
        f"KB backend initialised: tenant={tenant_id} campaign={campaign_id} "
        f"backend={cfg.kb.backend} cache_key={cache_key[0]}"
    )
    return backend


async def search_kb(
    tenant_id: str,
    campaign_id: str,
    query: str,
    k: int = 3,
    call_direction: str = "both",
) -> list[str]:
    """
    Retrieve the top-k relevant snippets for *query*.

    Args:
        tenant_id:      Tenant identifier (matches YAML filename or agent scope).
        campaign_id:    Campaign identifier (reserved for future routing).
        query:          The user's utterance.
        k:              Max number of snippets to return.
        call_direction: "inbound" | "outbound" | "both" — used by MongoKBBackend
                        to filter direction-specific KB chunks.

    Returns:
        List of plain-text strings.  Empty list if KB is disabled or no match.
    """
    if not query or not query.strip():
        return []

    try:
        backend = _get_backend(tenant_id, campaign_id)
        # MongoKBBackend supports direction-aware search; other backends use base search()
        if isinstance(backend, MongoKBBackend):
            snippets = await backend.search_directed(query, k=k, call_direction=call_direction)
        else:
            snippets = await backend.search(query, k=k)
        if snippets:
            logger.debug(
                f"KB hit: tenant={tenant_id} query='{query[:60]}' "
                f"→ {len(snippets)} snippet(s)"
            )
        return snippets
    except Exception as exc:
        logger.error(
            f"KB search failed for tenant={tenant_id}: {exc}", exc_info=True
        )
        return []


async def clear_backend_cache() -> None:
    """
    Close all cached backends and clear the cache.
    Call this on shutdown or when tenant configs are reloaded.
    """
    for backend in _BACKEND_CACHE.values():
        try:
            await backend.close()
        except Exception as exc:
            logger.warning(f"Error closing KB backend: {exc}")
    _BACKEND_CACHE.clear()
    logger.info("KB backend cache cleared.")


def clear_backend_cache_sync() -> None:
    """
    Synchronously clear the backend cache (no close() calls).
    Safe to call from sync code (e.g. admin_agents update endpoint).
    Forces the next KB search to rebuild the backend from fresh config.
    """
    _BACKEND_CACHE.clear()
    logger.info("KB backend cache cleared (sync).")
