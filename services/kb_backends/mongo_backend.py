"""
services/kb_backends/mongo_backend.py

MongoDB KB backend for the LiveKit voice agent.

Bridges the voice agent's KB system (services/kb_service.py) to the
dashboard-uploaded MongoDB RAG KB (utils/rag.py / kb_chunks collection).

How it works:
  - KB documents uploaded via the admin dashboard are chunked, embedded,
    and stored in db.kb_chunks (see utils/rag.py / routes/knowledge_base.py).
  - This backend calls vector_search() using the agent's MongoDB _id as the
    scope key, so each agent only sees its own KB chunks.
  - Supports direction-specific search (inbound/outbound) when the agent
    has use_direction_specific_kb enabled.

Search strategy (two-tier):
  1. Atlas Vector Search ($vectorSearch) — fast, requires Atlas index.
  2. In-process cosine similarity fallback — works without any Atlas index.
     Loads all chunk embeddings for the agent into memory on first call,
     then computes cosine similarity in Python. Good for small-to-medium
     KBs (< 1000 chunks). Used automatically when the Atlas index is missing.

Event-loop safety:
  Motor (AsyncIOMotorClient) binds to the asyncio event loop at creation time
  on the FastAPI main process. The LiveKit agent worker runs in a spawned
  subprocess with a different event loop, which causes
  "Future attached to a different loop" errors when Motor is used there.

  This backend avoids that by using the synchronous pymongo client
  (agents.tenant_config._PYMONGO_CLIENT) inside run_in_executor().
"""
import asyncio
import logging
import math
import os
from typing import Optional

from services.kb_backends.base import KBBackend

logger = logging.getLogger(__name__)

# Cached chunk embeddings for cosine-similarity fallback.
# Key: agent_id → list of (text, embedding, call_direction)
_CHUNK_CACHE: dict[str, list[tuple[str, list[float], str]]] = {}
_CHUNK_CACHE_TS: dict[str, float] = {}
_CHUNK_CACHE_TTL: float = 300.0  # 5 minutes — reduces cold cache loads per call
_USE_KB_CACHE: bool = os.environ.get("USE_CACHE", "true").lower() in ("true", "1", "yes")


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class MongoKBBackend(KBBackend):
    """
    KB backend that queries MongoDB Atlas Vector Search with an
    in-process cosine similarity fallback for when the Atlas index
    is not available.
    """

    def __init__(
        self,
        agent_id: str,
        use_direction_specific: bool = False,
    ) -> None:
        self._agent_id = agent_id
        self._use_direction_specific = use_direction_specific
        self._openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        self._atlas_index_available: Optional[bool] = None  # None = not checked yet

    async def search(self, query: str, k: int = 3) -> list[str]:
        """Default search — uses 'both' direction (no direction filtering)."""
        return await self.search_directed(query, k=k, call_direction="both")

    async def search_directed(
        self,
        query: str,
        k: int = 3,
        call_direction: str = "both",
    ) -> list[str]:
        """
        Direction-aware KB search.

        Tries Atlas Vector Search first; falls back to in-process cosine
        similarity when the index is missing.
        """
        if not query or not query.strip():
            return []

        api_key = self._openai_api_key or os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            try:
                from config import get_settings
                api_key = get_settings().OPENAI_API_KEY or ""
            except Exception:
                pass
        if not api_key:
            logger.warning(
                "[MongoKB] OPENAI_API_KEY not available — skipping KB search "
                f"for agent={self._agent_id}"
            )
            return []

        from agents.tenant_config import _PYMONGO_CLIENT, _PYMONGO_DB_NAME
        if _PYMONGO_CLIENT is None or not _PYMONGO_DB_NAME:
            logger.warning(
                "[MongoKB] Synchronous pymongo client not available — skipping KB search."
            )
            return []

        # Get query embedding
        try:
            from utils.rag import get_embedding, VECTOR_INDEX_NAME, MIN_SCORE, EMBEDDING_DIM
            emb = await get_embedding(query.strip()[:4000], api_key)
            if not emb or len(emb) != EMBEDDING_DIM:
                return []
        except Exception as exc:
            logger.error(f"[MongoKB] embedding failed for agent={self._agent_id}: {exc}")
            return []

        # Resolve direction filter
        effective_direction = "both"
        if self._use_direction_specific and call_direction in ("inbound", "outbound"):
            effective_direction = call_direction

        allowed_dirs: list[str] | None = None
        if effective_direction == "inbound":
            allowed_dirs = ["inbound", "both"]
        elif effective_direction == "outbound":
            allowed_dirs = ["outbound", "both"]

        # Try Atlas Vector Search whenever the index is not known to be missing.
        # NOTE: _atlas_index_available was previously only checked with `is True`,
        # which is never the initial state — so Atlas was never called and every
        # query paid full cosine fallback cost. None / True both attempt Atlas.
        if self._atlas_index_available is not False:
            atlas_results = await self._atlas_vector_search(
                emb, k, allowed_dirs, VECTOR_INDEX_NAME, MIN_SCORE,
                _PYMONGO_CLIENT, _PYMONGO_DB_NAME,
            )
            if atlas_results is None:
                self._atlas_index_available = False
            else:
                self._atlas_index_available = True
                if atlas_results:
                    logger.info(
                        f"[MongoKB] Atlas search: {len(atlas_results)} chunk(s) "
                        f"agent={self._agent_id} dir={call_direction}"
                    )
                    return atlas_results
                # Index responded but no matches above MIN_SCORE — cosine may still find chunks.

        # Cosine fallback: missing Atlas index, or Atlas returned no rows.
        results = await self._cosine_fallback_search(
            emb, k, allowed_dirs, MIN_SCORE,
            _PYMONGO_CLIENT, _PYMONGO_DB_NAME,
        )

        # Direction-aware fallback: if direction-specific search returned 0 results,
        # retry without direction filter so outbound-only chunks are still available
        # for inbound calls (and vice versa). Better to return cross-direction
        # results than nothing at all.
        if not results and allowed_dirs is not None:
            logger.info(
                f"[MongoKB] No chunks for dir={allowed_dirs} — retrying without direction filter"
            )
            results = await self._cosine_fallback_search(
                emb, k, None, MIN_SCORE,
                _PYMONGO_CLIENT, _PYMONGO_DB_NAME,
            )

        if results:
            logger.info(
                f"[MongoKB] Cosine fallback: {len(results)} chunk(s) "
                f"agent={self._agent_id} dir={call_direction}"
            )
        return results

    async def _atlas_vector_search(
        self,
        emb: list[float],
        k: int,
        allowed_dirs: list[str] | None,
        index_name: str,
        min_score: float,
        client: "Any",
        db_name: str,
    ) -> list[str] | None:
        """
        Run Atlas $vectorSearch. Returns list of texts on success,
        or None if the index doesn't exist (caller should fall back).
        """
        scope = self._agent_id
        match_stage: dict = {"score": {"$gte": min_score}}
        if allowed_dirs is not None:
            match_stage["call_direction"] = {"$in": allowed_dirs}

        pipeline = [
            {
                "$vectorSearch": {
                    "index": index_name,
                    "path": "embedding",
                    "queryVector": emb,
                    "numCandidates": 300 if allowed_dirs else 100,
                    "limit": k * 3 if allowed_dirs else k,
                    "filter": {"tenant_id": scope},
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$match": match_stage},
            {"$limit": k},
            {"$project": {"text": 1, "_id": 0}},
        ]

        def _run() -> list[str] | None:
            col = client[db_name]["kb_chunks"]
            try:
                seen: set = set()
                out: list[str] = []
                for doc in col.aggregate(pipeline):
                    t = (doc.get("text") or "").strip()
                    if t and t not in seen:
                        seen.add(t)
                        out.append(t)
                return out
            except Exception as exc:
                err_str = str(exc)
                # Detect missing index errors
                if "index not found" in err_str.lower() or "PlanExecutor" in err_str or "27" in str(getattr(exc, 'code', '')):
                    return None  # Signal: index missing
                logger.error(f"[MongoKB] Atlas vector search error: {exc}")
                return None

        try:
            return await asyncio.get_event_loop().run_in_executor(None, _run)
        except Exception as exc:
            logger.error(f"[MongoKB] executor error: {exc}")
            return None

    async def _cosine_fallback_search(
        self,
        query_emb: list[float],
        k: int,
        allowed_dirs: list[str] | None,
        min_score: float,
        client: "Any",
        db_name: str,
    ) -> list[str]:
        """
        In-process cosine similarity search. Loads all chunks for this agent
        into memory (cached) and computes similarity in Python.
        """
        # Load chunks into cache if not already loaded or cache expired
        import time as _time
        cache_age = _time.monotonic() - _CHUNK_CACHE_TS.get(self._agent_id, 0.0)
        if not _USE_KB_CACHE or self._agent_id not in _CHUNK_CACHE or cache_age > _CHUNK_CACHE_TTL:
            await self._load_chunks_into_cache(client, db_name)

        chunks = _CHUNK_CACHE.get(self._agent_id, [])
        if not chunks:
            return []

        # Filter by direction
        if allowed_dirs is not None:
            chunks = [c for c in chunks if c[2] in allowed_dirs]

        # Compute cosine similarity for each chunk
        scored: list[tuple[float, str]] = []
        for text, chunk_emb, _dir in chunks:
            if not chunk_emb:
                continue
            sim = _cosine_similarity(query_emb, chunk_emb)
            if sim >= min_score:
                scored.append((sim, text))

        # Sort by similarity descending, return top-k
        scored.sort(key=lambda x: x[0], reverse=True)

        seen: set = set()
        out: list[str] = []
        for _score, text in scored[:k]:
            if text not in seen:
                seen.add(text)
                out.append(text)

        return out

    async def _load_chunks_into_cache(self, client: "Any", db_name: str) -> None:
        """Load all chunk embeddings for this agent from MongoDB into memory."""
        scope = self._agent_id

        def _load() -> list[tuple[str, list[float], str]]:
            col = client[db_name]["kb_chunks"]
            results = []
            for doc in col.find(
                {"tenant_id": scope},
                {"text": 1, "embedding": 1, "call_direction": 1},
            ):
                text = (doc.get("text") or "").strip()
                emb = doc.get("embedding") or []
                direction = doc.get("call_direction", "both")
                if text and emb:
                    results.append((text, emb, direction))
            return results

        try:
            import time as _time
            chunks = await asyncio.get_event_loop().run_in_executor(None, _load)
            _CHUNK_CACHE[self._agent_id] = chunks
            _CHUNK_CACHE_TS[self._agent_id] = _time.monotonic()
            logger.info(
                f"[MongoKB] Loaded {len(chunks)} chunks into memory for agent={self._agent_id}"
            )
        except Exception as exc:
            logger.error(f"[MongoKB] Failed to load chunks for agent={self._agent_id}: {exc}")
            _CHUNK_CACHE[self._agent_id] = []
