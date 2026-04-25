"""
MongoDB Atlas Vector Search RAG for the Pipecat pipeline KB system.

NOTE: This is the MongoDB-backed RAG system for dashboard-uploaded KB files.
      It is SEPARATE from services/kb_service.py which serves LiveKit agent workers
      via JSONL file backends. These two systems must never be merged.

Requires Atlas Vector Search index: kb_chunks_vector_index_v2
  Collection: kb_chunks
  Field: embedding (1536 dimensions, cosine similarity)
"""
import re
import time
from typing import List, Optional

from loguru import logger

EMBEDDING_DIM = 1536
CHUNK_SIZE = 600
CHUNK_OVERLAP = 100
VECTOR_INDEX_NAME = "kb_chunks_vector_index_v2"
MIN_SCORE = 0.20  # Low threshold — let the LLM decide relevance, not cosine cutoff

# In-process embedding TTL cache — avoids double OpenAI API call when the early
# LLM trigger and the main RAG processor embed the same query within a few seconds.
_emb_cache: dict = {}
_EMB_CACHE_TTL = 30.0


def chunk_text(
    text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP
) -> List[str]:
    """Split text into overlapping fixed-size chunks for embedding."""
    if not text or not text.strip():
        return []
    text = text.strip()
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start = end - overlap if end < len(text) else len(text)
    return chunks


async def get_embedding(
    text: str, api_key: str, model: str = "text-embedding-3-small"
) -> List[float]:
    """Get OpenAI embedding with an in-process TTL cache."""
    if not text or not api_key:
        return []
    cache_key = f"{model}:{text[:200]}"
    now = time.monotonic()
    cached = _emb_cache.get(cache_key)
    if cached:
        emb, ts = cached
        if now - ts < _EMB_CACHE_TTL:
            logger.debug("[RAG] Embedding cache hit")
            return emb
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        r = await client.embeddings.create(input=text[:8000], model=model)
        if r.data:
            emb = r.data[0].embedding
            _emb_cache[cache_key] = (emb, now)
            return emb
    except Exception as exc:
        logger.warning(f"[RAG] Embedding failed: {exc}")
    return []


async def index_kb_item(
    tenant_id: str,
    kb_item_id: str,
    filename: str,
    content: str,
    api_key: str,
    agent_id: Optional[str] = None,
    call_direction: str = "both",
) -> int:
    """
    Chunk, embed, and store a KB item in the kb_chunks collection.
    agent_id scopes chunks to a specific agent when provided.
    call_direction: 'inbound' | 'outbound' | 'both'
    Returns number of chunks indexed.
    """
    from utils.db import db
    from datetime import datetime, timezone

    if db is None:
        logger.warning("[RAG] index_kb_item called but DB is not available")
        return 0
    if not content or not content.strip():
        return 0
    if not api_key:
        logger.warning("[RAG] No OPENAI_API_KEY — skipping indexing")
        return 0
    if call_direction not in ("inbound", "outbound", "both"):
        call_direction = "both"

    scope_tenant = agent_id if agent_id else tenant_id
    chunks = chunk_text(content)
    docs = []
    for i, chunk in enumerate(chunks):
        emb = await get_embedding(chunk, api_key)
        if not emb or len(emb) != EMBEDDING_DIM:
            continue
        docs.append({
            "tenant_id": scope_tenant,
            "kb_item_id": kb_item_id,
            "filename": filename,
            "chunk_index": i,
            "text": chunk,
            "embedding": emb,
            "call_direction": call_direction,
            "indexed_at": datetime.now(timezone.utc),
        })
    if docs:
        await db.kb_chunks.insert_many(docs)
        logger.info(
            f"[RAG] Indexed {len(docs)} chunks for scope={scope_tenant} "
            f"item={kb_item_id} direction={call_direction}"
        )
    return len(docs)


async def get_item_content(tenant_id: str, kb_item_id: str) -> str:
    """Return concatenated chunk text for a KB item (dashboard View button)."""
    from utils.db import db
    if db is None:
        return ""
    cursor = db.kb_chunks.find(
        {"tenant_id": tenant_id, "kb_item_id": kb_item_id}
    ).sort("chunk_index", 1)
    parts = []
    async for doc in cursor:
        t = (doc.get("text") or "").strip()
        if t:
            parts.append(t)
    return "\n\n".join(parts)


async def delete_chunks_for_item(tenant_id: str, kb_item_id: str) -> int:
    """Delete all chunks for a KB item. Returns deleted count."""
    from utils.db import db
    if db is None:
        return 0
    result = await db.kb_chunks.delete_many(
        {"tenant_id": tenant_id, "kb_item_id": kb_item_id}
    )
    if result.deleted_count:
        logger.info(f"[RAG] Deleted {result.deleted_count} chunks for item={kb_item_id}")
    return result.deleted_count


async def vector_search(
    tenant_id: str,
    query: str,
    api_key: str,
    top_k: int = 5,
    num_candidates: int = 100,
    agent_id: Optional[str] = None,
    call_direction: Optional[str] = None,
    use_direction_specific_kb: bool = False,
    min_score_override: Optional[float] = None,
) -> List[str]:
    """
    Embed query and run Atlas Vector Search. Returns deduplicated chunk texts.
    agent_id scopes search to a specific agent's KB when provided.
    use_direction_specific_kb: when True, filters by call_direction (inbound/outbound).
    Returns [] if index is missing, key absent, or search fails.
    """
    if not query or not query.strip() or not api_key:
        return []
    emb = await get_embedding(query.strip()[:4000], api_key)
    if not emb or len(emb) != EMBEDDING_DIM:
        return []

    from utils.db import db
    if db is None:
        return []

    scope = agent_id if agent_id else tenant_id
    effective = "both"
    if use_direction_specific_kb and call_direction in ("inbound", "outbound"):
        effective = call_direction

    search_filter: dict = {"tenant_id": scope}
    allowed_directions: Optional[list] = None
    if effective == "inbound":
        allowed_directions = ["inbound", "both"]
    elif effective == "outbound":
        allowed_directions = ["outbound", "both"]

    num_cands = min(num_candidates, 10000)
    if effective != "both":
        num_cands = max(num_cands, 300)

    min_score = min_score_override if min_score_override is not None else MIN_SCORE
    match_stage: dict = {"score": {"$gte": min_score}}
    if allowed_directions is not None:
        match_stage["call_direction"] = {"$in": allowed_directions}

    pipeline = [
        {
            "$vectorSearch": {
                "index": VECTOR_INDEX_NAME,
                "path": "embedding",
                "queryVector": emb,
                "numCandidates": num_cands,
                "limit": top_k * 3 if allowed_directions else top_k,
                "filter": search_filter,
            }
        },
        {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
        {"$match": match_stage},
        {"$limit": top_k},
        {"$project": {"text": 1, "_id": 0}},
    ]

    try:
        seen: set = set()
        out: List[str] = []
        async for doc in db.kb_chunks.aggregate(pipeline):
            t = (doc.get("text") or "").strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)

        # Fallback: retry with relaxed direction filter if direction-specific returned 0
        if len(out) == 0 and effective != "both":
            fallback_dirs = ["inbound", "both"] if effective == "inbound" else ["outbound", "both"]
            fallback_pipeline = [
                {
                    "$vectorSearch": {
                        "index": VECTOR_INDEX_NAME,
                        "path": "embedding",
                        "queryVector": emb,
                        "numCandidates": 500,
                        "limit": top_k * 3,
                        "filter": {"tenant_id": scope},
                    }
                },
                {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
                {"$match": {"score": {"$gte": min_score}, "call_direction": {"$in": fallback_dirs}}},
                {"$project": {"text": 1, "_id": 0}},
            ]
            try:
                async for doc in db.kb_chunks.aggregate(fallback_pipeline):
                    t = (doc.get("text") or "").strip()
                    if t and t not in seen:
                        seen.add(t)
                        out.append(t)
                        if len(out) >= top_k:
                            break
            except Exception:
                pass

        if out:
            logger.info(f"[RAG] Returned {len(out)} chunks for scope={scope!r}")
        return out

    except Exception as exc:
        logger.warning(f"[RAG] Vector search failed (index may be missing): {exc}")
        return []
