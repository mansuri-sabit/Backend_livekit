"""
routes/agents.py — Tenant-scoped agent CRUD + KB file management.

Prefix: /api/agents

Endpoints:
  POST   /api/agents/                                     — create agent
  GET    /api/agents/{tenant_id}                          — get agent by tenant
  PUT    /api/agents/{tenant_id}                          — upsert agent
  GET    /api/agents/{tenant_id}/persona                  — get persona sub-doc
  PUT    /api/agents/{tenant_id}/persona                  — update persona sub-doc
  POST   /api/agents/{tenant_id}/knowledge-base/upload    — upload KB files (TXT/PDF/DOCX)
  GET    /api/agents/{tenant_id}/knowledge-base/{item_id}/content — view KB item text
  DELETE /api/agents/{tenant_id}/knowledge-base           — clear all KB items
  DELETE /api/agents/{tenant_id}/knowledge-base/{item_id} — delete one KB item
  POST   /api/agents/migrate-strip-kb-content             — one-time migration helper

NOTE: This router manages agents keyed by tenant_id (Pipecat pipeline format).
The admin dashboard uses /api/v1/agents (admin_agents.py) which keys by MongoDB _id.
Both collections share db.agents but use different document schemas.
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from loguru import logger

from config import get_settings
from models import Agent, PersonaConfig
from utils.db import db, require_db
from utils.file_parser import ALLOWED_EXTENSIONS, extract_text_from_file
from utils.rag import delete_chunks_for_item, get_item_content, index_kb_item

router = APIRouter(prefix="/api/agents", tags=["Agents"])
settings = get_settings()


@router.post("/")
async def create_agent(agent: Agent):
    """Create a new agent with persona."""
    require_db()
    agent.created_at = datetime.now(timezone.utc)
    agent.updated_at = datetime.now(timezone.utc)
    if not agent.agent_id:
        agent.agent_id = str(uuid.uuid4())[:8]
    await db.agents.insert_one(agent.model_dump())
    return {"status": "created", "agent_id": agent.agent_id}


@router.get("/{tenant_id}")
async def get_agent(tenant_id: str):
    """Get agent by tenant ID."""
    require_db()
    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        raise HTTPException(404, "Agent not found")
    agent.pop("_id", None)
    return agent


@router.put("/{tenant_id}")
async def update_agent(tenant_id: str, agent: Agent):
    """Update or create agent (upsert). knowledge_base_items managed by upload/delete endpoints only."""
    require_db()
    agent.tenant_id = tenant_id
    agent.updated_at = datetime.now(timezone.utc)
    data = agent.model_dump(exclude={"created_at", "knowledge_base_items"})
    if not agent.agent_id:
        data["agent_id"] = str(uuid.uuid4())[:8]
    result = await db.agents.update_one(
        {"tenant_id": tenant_id},
        {"$set": data, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    try:
        from utils.db import redis_client, AGENT_CACHE_PREFIX
        if redis_client:
            await redis_client.delete(f"{AGENT_CACHE_PREFIX}{tenant_id}")
    except Exception as e:
        logger.warning("Agent cache invalidation failed (Redis): %s", e)
    status = "created" if result.upserted_id else "updated"
    return {"status": status}


@router.get("/{tenant_id}/persona")
async def get_persona(tenant_id: str):
    """Get just the persona config sub-document."""
    require_db()
    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        raise HTTPException(404, "Agent not found")
    return agent.get("persona", {})


@router.put("/{tenant_id}/persona")
async def update_persona(tenant_id: str, persona: PersonaConfig):
    """Update just the persona sub-document."""
    require_db()
    result = await db.agents.update_one(
        {"tenant_id": tenant_id},
        {"$set": {"persona": persona.model_dump(), "updated_at": datetime.now(timezone.utc)}},
    )
    if result.matched_count == 0:
        raise HTTPException(404, "Agent not found")
    try:
        from utils.db import redis_client, AGENT_CACHE_PREFIX
        if redis_client:
            await redis_client.delete(f"{AGENT_CACHE_PREFIX}{tenant_id}")
    except Exception as e:
        logger.warning("Agent cache invalidation failed (Redis): %s", e)
    return {"status": "persona updated"}


@router.post("/{tenant_id}/knowledge-base/upload")
async def upload_knowledge_base(tenant_id: str, files: list[UploadFile] = File(...)):
    """
    Upload files (TXT, PDF, DOCX) to the agent's knowledge base.
    Agent doc stores only metadata (id, filename, uploaded_at).
    Content is chunked and embedded into kb_chunks collection for vector search.
    """
    require_db()
    if not files:
        raise HTTPException(400, "No files provided")

    new_items = []
    content_by_id: dict = {}

    for f in files:
        if not f.filename:
            continue
        ext = Path(f.filename).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(400, f"Unsupported format: {ext}. Use .txt, .pdf, or .docx")
        content = await f.read()
        try:
            text = extract_text_from_file(f.filename, content)
            if text:
                item_id = str(uuid.uuid4())[:12]
                new_items.append({
                    "id": item_id,
                    "filename": f.filename,
                    "uploaded_at": datetime.now(timezone.utc),
                })
                content_by_id[item_id] = text
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    if not new_items:
        raise HTTPException(400, "No text could be extracted from the provided files")

    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        agent = {
            "tenant_id": tenant_id,
            "agent_id": str(uuid.uuid4())[:8],
            "name": "Agent",
            "persona": {},
            "system_prompt": "",
            "knowledge_base": "",
            "knowledge_base_items": [],
            "voice_id": getattr(settings, "FALLBACK_AGENT_VOICE_ID", "priya"),
            "language": getattr(settings, "FALLBACK_AGENT_LANGUAGE", "en-IN"),
            "greeting": getattr(settings, "FALLBACK_AGENT_GREETING", "Hello! How can I help you today?"),
            "max_concurrent": 100,
            "created_at": datetime.now(timezone.utc),
        }
        await db.agents.insert_one(agent)

    current_items = agent.get("knowledge_base_items") or []
    updated_items = current_items + new_items

    result = await db.agents.update_one(
        {"tenant_id": tenant_id},
        {"$set": {"knowledge_base_items": updated_items, "updated_at": datetime.now(timezone.utc)}},
    )
    if result.matched_count == 0:
        logger.error(f"[KB Upload] No agent found for tenant_id={tenant_id}")
        raise HTTPException(500, f"Agent not found for tenant {tenant_id}")

    logger.info(f"[KB Upload] tenant={tenant_id} added {len(new_items)} items, total={len(updated_items)}")

    api_key = getattr(settings, "OPENAI_API_KEY", None) or ""
    agent_scope = str(agent["_id"])
    chunks_indexed = 0
    for item in new_items:
        n = await index_kb_item(
            agent_scope, item["id"], item["filename"],
            content_by_id.get(item["id"], ""), api_key, agent_id=agent_scope,
        )
        chunks_indexed += n

    return {
        "status": "uploaded",
        "files_processed": len(new_items),
        "items_added": [{"id": x["id"], "filename": x["filename"]} for x in new_items],
        "total_items": len(updated_items),
        "chunks_indexed": chunks_indexed,
    }


@router.get("/{tenant_id}/knowledge-base/{item_id}/content")
async def get_knowledge_base_item_content(tenant_id: str, item_id: str):
    """Retrieve the text content of a KB item from RAG chunks (for dashboard preview)."""
    require_db()
    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        raise HTTPException(404, "Agent not found")
    items = agent.get("knowledge_base_items") or []
    if not any(x.get("id") == item_id for x in items):
        raise HTTPException(404, f"Knowledge base item {item_id} not found")
    content = await get_item_content(str(agent["_id"]), item_id)
    return {"item_id": item_id, "content": content}


@router.delete("/{tenant_id}/knowledge-base")
async def clear_all_knowledge_base(tenant_id: str):
    """Remove all KB items for this tenant (agent metadata + RAG chunks)."""
    require_db()
    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        raise HTTPException(404, "Agent not found")
    agent_scope = str(agent["_id"])
    items = agent.get("knowledge_base_items") or []
    for item in items:
        item_id = item.get("id")
        if item_id:
            await delete_chunks_for_item(agent_scope, item_id)
    await db.agents.update_one(
        {"tenant_id": tenant_id},
        {"$set": {"knowledge_base_items": [], "updated_at": datetime.now(timezone.utc)}},
    )
    logger.info(f"[KB Clear] tenant={tenant_id} agent={agent_scope} removed {len(items)} items")
    return {"status": "cleared", "items_removed": len(items)}


@router.delete("/{tenant_id}/knowledge-base/{item_id}")
async def delete_knowledge_base_item(tenant_id: str, item_id: str):
    """Delete one KB item from the agent and remove its RAG chunks."""
    require_db()
    agent = await db.agents.find_one({"tenant_id": tenant_id})
    if not agent:
        raise HTTPException(404, "Agent not found")

    items = agent.get("knowledge_base_items") or []
    new_items = [x for x in items if x.get("id") != item_id]
    if len(new_items) == len(items):
        raise HTTPException(404, f"Knowledge base item {item_id} not found")

    await db.agents.update_one(
        {"tenant_id": tenant_id},
        {"$set": {"knowledge_base_items": new_items, "updated_at": datetime.now(timezone.utc)}},
    )
    await delete_chunks_for_item(str(agent["_id"]), item_id)
    logger.info(f"[KB Delete] tenant={tenant_id} removed item {item_id}")
    return {"status": "deleted", "item_id": item_id, "remaining_items": len(new_items)}


@router.post("/migrate-strip-kb-content")
async def migrate_strip_kb_content():
    """
    One-time migration: strip 'content' field from all knowledge_base_items in agents collection.
    After migration, agent doc stores only id/filename/uploaded_at; content lives in kb_chunks.
    """
    require_db()
    cursor = db.agents.find({"knowledge_base_items": {"$exists": True, "$ne": []}})
    updated = 0
    async for agent in cursor:
        items = agent.get("knowledge_base_items") or []
        if not items:
            continue
        stripped = []
        had_content = False
        for item in items:
            if isinstance(item, dict) and "content" in item:
                had_content = True
            stripped.append({
                k: v for k, v in (item if isinstance(item, dict) else {}).items()
                if k in ("id", "filename", "uploaded_at")
            })
        if had_content:
            await db.agents.update_one(
                {"_id": agent["_id"]},
                {"$set": {"knowledge_base_items": stripped, "updated_at": datetime.now(timezone.utc)}},
            )
            updated += 1
            logger.info(f"[Migrate] Stripped content from agent tenant_id={agent.get('tenant_id', '?')}")
    return {"status": "ok", "agents_updated": updated}
