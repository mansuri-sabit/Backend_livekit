"""
routes/knowledge_base.py — MongoDB RAG Knowledge Base management API.

Prefix: /api/v1/knowledge-base

IMPORTANT: This router serves the MongoDB-backed RAG system ONLY.
It is entirely separate from services/kb_service.py, which serves LiveKit agent
workers via JSONL file backends. These two KB systems must NEVER be merged or
cross-referenced.

  MongoDB RAG KB (this file):
    - Used by Pipecat pipeline and admin dashboard
    - Documents stored in: db.kb_documents
    - Chunks + embeddings stored in: db.kb_chunks
    - Vector search via Atlas index: kb_chunks_vector_index_v2

  LiveKit JSONL KB (services/kb_service.py + agents/kb_hooks.py):
    - Used by LiveKit agent workers
    - Loaded from: config/kb/{tenant_id}/*.jsonl
    - PROTECTED — never modified by this migration

Endpoints:
  POST   /api/v1/knowledge-base/upload                   — upload KB doc (admin, async processing)
  GET    /api/v1/knowledge-base/stats/{agent_id}          — KB stats for an agent
  GET    /api/v1/knowledge-base/document/{document_id}    — get doc details + chunks
  GET    /api/v1/knowledge-base/{agent_id}                — list docs for agent
  DELETE /api/v1/knowledge-base/{document_id}             — soft-delete doc + chunks

  DIY (user-scoped, requires Phase 10 diy_agents collection):
  POST   /api/v1/knowledge-base/diy/upload
  GET    /api/v1/knowledge-base/diy/documents
  DELETE /api/v1/knowledge-base/diy/documents/{document_id}
  GET    /api/v1/knowledge-base/diy/stats
"""
import asyncio
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from config import get_settings
from models import ROLE_ADMIN, ROLE_SUPER_ADMIN
from utils.db import db, require_db
from utils.file_parser import extract_text_from_file
from utils.logger import logger
from utils.rag import chunk_text, delete_chunks_for_item, index_kb_item
from utils.security import get_current_user

router = APIRouter(prefix="/api/v1/knowledge-base", tags=["Knowledge Base"])

MAX_DIY_FILE_SIZE = 100 * 1024 * 1024  # 100 MB


def _require_admin(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        raise HTTPException(status_code=403, detail="Admin access required")


async def _get_diy_agent(user_id: str) -> Optional[dict]:
    """
    Resolve the agent used for DIY KB operations.

    The pipeline resolves agents via get_agent_for_scope → db.agents and uses
    str(agent["_id"]) as the RAG tenant_id for vector search. KB documents and
    chunks must be stored with that same _id.

    Returns the linked db.agents entry (the one the pipeline uses) with
    approval_status mirrored from diy_agents, or the raw diy_agents doc if
    no linked agent exists yet (Phase 10 forward-compat).
    """
    diy = await db.diy_agents.find_one({"user_id": user_id})
    linked = await db.agents.find_one({"user_id": user_id})
    if linked:
        if diy:
            linked["approval_status"] = diy.get("approval_status", "draft")
            linked["_diy_agent_id"] = diy["_id"]
        return linked
    return diy


def _kb_agent_ids(agent: dict) -> list:
    """All possible agent ObjectIds for kb_documents queries (current + legacy)."""
    ids = {agent["_id"]}
    diy_id = agent.get("_diy_agent_id")
    if diy_id:
        ids.add(diy_id)
    return list(ids)


def _kb_tenant_ids(agent: dict) -> list:
    """All possible tenant_id strings for kb_chunks queries."""
    ids = {str(agent["_id"])}
    diy_id = agent.get("_diy_agent_id")
    if diy_id:
        ids.add(str(diy_id))
    return list(ids)


async def _process_upload(
    result_id: ObjectId,
    agent_id: str,
    document_id: str,
    filename: str,
    content: bytes,
    call_direction: str,
) -> None:
    """Background task: extract text → chunk → embed → store in kb_chunks."""
    settings = get_settings()
    api_key = getattr(settings, "OPENAI_API_KEY", None) or ""
    now = datetime.now(timezone.utc)
    try:
        text = extract_text_from_file(filename, content)
        if not text or not text.strip():
            await db.kb_documents.update_one(
                {"_id": result_id},
                {"$set": {"status": "failed", "error": "No text extracted", "processed_at": now}},
            )
            return
        chunks = chunk_text(text)
        if not chunks or not api_key:
            await db.kb_documents.update_one(
                {"_id": result_id},
                {"$set": {
                    "status": "failed",
                    "error": "No chunks or missing OPENAI_API_KEY",
                    "processed_at": now,
                }},
            )
            return
        n = await index_kb_item(
            tenant_id=agent_id,
            kb_item_id=document_id,
            filename=filename,
            content=text,
            api_key=api_key,
            agent_id=agent_id,
            call_direction=call_direction,
        )
        await db.kb_documents.update_one(
            {"_id": result_id},
            {"$set": {
                "status": "ready",
                "total_chunks": n,
                "total_characters": len(text),
                "processed_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[KB] Document processed: {document_id} chunks={n}")
    except Exception as exc:
        logger.exception(f"[KB] Document processing failed: {document_id}")
        await db.kb_documents.update_one(
            {"_id": result_id},
            {"$set": {"status": "failed", "error": str(exc), "processed_at": datetime.now(timezone.utc)}},
        )


# ── Admin routes ──────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_document(
    current_user: dict = Depends(get_current_user),
    file: UploadFile = File(...),
    agentId: str = Form(...),
    callDirection: str = Form("both"),
):
    """
    Upload a KB document (PDF/DOCX/TXT) scoped to an agent.
    Processing (chunking + embedding) runs as a background asyncio task.
    Returns 202 immediately with documentId; poll /document/{id} for status.
    """
    require_db()
    _require_admin(current_user)

    if not agentId or not agentId.strip():
        raise HTTPException(status_code=400, detail="agentId is required")
    if callDirection not in ("inbound", "outbound", "both"):
        raise HTTPException(status_code=400, detail="callDirection must be one of: inbound, outbound, both")

    try:
        oid = ObjectId(agentId)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agentId")

    agent = await db.agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    content = await file.read()
    filename = file.filename or "document"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    file_type = "pdf" if ext == "pdf" else ("docx" if ext in ("docx", "doc") else "txt")

    now = datetime.now(timezone.utc)
    doc = {
        "agent_id": oid,
        "user_id": current_user.get("user_id"),
        "file_name": filename,
        "file_type": file_type,
        "file_size": len(content),
        "call_direction": callDirection,
        "status": "processing",
        "total_chunks": 0,
        "total_tokens": 0,
        "total_characters": 0,
        "uploaded_at": now,
        "processed_at": None,
        "is_active": True,
    }
    result = await db.kb_documents.insert_one(doc)
    document_id = str(result.inserted_id)

    asyncio.create_task(
        _process_upload(result.inserted_id, agentId, document_id, filename, content, callDirection)
    )

    return JSONResponse(
        status_code=202,
        content={
            "success": True,
            "message": "Document upload started. Processing in background.",
            "data": {"documentId": document_id, "fileName": filename, "status": "processing"},
        },
    )


@router.get("/stats/{agent_id}")
async def get_stats(agent_id: str, current_user: dict = Depends(get_current_user)):
    """Get KB stats (document count + chunk count) for an agent."""
    require_db()
    _require_admin(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agentId")

    total_docs = await db.kb_documents.count_documents({"agent_id": oid, "is_active": True})
    total_chunks = await db.kb_chunks.count_documents({"tenant_id": agent_id})
    return {"success": True, "data": {"totalDocuments": total_docs, "totalChunks": total_chunks}}


@router.get("/document/{document_id}")
async def get_document(document_id: str, current_user: dict = Depends(get_current_user)):
    """Get full details + chunks for a KB document."""
    require_db()
    _require_admin(current_user)
    try:
        oid = ObjectId(document_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Document not found")

    doc = await db.kb_documents.find_one({"_id": oid, "is_active": True})
    if not doc:
        raise HTTPException(status_code=404, detail="Knowledge base document not found")

    agent_id_str = str(doc["agent_id"])
    cursor = db.kb_chunks.find(
        {"tenant_id": agent_id_str, "kb_item_id": document_id}
    ).sort("chunk_index", 1)
    chunks = []
    async for c in cursor:
        chunks.append({
            "text": c.get("text", ""),
            "chunkIndex": c.get("chunk_index", 0),
            "metadata": c.get("metadata") or {},
        })

    return {
        "success": True,
        "data": {
            "id": str(doc["_id"]),
            "fileName": doc.get("file_name", ""),
            "fileType": doc.get("file_type", ""),
            "fileSize": doc.get("file_size", 0),
            "callDirection": doc.get("call_direction", "both"),
            "status": doc.get("status", ""),
            "totalChunks": doc.get("total_chunks", 0),
            "totalTokens": doc.get("total_tokens", 0),
            "totalCharacters": doc.get("total_characters", 0),
            "uploadedAt": doc.get("uploaded_at"),
            "processedAt": doc.get("processed_at"),
            "chunks": chunks,
            "error": doc.get("error"),
        },
    }


@router.get("/{agent_id}")
async def list_documents(
    agent_id: str,
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = None,
    callDirection: Optional[str] = None,
):
    """List KB documents for an agent (paginated by upload date, newest first)."""
    require_db()
    _require_admin(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agentId")

    query: dict = {"agent_id": oid, "is_active": True}
    if status:
        query["status"] = status
    if callDirection and callDirection in ("inbound", "outbound", "both"):
        query["call_direction"] = callDirection

    cursor = db.kb_documents.find(query).sort("uploaded_at", -1)
    documents = []
    async for doc in cursor:
        documents.append({
            "id": str(doc["_id"]),
            "fileName": doc.get("file_name", ""),
            "fileType": doc.get("file_type", ""),
            "fileSize": doc.get("file_size", 0),
            "callDirection": doc.get("call_direction", "both"),
            "status": doc.get("status", ""),
            "totalChunks": doc.get("total_chunks", 0),
            "totalTokens": doc.get("total_tokens", 0),
            "totalCharacters": doc.get("total_characters", 0),
            "uploadedAt": doc.get("uploaded_at"),
            "processedAt": doc.get("processed_at"),
            "error": doc.get("error"),
        })

    total_chunks = await db.kb_chunks.count_documents({"tenant_id": agent_id})
    return {
        "success": True,
        "data": {
            "documents": documents,
            "stats": {"totalDocuments": len(documents), "totalChunks": total_chunks},
        },
    }


@router.delete("/{document_id}")
async def delete_document(document_id: str, current_user: dict = Depends(get_current_user)):
    """Soft-delete a KB document and remove all its vector chunks."""
    require_db()
    _require_admin(current_user)
    try:
        oid = ObjectId(document_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Document not found")

    doc = await db.kb_documents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Knowledge base document not found")

    agent_id_str = str(doc["agent_id"])
    await db.kb_documents.update_one(
        {"_id": oid},
        {"$set": {"is_active": False, "deleted_at": datetime.now(timezone.utc)}},
    )
    await delete_chunks_for_item(tenant_id=agent_id_str, kb_item_id=document_id)
    logger.info(f"[KB] Document deleted: {document_id} fileName={doc.get('file_name')}")
    return {"success": True, "message": "Knowledge base document deleted successfully"}


# ── DIY routes (user-scoped, forward-compat with Phase 10 diy_agents) ─────────

@router.post("/diy/upload")
async def diy_upload_document(
    current_user: dict = Depends(get_current_user),
    file: UploadFile = File(...),
    callDirection: str = Form("both"),
):
    """
    Upload a KB document for the current user's own agent (DIY).
    Agent must be in draft or rejected state to allow edits.
    Requires Phase 10 diy_agents collection; returns 404 until then.
    """
    require_db()
    scope = (current_user.get("user_id") or "").strip()
    if not scope:
        raise HTTPException(status_code=401, detail="User scope required")

    agent = await _get_diy_agent(scope)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found. Create a DIY agent first.")

    approval = (agent.get("approval_status") or "draft").lower()
    if approval not in ("draft", "rejected"):
        raise HTTPException(
            status_code=403,
            detail="Cannot upload documents while agent is pending approval or approved",
        )
    if callDirection not in ("inbound", "outbound", "both"):
        raise HTTPException(status_code=400, detail="callDirection must be one of: inbound, outbound, both")

    content = await file.read()
    if len(content) > MAX_DIY_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Max {MAX_DIY_FILE_SIZE // (1024 * 1024)} MB.",
        )
    filename = file.filename or "document"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("pdf", "docx", "doc", "txt"):
        raise HTTPException(status_code=400, detail="Only PDF, DOCX, and TXT files are allowed")
    file_type = "pdf" if ext == "pdf" else ("docx" if ext in ("docx", "doc") else "txt")

    agent_id = str(agent["_id"])
    now = datetime.now(timezone.utc)
    doc = {
        "agent_id": ObjectId(agent_id),
        "user_id": scope,
        "file_name": filename,
        "file_type": file_type,
        "file_size": len(content),
        "call_direction": callDirection,
        "status": "processing",
        "total_chunks": 0,
        "total_tokens": 0,
        "total_characters": 0,
        "uploaded_at": now,
        "processed_at": None,
        "is_active": True,
    }
    result = await db.kb_documents.insert_one(doc)
    document_id = str(result.inserted_id)

    asyncio.create_task(
        _process_upload(result.inserted_id, agent_id, document_id, filename, content, callDirection)
    )

    return JSONResponse(
        status_code=202,
        content={
            "success": True,
            "message": "Document upload started. Processing in background.",
            "data": {"documentId": document_id, "fileName": filename, "status": "processing"},
        },
    )


@router.get("/diy/documents")
async def diy_list_documents(current_user: dict = Depends(get_current_user)):
    """List KB documents for the current user's own agent."""
    require_db()
    scope = (current_user.get("user_id") or "").strip()
    if not scope:
        raise HTTPException(status_code=401, detail="User scope required")

    agent = await _get_diy_agent(scope)
    if not agent:
        return {"success": True, "data": {"documents": [], "stats": {"totalDocuments": 0, "totalChunks": 0}}}

    agent_ids = _kb_agent_ids(agent)
    tenant_ids = _kb_tenant_ids(agent)
    cursor = db.kb_documents.find({"agent_id": {"$in": agent_ids}, "is_active": True}).sort("uploaded_at", -1)
    documents = []
    async for doc in cursor:
        documents.append({
            "id": str(doc["_id"]),
            "fileName": doc.get("file_name", ""),
            "fileType": doc.get("file_type", ""),
            "fileSize": doc.get("file_size", 0),
            "callDirection": doc.get("call_direction", "both"),
            "status": doc.get("status", ""),
            "totalChunks": doc.get("total_chunks", 0),
            "uploadedAt": doc.get("uploaded_at"),
            "processedAt": doc.get("processed_at"),
            "error": doc.get("error"),
        })

    total_chunks = await db.kb_chunks.count_documents({"tenant_id": {"$in": tenant_ids}})
    return {
        "success": True,
        "data": {"documents": documents, "stats": {"totalDocuments": len(documents), "totalChunks": total_chunks}},
    }


@router.delete("/diy/documents/{document_id}")
async def diy_delete_document(document_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a KB document from the current user's own agent."""
    require_db()
    scope = (current_user.get("user_id") or "").strip()
    if not scope:
        raise HTTPException(status_code=401, detail="User scope required")

    agent = await _get_diy_agent(scope)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    approval = (agent.get("approval_status") or "draft").lower()
    if approval not in ("draft", "rejected"):
        raise HTTPException(status_code=403, detail="Cannot delete documents while agent is pending approval or approved")

    try:
        oid = ObjectId(document_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid document ID")

    agent_ids = _kb_agent_ids(agent)
    doc = await db.kb_documents.find_one({"_id": oid, "agent_id": {"$in": agent_ids}, "is_active": True})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    await db.kb_documents.update_one(
        {"_id": oid},
        {"$set": {"is_active": False, "deleted_at": datetime.now(timezone.utc)}},
    )
    await delete_chunks_for_item(tenant_id=str(doc["agent_id"]), kb_item_id=document_id)
    logger.info(f"[KB DIY] Document deleted: {document_id} fileName={doc.get('file_name')}")
    return {"success": True, "message": "Document deleted successfully"}


@router.get("/diy/stats")
async def diy_get_stats(current_user: dict = Depends(get_current_user)):
    """Get KB stats for the current user's own agent."""
    require_db()
    scope = (current_user.get("user_id") or "").strip()
    if not scope:
        raise HTTPException(status_code=401, detail="User scope required")

    agent = await _get_diy_agent(scope)
    if not agent:
        return {"success": True, "data": {"totalDocuments": 0, "totalChunks": 0}}

    agent_ids = _kb_agent_ids(agent)
    tenant_ids = _kb_tenant_ids(agent)
    total_docs = await db.kb_documents.count_documents({"agent_id": {"$in": agent_ids}, "is_active": True})
    total_chunks = await db.kb_chunks.count_documents({"tenant_id": {"$in": tenant_ids}})
    return {"success": True, "data": {"totalDocuments": total_docs, "totalChunks": total_chunks}}
