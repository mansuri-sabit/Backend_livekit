"""
Text-based chat endpoint for agents.

Reuses the same persona (system_prompt field from DB agent doc) and
KB (vector_search from utils.rag) as the voice call pipeline — no duplicate logic.
Supports direction-aware simulation (inbound/outbound) with direction-specific greetings.

NOTE: Project A's agent_chat.py imported global_system_prompt and pipecat_pipeline,
both of which are Pipecat artifacts that do not exist in Project B. This version
uses an equivalent local _extract_agent_config() that reads the MongoDB agent document
and a minimal prompt wrapper — matching Project B's DB agent schema (system_prompt field,
PersonaConfig sub-doc with llm_model, etc.).
"""

from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import get_settings
from models import ROLE_ADMIN, ROLE_SUPER_ADMIN, ROLE_RESELLER
from utils.db import db
from utils.logger import logger
from utils.rag import vector_search
from utils.security import get_current_user

router = APIRouter(prefix="/api/v1/agents", tags=["Agent Chat"])


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: List[ChatMessage] = []
    direction: Optional[str] = None  # "inbound" or "outbound"


def _require_agent_access(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Access denied")


async def _fetch_agent(agent_id: str, current_user: dict) -> dict:
    """Fetch agent document with access checks."""
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    agent_doc = await db.agents.find_one({"_id": oid})
    if not agent_doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    if current_user.get("role") == ROLE_RESELLER:
        if agent_doc.get("managed_by") != "reseller" or agent_doc.get("owner_id") != current_user.get("user_id"):
            raise HTTPException(status_code=404, detail="Agent not found")

    return agent_doc


def _extract_agent_config(agent_doc: dict) -> dict:
    """
    Extract a flat config dict from a MongoDB agent document.

    Maps Project B's DB schema (system_prompt, persona sub-doc, etc.)
    to the keys expected by the chat endpoint.
    Direction-specific prompts aren't stored separately in B's schema —
    a single system_prompt is used for all directions (consistent with
    how voice_agent.py uses build_system_prompt).
    """
    persona = agent_doc.get("persona") or {}
    # Prefer flattened top-level fields (set by agents router on save),
    # fall back to nested persona sub-doc keys for backward compatibility.
    llm_model = (
        agent_doc.get("llm_model")
        or (persona.get("llm_model") if isinstance(persona, dict) else None)
        or "gpt-4o-mini"
    )
    llm_temperature = (
        agent_doc.get("llm_temperature")
        or (persona.get("llm_temperature") if isinstance(persona, dict) else None)
        or 0.7
    )
    # Single system_prompt for all directions in Project B
    system_prompt = agent_doc.get("system_prompt") or ""
    language = agent_doc.get("language") or "en-IN"
    greeting = agent_doc.get("greeting") or "Hello! How can I help you today?"
    transfer_settings = agent_doc.get("call_transfer_settings") or {}

    return {
        "prompt": system_prompt,
        "inbound_prompt": system_prompt,
        "outbound_prompt": system_prompt,
        "language": language,
        "llm_model": llm_model,
        "llm_temperature": float(llm_temperature),
        "greeting": greeting,
        "inbound_greeting": greeting,
        "outbound_greeting": greeting,
        "transfer_settings_cfg": transfer_settings,
        "use_direction_specific_kb": bool(agent_doc.get("use_direction_specific_kb", False)),
    }


def _normalize_direction(direction: Optional[str]) -> Optional[str]:
    d = (direction or "").strip().lower()
    return d if d in ("inbound", "outbound") else None


def _build_chat_system_prompt(agent_persona: str, language_code: str) -> str:
    """
    Build a minimal system prompt for text chat from the agent's stored persona.
    Strips phone-call-specific phrasing and adapts for text mode.
    """
    if not agent_persona:
        return (
            f"You are a helpful AI assistant. Respond in the language matching code: {language_code}. "
            "Be concise and professional. Do not use markdown in your responses."
        )
    prompt = agent_persona.replace(
        "You are on a PHONE CALL. Rules:",
        "You are a text-based AI assistant. Rules:",
    )
    return prompt


@router.get("/{agent_id}/chat/greeting")
async def agent_chat_greeting(
    agent_id: str,
    direction: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Return the resolved greeting message for the given direction."""
    _require_agent_access(current_user)
    agent_doc = await _fetch_agent(agent_id, current_user)
    agent_cfg = _extract_agent_config(agent_doc)
    direction = _normalize_direction(direction)

    if direction == "inbound":
        raw_greeting = agent_cfg.get("inbound_greeting", "")
    elif direction == "outbound":
        raw_greeting = agent_cfg.get("outbound_greeting", "")
    else:
        raw_greeting = agent_cfg.get("greeting", "")

    # Apply runtime date/time substitutions if greeting_service is available
    greeting = raw_greeting or "Hello! How can I help you today?"
    try:
        from utils.greeting_service import resolve_runtime_greeting
        if raw_greeting:
            greeting = resolve_runtime_greeting(raw_greeting)
    except Exception:
        pass

    return {
        "success": True,
        "data": {
            "greeting": greeting,
            "direction": direction or "default",
        },
    }


@router.post("/{agent_id}/chat")
async def agent_chat(agent_id: str, body: ChatRequest, current_user: dict = Depends(get_current_user)):
    """
    Send a text message to an agent and get a response.

    Uses the agent's persona (system_prompt from DB) and queries the Knowledge Base
    via vector search (same as the voice call pipeline). Supports direction-aware
    simulation: inbound/outbound use the same prompt in Project B.
    """
    _require_agent_access(current_user)
    agent_doc = await _fetch_agent(agent_id, current_user)

    settings = get_settings()
    agent_cfg = _extract_agent_config(agent_doc)
    direction = _normalize_direction(body.direction)

    # All directions use the same prompt in Project B's single-prompt schema
    agent_persona = agent_cfg["prompt"]
    agent_language = agent_cfg["language"]
    llm_model = agent_cfg["llm_model"]
    llm_temperature = agent_cfg["llm_temperature"]
    transfer_settings_cfg = agent_cfg.get("transfer_settings_cfg") or {}
    transfer_enabled = bool(transfer_settings_cfg.get("enabled", False))

    system_prompt = _build_chat_system_prompt(agent_persona, agent_language)

    # Append transfer capability notice if transfer is enabled
    if transfer_enabled:
        transfer_num = transfer_settings_cfg.get("transfer_number", "")
        if transfer_num:
            system_prompt += f"\n\nIf the user explicitly asks to speak with a human agent, let them know you can transfer them."

    user_message = (body.message or "").strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    # KB retrieval via vector search (same as voice pipeline's kb_hooks)
    kb_chunks = []
    openai_key = settings.OPENAI_API_KEY or ""
    use_direction_specific_kb = agent_cfg.get("use_direction_specific_kb", False)
    if openai_key and len(user_message.split()) >= settings.RAG_RELEVANT_MIN_WORDS:
        try:
            kb_chunks = await vector_search(
                tenant_id=str(agent_doc.get("_id")),
                query=user_message,
                api_key=openai_key,
                top_k=5,
                agent_id=str(agent_doc.get("_id")),
                call_direction=direction,
                use_direction_specific_kb=use_direction_specific_kb,
            )
            logger.info(f"[AgentChat] KB returned {len(kb_chunks)} chunks for agent {agent_id} direction={direction}")
        except Exception as e:
            logger.warning(f"[AgentChat] KB search failed: {e}")

    if kb_chunks:
        kb_block = "\n\n--- RELEVANT KNOWLEDGE ---\n\n" + "\n\n".join(kb_chunks)
        system_prompt = system_prompt.rstrip() + "\n\n" + kb_block

    messages = [{"role": "system", "content": system_prompt}]
    for msg in body.history[-20:]:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": user_message})

    try:
        if llm_model.startswith("claude"):
            import anthropic
            api_key = settings.ANTHROPIC_API_KEY or ""
            if not api_key:
                raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")
            client = anthropic.AsyncAnthropic(api_key=api_key)
            response = await client.messages.create(
                model=llm_model,
                max_tokens=1024,
                system=system_prompt,
                messages=[m for m in messages if m["role"] != "system"],
                temperature=llm_temperature,
            )
            reply = response.content[0].text
        else:
            from openai import AsyncOpenAI
            if not openai_key:
                raise HTTPException(status_code=503, detail="OPENAI_API_KEY not configured")
            client = AsyncOpenAI(api_key=openai_key)
            response = await client.chat.completions.create(
                model=llm_model,
                messages=messages,
                temperature=llm_temperature,
                max_tokens=1024,
            )
            reply = response.choices[0].message.content
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[AgentChat] LLM call failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to get response from AI model")

    return {
        "success": True,
        "data": {
            "reply": reply,
            "model": llm_model,
            "kbChunksUsed": len(kb_chunks),
            "direction": direction or "default",
        },
    }
