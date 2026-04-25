"""
agents/kb_hooks.py

TenantAgent: an Agent subclass that injects KB context into each LLM call.

How it works (livekit-agents 1.x):
  - Optional parallel RAG: voice_agent registers AgentSession.on("user_input_transcribed").
    On each final STT segment, schedule_kb_prefetch() starts embedding + Mongo search
    immediately so retrieval overlaps VAD / pipeline work before llm_node runs.
  - llm_node() uses _retrieve_kb_snippets_for_llm(): if prefetch already finished, snippets
    are injected; if prefetch is still running, the LLM starts without blocking (optional
    KB_PREFETCH_GRACE_SEC wait); cold searches run in the background so the first token is
    not delayed by Mongo.
  - Appends snippets to the system prompt, trims history to MAX_TURNS, then delegates
    to Agent.default.llm_node().

Usage:
    from agents.kb_hooks import TenantAgent

    agent = TenantAgent(
        tenant_id="my_agent_id",
        campaign_id="default",
        instructions=system_prompt,
        vad=..., stt=..., llm=..., tts=...,
    )
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterable

from livekit.agents import Agent
from livekit.agents import llm

from services.kb_service import search_kb

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
_RAG_MIN_WORDS = 1  # Allow single-word queries like "pricing", "address", "email"
# Max time to wait for embedding + Mongo KB search before calling the LLM.
# A 500ms cap caused almost no snippets to be injected (embedding alone often exceeds that).
_RAG_SEARCH_TIMEOUT = float(os.environ.get("KB_SEARCH_TIMEOUT_SEC", "4.0"))
# Optional wait (seconds) for in-flight prefetch before starting the LLM stream (default 0 = no wait).
_KB_PREFETCH_GRACE_SEC = float(os.environ.get("KB_PREFETCH_GRACE_SEC", "0"))
_KB_TOP_K = max(1, min(10, int(os.environ.get("KB_TOP_K", "3"))))
_MAX_TURNS = 20  # Keep last N user+assistant turns (matches reference backend)

_RAG_SKIP_PHRASES: frozenset[str] = frozenset({
    "hello", "hi", "hey", "yes", "no", "ok", "okay", "bye", "goodbye",
    "thank you", "thanks", "good", "fine", "great", "sure", "hmm",
    "haan", "nahi", "theek hai", "accha", "shukriya", "namaste",
    "haa", "ji", "ji haan", "nahi ji", "chaliye", "chalo",
})


def _normalize_for_skip(text: str) -> str:
    """Lowercase, remove punctuation (keep spaces)."""
    if not text:
        return ""
    return re.sub(r"[^\w\s]", "", text.strip().lower())


def _is_query_relevant(query: str) -> bool:
    """Return True if the query is worth searching the KB for."""
    if not query or not query.strip():
        return False
    normalized = _normalize_for_skip(query)
    if normalized in _RAG_SKIP_PHRASES:
        return False
    words = query.strip().split()
    if len(words) < _RAG_MIN_WORDS:
        return False
    return True


def _trim_history(chat_ctx: llm.ChatContext) -> None:
    """
    Trim conversation history to keep only the system message(s) and the last
    MAX_TURNS user+assistant exchanges. Prevents context window bloat on long calls.
    """
    items = chat_ctx.items
    # Find where non-system messages start
    first_non_system = 0
    for i, item in enumerate(items):
        if isinstance(item, llm.ChatMessage) and item.role != "system":
            first_non_system = i
            break

    non_system = items[first_non_system:]
    # Count user messages as "turns"
    user_count = sum(
        1 for m in non_system
        if isinstance(m, llm.ChatMessage) and m.role == "user"
    )
    if user_count <= _MAX_TURNS:
        return

    # Keep system messages + last MAX_TURNS worth of messages
    excess = user_count - _MAX_TURNS
    trimmed_count = 0
    trim_up_to = first_non_system
    for i in range(first_non_system, len(items)):
        item = items[i]
        if isinstance(item, llm.ChatMessage) and item.role == "user":
            trimmed_count += 1
            if trimmed_count >= excess:
                # Also remove the assistant reply after this user message
                trim_up_to = i + 2  # user + assistant
                break
        trim_up_to = i + 1

    if trim_up_to > first_non_system:
        del items[first_non_system:trim_up_to]
        logger.debug(f"[KB] Trimmed history: removed {trim_up_to - first_non_system} old messages")


class TenantAgent(Agent):
    """
    Agent subclass that enriches each LLM call with KB context.

    Extra constructor args (beyond standard Agent args):
        tenant_id:   Tenant identifier — drives KB and config lookup.
        campaign_id: Campaign identifier — reserved for future routing.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        campaign_id: str = "default",
        call_direction: str = "inbound",
        use_direction_specific_kb: bool = False,
        call_sid: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._tenant_id = tenant_id
        self._campaign_id = campaign_id
        self._call_direction = call_direction
        self._use_direction_specific_kb = use_direction_specific_kb
        self._call_sid = call_sid
        self._transcript: list[dict] = []
        self._seen_content_keys: set[str] = set()
        self._persisted_count: int = 0
        # Parallel RAG: prefetch task started from user_input_transcribed (final)
        self._kb_prefetch_task: asyncio.Task | None = None
        self._prefetch_for_text: str = ""

    def schedule_kb_prefetch(self, transcript: str) -> None:
        """
        Start KB retrieval as soon as STT emits a final transcript (parallel RAG).
        Called from AgentSession \"user_input_transcribed\" so embedding + search
        overlap time before llm_node runs.
        """
        text = (transcript or "").strip()
        if not text or not _is_query_relevant(text):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("[KB] Prefetch skipped: no running event loop")
            return

        if self._kb_prefetch_task is not None and not self._kb_prefetch_task.done():
            self._kb_prefetch_task.cancel()

        self._prefetch_for_text = text

        async def _run_prefetch() -> list[str]:
            return await asyncio.wait_for(
                search_kb(
                    self._tenant_id,
                    self._campaign_id,
                    text,
                    k=_KB_TOP_K,
                    call_direction=self._call_direction,
                ),
                timeout=_RAG_SEARCH_TIMEOUT,
            )

        self._kb_prefetch_task = loop.create_task(_run_prefetch())
        logger.debug(f"[KB] Parallel prefetch started for query='{text[:80]}'")

    async def _retrieve_kb_snippets(self, user_text: str) -> tuple[list[str], bool]:
        """
        Return (snippets, timed_out). If a prefetch matches user_text, await it;
        otherwise run a fresh search_kb.
        """
        if not user_text or not _is_query_relevant(user_text):
            return [], False

        ut = user_text.strip()
        if (
            self._kb_prefetch_task is not None
            and self._prefetch_for_text == ut
        ):
            try:
                snippets = await asyncio.wait_for(
                    asyncio.shield(self._kb_prefetch_task),
                    timeout=_RAG_SEARCH_TIMEOUT,
                )
                logger.info(
                    f"[KB] Used parallel prefetch for tenant='{self._tenant_id}' "
                    f"query='{ut[:60]}'"
                )
                return snippets, False
            except asyncio.TimeoutError:
                logger.warning(
                    f"[KB] Prefetch timed out after {_RAG_SEARCH_TIMEOUT}s for "
                    f"tenant='{self._tenant_id}' query='{ut[:60]}'"
                )
                return [], True
            except asyncio.CancelledError:
                logger.debug("[KB] Prefetch task was cancelled — running cold search")
            except Exception as exc:
                logger.debug(f"[KB] Prefetch await failed: {exc}")
            finally:
                self._kb_prefetch_task = None
                self._prefetch_for_text = ""
        else:
            if self._kb_prefetch_task is not None and not self._kb_prefetch_task.done():
                self._kb_prefetch_task.cancel()
            self._kb_prefetch_task = None
            self._prefetch_for_text = ""

        try:
            snippets = await asyncio.wait_for(
                search_kb(
                    self._tenant_id,
                    self._campaign_id,
                    user_text,
                    k=_KB_TOP_K,
                    call_direction=self._call_direction,
                ),
                timeout=_RAG_SEARCH_TIMEOUT,
            )
            return snippets, False
        except asyncio.TimeoutError:
            logger.warning(
                f"[KB] Search timed out after {_RAG_SEARCH_TIMEOUT}s for "
                f"tenant='{self._tenant_id}' query='{user_text[:60]}'"
            )
            return [], True
        except Exception as exc:
            logger.debug(f"[KB] Search failed for tenant='{self._tenant_id}': {exc}")
            return [], False

    async def _retrieve_kb_snippets_for_llm(self, user_text: str) -> tuple[list[str], bool]:
        """
        KB snippets for this turn without blocking the LLM stream on cold search.

        - If parallel prefetch already completed: use it (same as before).
        - If prefetch is still running: optionally wait up to KB_PREFETCH_GRACE_SEC only.
        - Otherwise: start a background search (for cache / overlap) and return no snippets
          so Agent.default.llm_node can start streaming immediately.
        """
        if not user_text or not _is_query_relevant(user_text):
            return [], False

        ut = user_text.strip()

        if self._kb_prefetch_task is not None and self._prefetch_for_text == ut:
            if self._kb_prefetch_task.done():
                return await self._retrieve_kb_snippets(user_text)
            if _KB_PREFETCH_GRACE_SEC > 0:
                try:
                    snippets = await asyncio.wait_for(
                        asyncio.shield(self._kb_prefetch_task),
                        timeout=_KB_PREFETCH_GRACE_SEC,
                    )
                    logger.info(
                        f"[KB] Prefetch won grace window ({_KB_PREFETCH_GRACE_SEC}s) for "
                        f"tenant='{self._tenant_id}' query='{ut[:60]}'"
                    )
                    self._kb_prefetch_task = None
                    self._prefetch_for_text = ""
                    return snippets, False
                except asyncio.TimeoutError:
                    logger.debug(
                        "[KB] Prefetch not ready within grace — starting LLM without KB; "
                        "search continues in background"
                    )
                except Exception as exc:
                    logger.debug(f"[KB] Prefetch grace await failed: {exc}")

            task = self._kb_prefetch_task

            async def _finish_prefetch() -> None:
                try:
                    await asyncio.wait_for(task, timeout=_RAG_SEARCH_TIMEOUT)
                except Exception:
                    pass

            asyncio.create_task(_finish_prefetch())
            self._kb_prefetch_task = None
            self._prefetch_for_text = ""
            return [], False

        async def _cold_kb() -> None:
            try:
                await asyncio.wait_for(
                    search_kb(
                        self._tenant_id,
                        self._campaign_id,
                        user_text,
                        k=_KB_TOP_K,
                        call_direction=self._call_direction,
                    ),
                    timeout=_RAG_SEARCH_TIMEOUT,
                )
            except Exception:
                pass

        asyncio.create_task(_cold_kb())
        return [], False

    def _capture_chat_items(self, chat_ctx: llm.ChatContext) -> None:
        for item in chat_ctx.items:
            if not isinstance(item, llm.ChatMessage):
                continue
            if item.role not in ("user", "assistant"):
                continue
            content = (item.text_content or "").strip()
            if not content:
                continue
            key = f"{item.role}:{content}"
            if key not in self._seen_content_keys:
                self._seen_content_keys.add(key)
                self._transcript.append({
                    "role": item.role,
                    "content": content,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

    def add_utterance(self, role: str, content: str) -> None:
        content = content.strip()
        if not content:
            return
        key = f"{role}:{content}"
        if key not in self._seen_content_keys:
            self._seen_content_keys.add(key)
            self._transcript.append({
                "role": role,
                "content": content,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    async def _persist_transcript(self, transcript_snapshot: list[dict]) -> None:
        if not self._call_sid or not transcript_snapshot:
            return
        try:
            from agents.tenant_config import calls_set_fields_sync

            ok = await asyncio.to_thread(
                calls_set_fields_sync,
                self._call_sid,
                {"transcript": transcript_snapshot},
            )
            if ok:
                self._persisted_count = len(transcript_snapshot)
                logger.debug(
                    f"[KB] Transcript persisted: {len(transcript_snapshot)} turns "
                    f"for call={self._call_sid}"
                )
        except Exception as exc:
            logger.debug(f"[KB] Transcript persist error for call={self._call_sid}: {exc}")

    async def llm_node(
        self,
        chat_ctx: llm.ChatContext,
        tools: list,
        model_settings: Any,
    ) -> AsyncIterable[llm.ChatChunk | str] | Any:
        """
        Override the LLM node to inject KB context and manage history.

        Steps:
          1. Trim conversation history to last MAX_TURNS.
          2. Extract the last user message from chat_ctx.
          3. Check relevance — skip KB for short/conversational phrases.
          4. Await KB search (up to _RAG_SEARCH_TIMEOUT) for relevant snippets.
          5. Append KB snippets to the system prompt when returned.
          6. Delegate to the default LLM node.
        """
        self._capture_chat_items(chat_ctx)
        if self._call_sid and len(self._transcript) > self._persisted_count:
            asyncio.get_running_loop().create_task(
                self._persist_transcript(list(self._transcript))
            )

        # ── 1. Trim old conversation history ──────────────────────────────
        _trim_history(chat_ctx)

        # ── 2. Extract user text ──────────────────────────────────────────
        user_text = _extract_last_user_text(chat_ctx)

        # ── 3-4. KB retrieval (parallel prefetch when available) ────────────
        snippets: list[str] = []
        kb_search_timed_out = False
        if user_text and _is_query_relevant(user_text):
            snippets, kb_search_timed_out = await self._retrieve_kb_snippets_for_llm(user_text)

        # ── 5. Inject KB into system prompt ────────────────────────────────
        if snippets:
            kb_block = (
                "\n\n--- RELEVANT KNOWLEDGE ---\n\n"
                + "\n\n".join(snippets)
            )
            _append_to_system_prompt(chat_ctx, kb_block)

            logger.info(
                f"[KB] Injected {len(snippets)} snippet(s) for "
                f"tenant='{self._tenant_id}' query='{user_text[:60]}'"
            )
            for idx, snip in enumerate(snippets, 1):
                logger.info(f"[KB]   snippet {idx}: {snip[:120]}...")
        elif user_text and _is_query_relevant(user_text) and not kb_search_timed_out:
            logger.info(
                f"[KB] No matching chunks for tenant='{self._tenant_id}' "
                f"query='{user_text[:60]}'"
            )

        if user_text and not _is_query_relevant(user_text):
            logger.debug(
                f"[KB] Skipped (conversational): '{user_text[:60]}'"
            )

        return Agent.default.llm_node(self, chat_ctx, tools, model_settings)


def _extract_last_user_text(chat_ctx: llm.ChatContext) -> str:
    """Return the text content of the last user message, or ''."""
    for item in reversed(chat_ctx.items):
        if isinstance(item, llm.ChatMessage) and item.role == "user":
            return item.text_content or ""
    return ""


def _append_to_system_prompt(chat_ctx: llm.ChatContext, kb_block: str) -> None:
    """
    Append KB context to the first system message in chat_ctx.

    This mirrors the reference backend approach: KB snippets become part of
    the system prompt so the LLM treats them as core knowledge rather than
    a separate injection. The LLM sees:
      [system: persona + KB context] → [history] → [user message]
    """
    for item in chat_ctx.items:
        if isinstance(item, llm.ChatMessage) and item.role == "system":
            # Get current system text
            current_text = item.text_content or ""
            # Remove any previous KB block (from earlier turns) to avoid stacking
            if "--- RELEVANT KNOWLEDGE ---" in current_text:
                current_text = current_text.split("--- RELEVANT KNOWLEDGE ---")[0].rstrip()
            # Append new KB block
            new_text = current_text + kb_block
            # Replace the content
            item.content = [new_text]
            return

    # No system message found — create one with just the KB block
    kb_message = llm.ChatMessage(role="system", content=[kb_block])
    chat_ctx.items.insert(0, kb_message)
