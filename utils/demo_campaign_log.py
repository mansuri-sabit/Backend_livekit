"""
Demo panel launch log: single file logs/demo_panel_launch.log.

Written when user clicks Launch Campaign (demo panel): Selected Agent Voice,
Industry, both genders voice, Greeting, Knowledge base, Call settings language,
Chat conversation (pending).

When each call ends, the chat conversation is appended to the same file.
"""

import os
from datetime import datetime, timezone

DEMO_PANEL_LOG_FILENAME = "demo_panel_launch.log"


def _log_dir():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")


def _demo_panel_log_path():
    return os.path.join(_log_dir(), DEMO_PANEL_LOG_FILENAME)


def write_demo_panel_launch_on_click(
    selected_agent_voice: str,
    selected_industry: str,
    industry_agent_id: str,
    voice_female: str,
    voice_male: str,
    greeting: str,
    knowledge_base: str,
    call_settings_language: str,
    agent_db_greeting: str = "",
):
    """
    Called when user clicks Launch Campaign on the demo panel.
    Appends one block to logs/demo_panel_launch.log with all panel settings.
    """
    try:
        from utils.logger import logger
        log_dir = _log_dir()
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.abspath(_demo_panel_log_path())
        logger.info(f"[Demo Panel Log] Writing to: {path}")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        def _trunc(s: str) -> str:
            return s[:500] + "..." if s and len(s) > 500 else (s or "(not set)")

        block = [
            "",
            "========== DEMO PANEL — LAUNCH CAMPAIGN ==========",
            f"Timestamp: {ts}",
            f"Selected Agent Voice: {selected_agent_voice or '(not set)'}",
            f"Selected Industry: {selected_industry or '(not set)'}",
            f"Industry Agent Id: {industry_agent_id or '(not set)'}",
            f"Voice (Female): {voice_female or '(not set)'}",
            f"Voice (Male): {voice_male or '(not set)'}",
            f"Greeting (played on call): {_trunc(greeting)}",
        ]
        if agent_db_greeting and agent_db_greeting.strip() != (greeting or "").strip():
            block.append(f"Agent Greeting (DB): {_trunc(agent_db_greeting)}")
        block += [
            f"Selected Knowledge Base: {knowledge_base or '(not set)'}",
            f"Call Settings Language: {call_settings_language or '(not set)'}",
            "Chat conversation: (pending)",
            "==================================================",
        ]
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(block) + "\n")
        logger.info(
            f"[Demo Panel Log] Written to {path} — "
            f"industry={selected_industry or '(none)'} "
            f"agent_id={industry_agent_id or '(none)'} "
            f"kb={knowledge_base or '(none)'}"
        )
    except Exception as e:
        try:
            from utils.logger import logger
            logger.warning(f"[Demo Panel Log] Failed to write: {e}")
        except Exception:
            pass


def append_demo_panel_conversation(call_id: str, transcript: list):
    """Append chat conversation to logs/demo_panel_launch.log when a demo call ends."""
    try:
        path = os.path.abspath(_demo_panel_log_path())
        if not os.path.isfile(path):
            return
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [
            "",
            f"--- Chat conversation (call_id={call_id}) @ {ts} ---",
        ]
        for item in (transcript or []):
            if isinstance(item, dict):
                role = (item.get("role") or "unknown").upper()
                content = (item.get("content") or "").strip()
                if content:
                    lines.append(f"  {role}: {content}")
        lines.append("---")
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception:
        pass


def write_demo_campaign_log(
    call_id: str,
    agent_id: str,
    agent_name: str,
    greeting: str,
    persona: str,
    knowledge_base: str,
    transcript: list,
):
    """
    When a demo campaign call ends: append the conversation to demo_panel_launch.log.
    """
    append_demo_panel_conversation(call_id, transcript if isinstance(transcript, list) else [])
