"""
agents/prompt_builder.py

Builds the system prompt and greeting from external config + Jinja2 templates.

No hardcoded persona, no hardcoded guardrails — everything comes from:
  - The Jinja2 template file (config.prompts.template)
  - The TenantConfig dataclass (company_name, vertical, guardrails, etc.)

Usage:
    from agents.prompt_builder import build_system_prompt, build_greeting
    from agents.tenant_config import get_tenant_config

    cfg = get_tenant_config("my_agent_id")
    system_prompt = build_system_prompt(cfg)
    greeting      = build_greeting(cfg, call_direction="inbound")
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, TemplateNotFound, StrictUndefined

from utils.greeting_service import resolve_runtime_greeting

if TYPE_CHECKING:
    from agents.tenant_config import TenantConfig

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

_JINJA_ENV = Environment(
    loader=FileSystemLoader(str(_PROJECT_ROOT)),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=False,
)


def build_persona_from_fields(persona: dict, agent_name: str = "Assistant") -> str:
    """
    Auto-generate persona text from structured PersonaConfig fields.

    Mirrors the reference backend's build_persona_from_fields() function.
    Used when no explicit system_prompt / config.prompt is configured.

    Args:
        persona: The persona sub-document from the MongoDB agent document.
        agent_name: Fallback name from the agent document.

    Returns:
        A multi-line persona string, or empty string if no fields are set.
    """
    if not persona:
        return ""

    name = persona.get("name") or agent_name or "Assistant"
    company = persona.get("company", "")
    role = persona.get("role", "AI Assistant")
    personality = persona.get("personality", "friendly")
    speaking_style = persona.get("speaking_style", "casual")
    tone = persona.get("tone", "warm")
    response_length = persona.get("response_length", "short")

    parts = []
    parts.append(
        f"You are {name}, a {personality} {role}"
        f"{f' at {company}' if company else ''}."
    )
    parts.append(f"Your tone is {tone}. Your speaking style is {speaking_style}.")

    length_map = {
        "short": "1-2 sentences",
        "medium": "2-3 sentences",
        "detailed": "3-5 sentences",
    }
    parts.append(
        f"Keep responses {length_map.get(response_length, '1-2 sentences')} long."
    )

    dos = persona.get("dos") or []
    if dos:
        parts.append("\nTHINGS YOU MUST ALWAYS DO:")
        for d in dos:
            if isinstance(d, str) and d.strip():
                parts.append(f"- {d.strip()}")

    donts = persona.get("donts") or []
    if donts:
        parts.append("\nTHINGS YOU MUST NEVER DO:")
        for d in donts:
            if isinstance(d, str) and d.strip():
                parts.append(f"- {d.strip()}")

    objection = (persona.get("objection_handling") or "").strip()
    if objection:
        parts.append(f"\nWHEN CUSTOMER OBJECTS OR SAYS NOT INTERESTED:\n{objection}")

    escalation = (persona.get("escalation_trigger") or "").strip()
    if escalation:
        parts.append(f"\nWHEN TO TRANSFER TO HUMAN:\n{escalation}")

    closing = (persona.get("closing_style") or "").strip()
    if closing:
        parts.append(f"\nHOW TO END THE CALL:\n{closing}")

    custom = (persona.get("custom_instructions") or "").strip()
    if custom:
        parts.append(f"\nADDITIONAL INSTRUCTIONS:\n{custom}")

    result = "\n".join(parts)
    return result if result.strip() else ""


def build_system_prompt(cfg: "TenantConfig") -> str:
    """
    Render the system prompt from the tenant's Jinja2 template.

    Template variables injected:
        company_name, vertical, languages, language_code, agent_gender,
        extra_role_context,
        hard_restrictions, forbidden_topics, escalation_keywords
    """
    template_path = cfg.prompts.template

    try:
        template = _JINJA_ENV.get_template(template_path)
    except TemplateNotFound:
        logger.error(
            f"Prompt template not found: '{template_path}'. "
            "Falling back to base_system.j2."
        )
        try:
            template = _JINJA_ENV.get_template("config/prompts/base_system.j2")
        except TemplateNotFound:
            logger.critical(
                "base_system.j2 not found — returning minimal inline fallback prompt."
            )
            return (
                f"You are a helpful AI voice assistant for {cfg.company_name}. "
                f"Help with {cfg.vertical} queries. Be concise. No markdown."
            )

    # Extract primary language code for language/gender rules in template
    language_code = (cfg.locale or "en-IN").split("-")[0].lower()

    # Agent gender — stored in TenantConfig if available
    agent_gender = getattr(cfg, "agent_gender", "female")

    context = {
        "company_name": cfg.company_name,
        "vertical": cfg.vertical,
        "languages": cfg.voice.languages,
        "language_code": language_code,
        "agent_gender": agent_gender,
        "extra_role_context": cfg.prompts.extra_role_context.strip(),
        "hard_restrictions": cfg.guardrails.hard_restrictions,
        "forbidden_topics": cfg.guardrails.forbidden_topics,
        "escalation_keywords": cfg.guardrails.escalation_keywords,
        "transfer_instructions": getattr(cfg.prompts, "transfer_instructions", ""),
        "appointment_instructions": getattr(cfg.prompts, "appointment_instructions", ""),
        "data_collection_instructions": getattr(cfg.prompts, "data_collection_instructions", ""),
        "end_call_phrases": getattr(cfg.prompts, "end_call_phrases", []),
    }

    try:
        rendered = template.render(**context).strip()
        logger.debug(
            f"System prompt built for tenant='{cfg.tenant_id}' "
            f"({len(rendered)} chars, template='{template_path}')"
        )
        return rendered
    except Exception as exc:
        logger.error(
            f"Template render error for tenant='{cfg.tenant_id}': {exc}",
            exc_info=True,
        )
        return (
            f"You are a helpful AI calling agent for {cfg.company_name}. "
            f"Help with {cfg.vertical} queries. Be concise. No markdown."
        )


def build_greeting(
    cfg: "TenantConfig",
    call_direction: str = "inbound",
    contact_name: str = "",
) -> str:
    """
    Build the opening greeting from tenant config.

    The greeting templates in the YAML use {company_name} and {vertical}
    as Python str.format() placeholders — NOT Jinja2 syntax.
    """
    direction = call_direction.lower().strip()

    if direction == "outbound":
        template_str = cfg.greetings.outbound
    else:
        template_str = cfg.greetings.inbound

    # Resolve {{greeting}} and {{name}} BEFORE Python .format() which would
    # mangle double-braces (Python treats {{ as an escaped literal brace).
    greeting = resolve_runtime_greeting(template_str)

    # Resolve {{name}} — use contact_name if provided, otherwise strip placeholder.
    if "{{name}}" in greeting or "{{ name }}" in greeting:
        name_val = contact_name.strip() if contact_name else ""
        greeting = greeting.replace("{{name}}", name_val).replace("{{ name }}", name_val)
        if not name_val:
            greeting = greeting.replace("  ", " ").replace(" ,", ",").replace(" !", "!")

    # Now resolve {company_name} and {vertical} (single-brace Python format placeholders)
    try:
        greeting = greeting.format(
            company_name=cfg.company_name,
            vertical=cfg.vertical,
        )
    except KeyError as exc:
        logger.warning(
            f"Unknown placeholder {exc} in greeting template for tenant='{cfg.tenant_id}'. "
            "Using raw template."
        )

    greeting = " ".join(greeting.split())
    logger.info(
        f"[Greeting] direction='{direction}' tenant='{cfg.tenant_id}' "
        f"source={'direction-specific' if (direction=='outbound' and cfg.greetings.outbound != (cfg.greetings.inbound)) else 'default'} "
        f"→ \"{greeting[:80]}{'...' if len(greeting)>80 else ''}\""
    )
    return greeting
