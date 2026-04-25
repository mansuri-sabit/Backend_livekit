"""TTS greeting utilities: time-based greeting and LLM-generated greeting templates."""
from datetime import datetime
from typing import Optional

from loguru import logger

from config import get_settings


def get_time_based_greeting() -> str:
    """Return 'Good morning', 'Good afternoon', or 'Good evening' based on local time."""
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "Good morning"
    if 12 <= hour < 17:
        return "Good afternoon"
    return "Good evening"


def resolve_runtime_greeting(template: str) -> str:
    """Replace {{greeting}} in template with the current time-based greeting string."""
    if not template:
        return ""
    greeting = get_time_based_greeting()
    return template.replace("{{greeting}}", greeting).replace("{{ greeting }}", greeting)


async def generate_greeting_template(
    persona_text: str, model: str = "gpt-4o-mini"
) -> Optional[str]:
    """
    Use LLM to generate a natural greeting template from the agent's persona description.
    The template will contain '{{greeting}}' as a placeholder for time-based substitution.
    Returns None if OPENAI_API_KEY is not set or the call fails.
    """
    settings = get_settings()
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        logger.warning("[GreetingService] OPENAI_API_KEY not set — cannot generate template")
        return None

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert at writing natural phone call greetings for AI agents. "
                        "Write a single short opening sentence. "
                        "CRITICAL: Start with the placeholder '{{greeting}}'. "
                        "No markdown, no special characters."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Agent Persona: {persona_text}\n\n"
                        "Write a natural opening line (max 15 words) starting with '{{greeting}}'."
                    ),
                },
            ],
            temperature=0.7,
            max_tokens=50,
        )
        template = response.choices[0].message.content.strip()
        if "{{greeting}}" not in template:
            template = "{{greeting}}! " + template
        return template
    except Exception as exc:
        logger.error(f"[GreetingService] Failed to generate greeting template: {exc}")
        return None
