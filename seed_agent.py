"""
Seed a default agent configuration into MongoDB for a given tenant.

Run from the Local_Livekit folder:
    python seed_agent.py

Required environment variable:
    SEED_TENANT_ID  — tenant ID to seed the agent for (e.g. "my_company")

NOTE: proposal_flow is created in Phase 6. Until then, proposal_templates will be
seeded as an empty list. Re-run this script after Phase 6 to populate templates.
"""
import asyncio
import os
import sys

from dotenv import load_dotenv
load_dotenv()

from config import get_settings
from utils.db import db

settings = get_settings()

# proposal_flow is created in Phase 6; make import conditional so seed_agent.py
# works both before and after Phase 6 without changes.
try:
    from proposal_flow import get_proposal_templates as _get_templates
except ImportError:
    def _get_templates(s):
        return []


async def seed():
    if db is None:
        print("ERROR: MongoDB is not connected. Set MONGO_URI in .env and ensure MongoDB is running.", file=sys.stderr)
        sys.exit(1)

    tenant_id = os.environ.get("SEED_TENANT_ID", "").strip()
    if not tenant_id:
        print("ERROR: SEED_TENANT_ID environment variable is required.", file=sys.stderr)
        sys.exit(1)

    print(f"Seeding agent for tenant: {tenant_id}")

    agent_data = {
        "tenant_id": tenant_id,
        "agent_id": "sarah-01",
        "name": "Sarah",
        "persona": {
            "name": "Sarah",
            "company": "AI Platform Demo",
            "role": "Customer Relationship Manager",
            "personality": "friendly",
            "speaking_style": "casual",
            "tone": "warm",
            "response_length": "short",
            "language_mix": "english",
            "dos": [
                "Always greet warmly",
                "Confirm appointment details clearly",
                "Offer to reschedule if needed",
                "Thank the customer at the end",
            ],
            "donts": [
                "Never be pushy or aggressive",
                "Never share other customers' information",
                "Never make promises you can't keep",
            ],
            "objection_handling": "If customer says not interested, politely thank them and offer to call back another time.",
            "escalation_trigger": "If customer asks to speak to a human or manager, say you will transfer them.",
            "closing_style": "Thank the customer, confirm next steps, wish them a good day.",
            "custom_instructions": "",
            "llm_model": getattr(settings, "DEFAULT_LLM_MODEL", "gpt-4o-mini"),
            "tts_model": "bulbul:v3",
            "tts_pace_min": 0.78,
            "tts_pace_max": 1.12,
            "tts_temperature_min": 0.70,
            "tts_temperature_max": 1.00,
            "max_call_duration": 300,
            "transfer_enabled": True,
            "human_agent_number": (getattr(settings, "TRANSFER_HUMAN_AGENT_NUMBER", None) or "").strip(),
            "proposal_enabled": True,
            "proposal_templates": _get_templates(settings),
            "lead_keywords": [
                "interested", "tell me more", "yes please", "send details",
                "pricing", "quote", "buy", "purchase", "sign up",
                "book", "appointment", "haan", "batao",
            ],
        },
        "system_prompt": "",
        "virtual_number": "",
        "voice_id": getattr(settings, "FALLBACK_AGENT_VOICE_ID", "priya"),
        "language": getattr(settings, "FALLBACK_AGENT_LANGUAGE", "en-IN"),
        "greeting": getattr(settings, "FALLBACK_AGENT_GREETING", "Hello! How can I help you today?"),
        "max_concurrent": 100,
        "knowledge_base": "",
    }

    existing = await db.agents.find_one({"tenant_id": tenant_id})
    if existing:
        print("  Updating existing agent...")
        await db.agents.update_one({"tenant_id": tenant_id}, {"$set": agent_data})
    else:
        print("  Creating new agent...")
        await db.agents.insert_one(agent_data)

    print(f"  Agent seeded for tenant: {tenant_id}")
    if not _get_templates(settings):
        print("  NOTE: proposal_templates is empty — re-run after Phase 6 to populate templates.")


if __name__ == "__main__":
    asyncio.run(seed())
