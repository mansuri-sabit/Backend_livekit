"""
Demo panel: default female/male voice from db.settings (demo_voice) with config fallback.
Use for GET /api/demo/tts-config and anywhere demo default voice is needed.
"""

from config import get_settings


DEMO_VOICE_SETTINGS_ID = "demo_voice"


async def get_demo_voice_config(db):
    """
    Return default demo voice config from db.settings document _id "demo_voice".
    Schema: { "female": { "provider": "sarvam", "voiceId": "priya" }, "male": { "provider": "sarvam", "voiceId": "advait" } }
    Falls back to config (SARVAM_DEFAULT_VOICE, SARVAM_FEMALE_VOICE_IDS, SARVAM_V3_SPEAKERS) when DB doc is missing.
    """
    settings = get_settings()
    fallback_female = (getattr(settings, "SARVAM_DEFAULT_VOICE", None) or "priya").strip().lower()
    fallback_male = (getattr(settings, "SARVAM_DEFAULT_MALE_VOICE", None) or "advait").strip().lower()
    female_ids = tuple(
        s.strip().lower()
        for s in (getattr(settings, "SARVAM_FEMALE_VOICE_IDS", "") or "priya").split(",")
        if s.strip()
    )
    v3 = tuple(
        s.strip().lower()
        for s in (getattr(settings, "SARVAM_V3_SPEAKERS", "") or "priya,advait").split(",")
        if s.strip()
    )
    if female_ids:
        fallback_female = female_ids[0]
    male_candidates = [v for v in v3 if v not in female_ids]
    if male_candidates:
        fallback_male = male_candidates[0]

    try:
        doc = await db.settings.find_one({"_id": DEMO_VOICE_SETTINGS_ID})
        if not doc:
            return {
                "female": {"provider": "sarvam", "voiceId": fallback_female},
                "male": {"provider": "sarvam", "voiceId": fallback_male},
            }
        female = doc.get("female") or {}
        male = doc.get("male") or {}
        return {
            "female": {
                "provider": (female.get("provider") or "sarvam").strip().lower(),
                "voiceId": (female.get("voiceId") or "").strip() or fallback_female,
            },
            "male": {
                "provider": (male.get("provider") or "sarvam").strip().lower(),
                "voiceId": (male.get("voiceId") or "").strip() or fallback_male,
            },
        }
    except Exception:
        return {
            "female": {"provider": "sarvam", "voiceId": fallback_female},
            "male": {"provider": "sarvam", "voiceId": fallback_male},
        }
