"""
Seed users for roles 0 (super admin) and 4 (demo).

Run from the Local_Livekit folder:
    python seed_users.py

Required environment variables (set in .env before running):
    SEED_SUPER_ADMIN_EMAIL     — super admin login email
    SEED_SUPER_ADMIN_PASSWORD  — super admin normal password
    SEED_SUPER_PASSWORD_PLAIN  — master "impersonate any user" password
    SEED_DEMO_EMAIL            — demo account email
    SEED_DEMO_PASSWORD         — demo account password

Roles 1 (admin), 2 (reseller), 3 (user) are created via the dashboard by super admin.
Super password: using it with ANY existing user email logs in as that user (impersonation).
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

import bcrypt

# Load dotenv before importing project modules so MONGO_URI etc. are available
from dotenv import load_dotenv
load_dotenv()

from models import ROLE_DEMO, ROLE_SUPER_ADMIN
from utils.db import db


def _hash_password(password: str) -> str:
    """Bcrypt-hash a password. Truncates to 72 bytes to avoid ValueError."""
    raw = password.encode("utf-8")
    if len(raw) > 72:
        raw = raw[:72]
    return bcrypt.hashpw(raw, bcrypt.gensalt()).decode("utf-8")


def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"ERROR: Environment variable {name} is required but not set.", file=sys.stderr)
        sys.exit(1)
    return val


SUPER_ADMIN_EMAIL    = _require_env("SEED_SUPER_ADMIN_EMAIL")
SUPER_ADMIN_PASSWORD = _require_env("SEED_SUPER_ADMIN_PASSWORD")
SUPER_PASSWORD_PLAIN = _require_env("SEED_SUPER_PASSWORD_PLAIN")
DEMO_EMAIL           = _require_env("SEED_DEMO_EMAIL")
DEMO_PASSWORD        = _require_env("SEED_DEMO_PASSWORD")


async def seed():
    if db is None:
        print("ERROR: MongoDB is not connected. Set MONGO_URI in .env and ensure MongoDB is running.", file=sys.stderr)
        sys.exit(1)

    print("Seeding users (0=super admin, 4=demo)...")
    now = datetime.now(timezone.utc)
    SUPER_ADMIN_CONCURRENCY_POOL = 100_000

    # ── Super Admin ───────────────────────────────────────────────────────────
    existing_sa = await db.users.find_one({"email": SUPER_ADMIN_EMAIL})
    if not existing_sa:
        doc = {
            "email": SUPER_ADMIN_EMAIL,
            "password_hash": _hash_password(SUPER_ADMIN_PASSWORD),
            "super_password_hash": _hash_password(SUPER_PASSWORD_PLAIN),
            "role": ROLE_SUPER_ADMIN,
            "credits": 0,
            "concurrency": SUPER_ADMIN_CONCURRENCY_POOL,
            "concurrency_limit": SUPER_ADMIN_CONCURRENCY_POOL,
            "created_at": now,
            "created_by": None,
            "updated_at": now,
        }
        await db.users.insert_one(doc)
        print(f"  Created super admin: {SUPER_ADMIN_EMAIL} (concurrency pool: {SUPER_ADMIN_CONCURRENCY_POOL})")
    else:
        updates = {}
        if "super_password_hash" not in existing_sa:
            updates["super_password_hash"] = _hash_password(SUPER_PASSWORD_PLAIN)
        if existing_sa.get("concurrency_limit") is None:
            updates["concurrency_limit"] = SUPER_ADMIN_CONCURRENCY_POOL
            updates["concurrency"] = SUPER_ADMIN_CONCURRENCY_POOL
            print(f"  Giving super admin initial concurrency pool: {SUPER_ADMIN_CONCURRENCY_POOL}")
        if updates:
            updates["updated_at"] = now
            await db.users.update_one({"email": SUPER_ADMIN_EMAIL}, {"$set": updates})
            print(f"  Updated super admin fields: {list(updates.keys())}")
        else:
            print(f"  Super admin already exists: {SUPER_ADMIN_EMAIL}")

    # ── Demo user ─────────────────────────────────────────────────────────────
    if not await db.users.find_one({"email": DEMO_EMAIL}):
        await db.users.insert_one({
            "email": DEMO_EMAIL,
            "password_hash": _hash_password(DEMO_PASSWORD),
            "role": ROLE_DEMO,
            "created_at": now,
            "created_by": None,
            "updated_at": now,
        })
        print(f"  Created demo: {DEMO_EMAIL}")
    else:
        print(f"  Demo already exists: {DEMO_EMAIL}")

    print("\nDone.")
    print("Super password is stored only on the super admin document.")
    print("Use it with any user's email to log in as that user (impersonation).")


if __name__ == "__main__":
    asyncio.run(seed())
