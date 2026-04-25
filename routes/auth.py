"""
routes/auth.py — Dashboard authentication.

Endpoints:
  POST /auth/login         — email+password → JWT (or OTP challenge for non-admin roles)
  POST /auth/verify-otp    — confirm 6-digit OTP → JWT
  POST /auth/resend-otp    — resend OTP (max 3 times, 60s cooldown)

OTP store is in-process (dict). Fine for single-process deployment; for multi-process
scale-out, replace with Redis-backed store using redis_client from utils.db.
"""
import secrets
import time
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from config import get_settings
from models import ROLE_ADMIN, ROLE_DEMO, ROLE_RESELLER, ROLE_SUPER_ADMIN, ROLE_USER
from utils.db import db, require_db
from utils.logger import logger
from utils.password import verify_password
from utils.rate_limit import limiter
from utils.security import create_access_token

router = APIRouter(prefix="/auth", tags=["Auth"])

# In-memory OTP store — { email: { otp, expires_at, user_id, role, user_email, resend_count, last_sent_at } }
_otp_store: dict = {}


class LoginBody(BaseModel):
    email: str
    password: str
    dashboard: str = "user"  # "user" or "admin"


class VerifyOTPBody(BaseModel):
    email: str
    otp: str


class ResendOTPBody(BaseModel):
    email: str


def _send_otp_email(settings, to_email: str, otp: str) -> None:
    """Send OTP via Resend. Raises HTTPException(500) on failure."""
    if not settings.RESEND_API_KEY:
        raise HTTPException(status_code=503, detail="Email service not configured (RESEND_API_KEY missing)")
    from_email = (getattr(settings, "RESEND_FROM_EMAIL", None) or "").strip()
    if not from_email:
        raise HTTPException(status_code=503, detail="Email service not configured (RESEND_FROM_EMAIL missing)")
    try:
        import resend
        resend.api_key = settings.RESEND_API_KEY
        resend.Emails.send({
            "from": from_email,
            "to": to_email,
            "subject": "Your login OTP",
            "html": f"""
                <div style="font-family: Arial, sans-serif; max-width: 400px; margin: 0 auto;">
                    <h2>Login Verification</h2>
                    <p>Your OTP for login is:</p>
                    <h1 style="letter-spacing: 8px; color: #4F46E5;">{otp}</h1>
                    <p>This OTP expires in 5 minutes.</p>
                    <p>If you did not request this, ignore this email.</p>
                </div>
            """,
        })
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Failed to send OTP email to {to_email}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to send OTP email.")


@router.post("/login")
@limiter.limit("15/minute")
async def login(request: Request, body: LoginBody):
    """
    Validate email/password and return JWT + user info.

    Roles 0 (super admin) and 4 (demo): direct JWT, no OTP.
    Roles 1-3: OTP challenge sent via Resend email.
    Super password (only on super admin doc): matches any email → impersonation JWT.
    """
    require_db()
    email = body.email.strip().lower()
    password = body.password

    # ── Super-password impersonation ─────────────────────────────────────────
    super_admin = await db.users.find_one(
        {"role": ROLE_SUPER_ADMIN, "super_password_hash": {"$exists": True}}
    )
    if super_admin and super_admin.get("super_password_hash"):
        if verify_password(password, super_admin["super_password_hash"]):
            user_doc = await db.users.find_one({"email": email})
            if not user_doc:
                raise HTTPException(status_code=401, detail="No user with this email")
            user_id = str(user_doc["_id"])
            role = int(user_doc["role"])
            from datetime import timedelta as _td
            from jose import jwt as _jwt
            _settings = get_settings()
            expire = datetime.now(timezone.utc) + _td(
                hours=getattr(_settings, "JWT_ACCESS_TOKEN_EXPIRE_HOURS", 24)
            )
            payload = {
                "sub": user_id,
                "role": role,
                "exp": expire,
                "impersonator": str(super_admin.get("_id")),
            }
            token = _jwt.encode(payload, _settings.JWT_SECRET, algorithm=_settings.JWT_ALGORITHM)
            return {
                "access_token": token,
                "token_type": "bearer",
                "user": {
                    "id": user_id,
                    "email": user_doc["email"],
                    "role": role,
                    "is_diy": bool(user_doc.get("is_diy", False)),
                },
            }

    # ── Normal login ──────────────────────────────────────────────────────────
    user_doc = await db.users.find_one({"email": email})
    if not user_doc:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user_id = str(user_doc["_id"])
    role = int(user_doc["role"])

    # Status + expiry checks (exempt super admin)
    if role != ROLE_SUPER_ADMIN:
        if not user_doc.get("status", True):
            raise HTTPException(status_code=401, detail="Your account is inactive. Please contact support.")
        expiry_str = user_doc.get("status_expires_at")
        if expiry_str:
            try:
                expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) > expiry_dt:
                    raise HTTPException(
                        status_code=401,
                        detail="Your plan has expired. Please renew your subscription.",
                    )
            except (ValueError, TypeError):
                pass

    if not verify_password(password, user_doc["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Dashboard-role gate
    dashboard = body.dashboard.lower()
    if dashboard == "admin" and role not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Access denied. This dashboard is for admins only.")
    if dashboard == "user" and role not in (ROLE_USER, ROLE_DEMO):
        raise HTTPException(status_code=403, detail="Access denied. This dashboard is for users only.")

    # Direct JWT for super admin and demo (no OTP)
    if role in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_DEMO):
        token = create_access_token(user_id=user_id, role=role)
        return {
            "access_token": token,
            "token_type": "bearer",
            "user": {
                "id": user_id,
                "email": user_doc["email"],
                "role": role,
                "is_diy": bool(user_doc.get("is_diy", False)),
            },
        }

    # OTP flow for reseller + user roles
    settings = get_settings()
    otp = str(secrets.randbelow(900000) + 100000)
    now = time.time()
    _otp_store[email] = {
        "otp": otp,
        "expires_at": now + settings.OTP_EXPIRY_SECONDS,
        "user_id": user_id,
        "role": role,
        "user_email": user_doc["email"],
        "resend_count": 0,
        "last_sent_at": now,
    }

    _send_otp_email(settings, user_doc["email"], otp)

    return {
        "otp_required": True,
        "email": user_doc["email"],
        "message": "OTP sent to your email.",
    }


@router.post("/verify-otp")
async def verify_otp(body: VerifyOTPBody):
    """Verify 6-digit OTP and return JWT token."""
    require_db()
    email = body.email.strip().lower()
    otp = body.otp.strip()

    record = _otp_store.get(email)
    if not record:
        raise HTTPException(status_code=400, detail="OTP not found. Please login again.")
    if time.time() > record["expires_at"]:
        del _otp_store[email]
        raise HTTPException(status_code=400, detail="OTP has expired. Please login again.")
    if record["otp"] != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP.")

    del _otp_store[email]

    user_doc = await db.users.find_one({"_id": ObjectId(record["user_id"])})
    is_diy = bool(user_doc.get("is_diy", False)) if user_doc else False

    token = create_access_token(user_id=record["user_id"], role=record["role"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": record["user_id"],
            "email": record["user_email"],
            "role": record["role"],
            "is_diy": is_diy,
        },
    }


@router.post("/resend-otp")
@limiter.limit("5/minute")
async def resend_otp(request: Request, body: ResendOTPBody):
    """Resend OTP. Max 3 resends per session; 60-second cooldown between each."""
    email = body.email.strip().lower()

    record = _otp_store.get(email)
    if not record:
        raise HTTPException(status_code=400, detail="No active OTP session. Please login again.")

    resend_count = record.get("resend_count", 0)
    if resend_count >= 3:
        raise HTTPException(status_code=429, detail="Maximum OTP resend limit reached. Please login again.")

    elapsed = time.time() - record.get("last_sent_at", 0)
    cooldown = 60
    if elapsed < cooldown:
        wait = int(cooldown - elapsed) + 1
        raise HTTPException(status_code=429, detail=f"Please wait {wait} seconds before requesting a new OTP.")

    settings = get_settings()
    otp = str(secrets.randbelow(900000) + 100000)
    now = time.time()
    _otp_store[email] = {
        **record,
        "otp": otp,
        "expires_at": now + settings.OTP_EXPIRY_SECONDS,
        "resend_count": resend_count + 1,
        "last_sent_at": now,
    }

    _send_otp_email(settings, record["user_email"], otp)

    remaining = 3 - (resend_count + 1)
    return {
        "message": "OTP resent to your email.",
        "resends_remaining": remaining,
    }
