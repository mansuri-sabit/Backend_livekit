"""Bcrypt password hashing/verification. Direct bcrypt usage avoids passlib/bcrypt 4.1+ incompatibility."""
import bcrypt


def _truncate_72(password: str) -> bytes:
    """bcrypt silently truncates at 72 bytes — enforce it explicitly."""
    raw = password.encode("utf-8")
    return raw[:72] if len(raw) > 72 else raw


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_truncate_72(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash:
        return False
    raw = _truncate_72(password)
    h = stored_hash.encode("utf-8") if isinstance(stored_hash, str) else stored_hash
    try:
        return bcrypt.checkpw(raw, h)
    except Exception:
        return False
