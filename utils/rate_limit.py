"""
SlowAPI rate limiter instance.
Import `limiter` here; attach to FastAPI app in main.py.
config_filename="" prevents SlowAPI from reading .env (encoding issues on Windows).
"""
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, config_filename="")
