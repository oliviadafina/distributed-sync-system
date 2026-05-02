"""
conftest.py — Sets required env variables before any src module is imported.
This allows unit tests to run without a running Docker / .env file.
"""
import os

# Provide minimal environment so pydantic-settings doesn't complain
os.environ.setdefault("NODE_ID", "node_test")
os.environ.setdefault("PEERS", "")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
