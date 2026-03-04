import sqlite3
from config import JOBS_DB,INGEST_META_DB
# from app.config import JOBS_DB
print(JOBS_DB)
print(JOBS_DB.exists())

def get_db():
    if not JOBS_DB.exists():
        raise RuntimeError(
            f"jobs.db not found at {JOBS_DB}. "
            "Start the worker once to initialize it."
        )
    conn = sqlite3.connect(JOBS_DB)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def get_ingest_db():
    conn = sqlite3.connect(INGEST_META_DB)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()