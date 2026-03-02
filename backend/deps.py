import sqlite3
from config import JOBS_DB,INGEST_META_DB

def get_db():
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