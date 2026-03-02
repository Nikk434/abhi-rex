# app/job_store.py
import sqlite3
from typing import List, Dict
from pathlib import Path


JOB_STATUSES = ("pending", "running", "done", "failed")

def init_jobs_db(db_path: Path) -> sqlite3.Connection:
    """
    Initialize the ingest_jobs table if it does not exist.
    This MUST be called once on worker startup.
    """
    conn = get_conn(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        )
    """)

    conn.commit()
    return conn

def get_conn(db_path: Path) -> sqlite3.Connection:
    """
    Create a SQLite connection suitable for multi-process access.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        str(db_path),
        timeout=30,               # wait for locks
        check_same_thread=False   # worker safety
    )
    conn.row_factory = sqlite3.Row

    # sane defaults
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    return conn


def fetch_pending_jobs(
    conn: sqlite3.Connection,
    limit: int = 1
) -> List[Dict]:
    """
    Fetch pending jobs in FIFO order.
    """
    cur = conn.execute(
        """
        SELECT *
        FROM ingest_jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (limit,)
    )
    return [dict(row) for row in cur.fetchall()]


def mark_job_running(conn: sqlite3.Connection, job_id: int) -> None:
    _update_job_status(
        conn,
        job_id,
        status="running",
        extra_sql=", started_at = CURRENT_TIMESTAMP"
    )


def mark_job_done(conn: sqlite3.Connection, job_id: int) -> None:
    _update_job_status(
        conn,
        job_id,
        status="done",
        extra_sql=", finished_at = CURRENT_TIMESTAMP"
    )


def mark_job_failed(
    conn: sqlite3.Connection,
    job_id: int,
    error: str
) -> None:
    conn.execute(
        """
        UPDATE ingest_jobs
        SET
            status = 'failed',
            error = ?,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (error, job_id)
    )
    conn.commit()


def _update_job_status(
    conn: sqlite3.Connection,
    job_id: int,
    status: str,
    extra_sql: str = ""
) -> None:
    if status not in JOB_STATUSES:
        raise ValueError(f"Invalid job status: {status}")

    conn.execute(
        f"""
        UPDATE ingest_jobs
        SET status = ? {extra_sql}
        WHERE id = ?
        """,
        (status, job_id)
    )
    conn.commit()