from typing import List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session


JOB_STATUSES = ("pending", "running", "done", "failed")


def init_jobs_db() -> Session:
    engine = create_engine(
        "postgresql+psycopg2://postgres:nik_admin_434@localhost:5432/postgres",
        pool_pre_ping=True,
    )

    with engine.connect() as conn:

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                status TEXT DEFAULT 'pending',
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """)
        )

        conn.commit()

    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def fetch_pending_jobs(conn: Session, limit: int = 1) -> List[Dict]:
    result = conn.execute(
        text("""
        SELECT *
        FROM jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT :limit
        """),
        {"limit": limit}
    ).mappings().all()

    return [dict(row) for row in result]


def mark_job_running(conn: Session, job_id: int) -> None:
    _update_job_status(
        conn,
        job_id,
        status="running",
        extra_sql=", started_at = CURRENT_TIMESTAMP"
    )


def mark_job_done(conn: Session, job_id: int) -> None:
    _update_job_status(
        conn,
        job_id,
        status="done",
        extra_sql=", finished_at = CURRENT_TIMESTAMP"
    )


def mark_job_failed(conn: Session, job_id: int, error: str) -> None:
    conn.execute(
        text("""
        UPDATE jobs
        SET
            status = 'failed',
            error = :error,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = :job_id
        """),
        {"error": error, "job_id": job_id}
    )
    conn.commit()


def _update_job_status(conn: Session, job_id: int, status: str, extra_sql: str = "") -> None:
    if status not in JOB_STATUSES:
        raise ValueError(f"Invalid job status: {status}")

    conn.execute(
        text(f"""
        UPDATE jobs
        SET status = :status {extra_sql}
        WHERE id = :job_id
        """),
        {"status": status, "job_id": job_id}
    )

    conn.commit()