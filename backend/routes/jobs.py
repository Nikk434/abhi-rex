from fastapi import APIRouter, Depends, HTTPException
import sqlite3

from deps import get_db

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.get("/{job_id}")
def get_job(job_id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute(
        "SELECT id, status, error FROM ingest_jobs WHERE id = ?",
        (job_id,)
    )
    row = cur.fetchone()

    if not row:
        raise HTTPException(404, "Job not found")

    return dict(row)