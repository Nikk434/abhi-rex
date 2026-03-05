from fastapi import APIRouter, Depends, HTTPException
import sqlite3
import json

from deps import get_db

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.get("/{job_id}")
def get_job(job_id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()

    cur.execute(
        """
        SELECT
            *
        FROM ingest_jobs
        WHERE id = ?
        """,
        (job_id,)
    )

    row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    job = dict(row)

    # decode result json if present
    if job.get("result_json"):
        try:
            job["result"] = json.loads(job["result_json"])
        except Exception:
            job["result"] = None

    # remove raw column
    job.pop("result_json", None)

    print("++++JOBS+++++",job)
    return job
# {'id': 32, 'payload': '{"video": "D:\\\\ABHI-REX\\\\new_content_detection\\\\FAKE_UPLOAD\\\\Screen Recording 2026-01-11 131818.mp4", "metadata": {"type": "movie", "title": "wefg", "year": 2021}}', 'status': 'running', 'error': None, 'created_at': '2026-03-05 04:57:54', 'started_at': '2026-03-05 04:57:56', 'finished_at': None}