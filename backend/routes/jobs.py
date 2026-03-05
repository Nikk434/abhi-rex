from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

from deps import get_db

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
        SELECT
            *
        FROM jobs
        WHERE id = :job_id
        """),
        {"job_id": job_id}
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    job = dict(row)

    if job.get("result_json"):
        try:
            job["result"] = json.loads(job["result_json"])
        except Exception:
            job["result"] = None

    job.pop("result_json", None)

    # print("++++JOBS+++++", job)
    return job
# {'id': 32, 'payload': '{"video": "D:\\\\ABHI-REX\\\\new_content_detection\\\\FAKE_UPLOAD\\\\Screen Recording 2026-01-11 131818.mp4", "metadata": {"type": "movie", "title": "wefg", "year": 2021}}', 'status': 'running', 'error': None, 'created_at': '2026-03-05 04:57:54', 'started_at': '2026-03-05 04:57:56', 'finished_at': None}