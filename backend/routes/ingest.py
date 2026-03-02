from fastapi import APIRouter, Depends, HTTPException
import json
import sqlite3

from model import IngestRequest, JobResponse
from deps import get_db, get_ingest_db

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("", response_model=JobResponse)
def enqueue_ingest(
    req: IngestRequest,
    db: sqlite3.Connection = Depends(get_db)
):
    payload = json.dumps(req.model_dump())

    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO ingest_jobs (payload, status)
        VALUES (?, 'pending')
        """,
        (payload,)
    )
    db.commit()

    return {
        "job_id": cur.lastrowid,
        "status": "pending"
    }

@router.get("/{job_id}/result")
def ingest_result(
    job_id: int,
    jobs_db: sqlite3.Connection = Depends(get_db),
    ingest_db: sqlite3.Connection = Depends(get_ingest_db),
):
    # 1. check job status
    cur = jobs_db.execute(
        "SELECT status, payload FROM ingest_jobs WHERE id = ?",
        (job_id,)
    )
    job = cur.fetchone()

    if not job:
        raise HTTPException(404, "Job not found")

    if job["status"] != "done":
        return {
            "job_id": job_id,
            "status": job["status"],
            "vectors": None
        }

    # 2. extract content_id
    payload = json.loads(job["payload"])
    content_id = payload.get("content_id")

    if not content_id:
        raise HTTPException(
            400,
            "Job payload has no content_id; cannot compute vectors"
        )

    # 3. count vectors
    cur = ingest_db.execute(
        "SELECT COUNT(*) AS cnt FROM vector_mapping WHERE content_id = ?",
        (content_id,)
    )
    row = cur.fetchone()

    return {
        "job_id": job_id,
        "status": "done",
        "vectors": row["cnt"]
    }