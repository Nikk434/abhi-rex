from fastapi import APIRouter, Depends, HTTPException
import json
from sqlalchemy.orm import Session
from sqlalchemy import text

from model import IngestRequest, JobResponse
from deps import get_db

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("", response_model=JobResponse)
def enqueue_ingest(
    req: IngestRequest,
    db: Session = Depends(get_db)
):
    payload = json.dumps(req.model_dump())

    result = db.execute(
        text("""
        INSERT INTO jobs (payload, status)
        VALUES (:payload, 'pending')
        RETURNING id
        """),
        {"payload": payload}
    )

    job_id = result.scalar()
    db.commit()

    return {
        "job_id": job_id,
        "status": "pending"
    }

@router.get("/{job_id}/result")
def ingest_result(
    job_id: int,
    db: Session = Depends(get_db),
):
    job = db.execute(
        text("""
        SELECT status, payload
        FROM jobs
        WHERE id = :job_id
        """),
        {"job_id": job_id}
    ).mappings().first()

    if not job:
        raise HTTPException(404, "Job not found")

    if job["status"] != "done":
        return {
            "job_id": job_id,
            "status": job["status"],
            "vectors": None
        }

    payload = json.loads(job["payload"])
    content_id = payload.get("content_id")

    if not content_id:
        return {
            "job_id": job_id,
            "status": "done",
            "vectors": None
        }

    row = db.execute(
        text("""
        SELECT COUNT(*) AS cnt
        FROM ingest.vectors
        WHERE content_id = :content_id
        """),
        {"content_id": content_id}
    ).mappings().first()

    return {
        "job_id": job_id,
        "status": "done",
        "vectors": row["cnt"]
    }