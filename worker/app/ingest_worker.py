# app/ingest_worker.py
import time
import json
import traceback

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import (
    ensure_dirs,
    # JOBS_DB,
    INGEST_OUT_DIR,
    # INGEST_META_DB,
    FAISS_INDEX_FRAMES,
    FAISS_INDEX_FRAMES_FLAT
)

from app.job_store import (
    init_jobs_db,
    fetch_pending_jobs,
    mark_job_running,
    mark_job_done,
    mark_job_failed,
)

from ingest.core import ingest_job
from ingest.faiss_utils import load_faiss_index, save_faiss_index
from ingest.clip_embedder import CLIPEmbedder
from ingest.db import init_db
from prototypes.core import build_prototypes
from prototypes.build_prototypes import build_prototypes_flexible


def run_worker(poll_interval: int = 5) -> None:
    ensure_dirs()
    DATABASE_URL = "postgresql+psycopg2://postgres:nik_admin_434@localhost:5432/postgres"
    engine = create_engine(DATABASE_URL)

    Session = sessionmaker(bind=engine)

    jobs_db = init_jobs_db()
    ingest_db = Session()

    init_db(ingest_db)

    faiss_index = load_faiss_index(FAISS_INDEX_FRAMES)
    faiss_flat = load_faiss_index(FAISS_INDEX_FRAMES_FLAT)

    embedder = CLIPEmbedder()

    print("[WORKER] ingest worker started")

    while True:
        jobs = fetch_pending_jobs(jobs_db, limit=1)

        if not jobs:
            time.sleep(poll_interval)
            continue

        job = jobs[0]
        job_id = job["id"]

        print(f"[WORKER] picked job {job_id}")
        mark_job_running(jobs_db, job_id)

        try:
            payload = job["payload"]
            print("RAW PAYLOAD:", payload)

            # payload = json.loads(payload)

            metadata = payload.get("metadata", {})
            content_type = metadata.get("type", "episode")
            content_id = metadata.get("title")  # or generate a unique ID

            # Check if content already exists, else insert
            existing = ingest_db.execute(
                text("SELECT 1 FROM content WHERE content_id = :content_id"),
                {"content_id": content_id}
            ).fetchone()

            if not existing:
                ingest_db.execute(
                    text("""INSERT INTO content (content_id, type, title, year)
                    VALUES (:content_id, :type, :title, :year)"""),
                    {
                        "content_id": content_id,
                        "type": content_type,
                        "title": content_id,
                        "year": metadata.get("year")
                    }
                )
                ingest_db.commit()
                print(f"[WORKER] inserted content {content_id} into DB")

            faiss_index, faiss_flat = ingest_job(
                payload=payload,
                conn=ingest_db,
                faiss_index=faiss_index,
                faiss_flat=faiss_flat,
                embedder=embedder,
                out_dir=INGEST_OUT_DIR,
            )

            

            proto_count = build_prototypes_flexible(
                out_dir=INGEST_OUT_DIR,
                content_type=payload.get("metadata", {}).get("type", "episode"),
            )

            print(f"[WORKER] built {proto_count} prototypes")

            mark_job_done(jobs_db, job_id)

        except Exception as e:
            traceback.print_exc()
            mark_job_failed(jobs_db, job_id, str(e))

        if faiss_index is not None:
            save_faiss_index(faiss_index, FAISS_INDEX_FRAMES)

        if faiss_flat is not None:
            save_faiss_index(faiss_flat, FAISS_INDEX_FRAMES_FLAT)

if __name__ == "__main__":
    run_worker()