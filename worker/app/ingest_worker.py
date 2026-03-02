# app/ingest_worker.py
import time
import json
import traceback

from app.config import (
    ensure_dirs,
    JOBS_DB,
    INGEST_OUT_DIR,
    INGEST_META_DB,
    FAISS_INDEX_FRAMES,
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


def run_worker(poll_interval: int = 5) -> None:
    # ensure filesystem layout
    ensure_dirs()

    # --- job queue DB (shared with API)
    jobs_conn = init_jobs_db(JOBS_DB)

    # --- ingest metadata DB (worker-only)
    ingest_conn = init_db(INGEST_META_DB)

    # --- FAISS
    faiss_index = load_faiss_index(FAISS_INDEX_FRAMES)

    # --- CLIP
    embedder = CLIPEmbedder()

    print("[WORKER] ingest worker started")

    while True:
        jobs = fetch_pending_jobs(jobs_conn, limit=1)

        if not jobs:
            time.sleep(poll_interval)
            continue

        job = jobs[0]
        job_id = job["id"]

        print(f"[WORKER] picked job {job_id}")
        mark_job_running(jobs_conn, job_id)

        try:
            payload_raw = job["payload"]
            print("RAW PAYLOAD:", repr(payload_raw))

            payload = json.loads(payload_raw)

            faiss_index = ingest_job(
                payload=payload,
                conn=ingest_conn,           # ingest DB only
                faiss_index=faiss_index,
                embedder=embedder,
                out_dir=INGEST_OUT_DIR,
            )

            mark_job_done(jobs_conn, job_id)

        except Exception as e:
            traceback.print_exc()
            mark_job_failed(jobs_conn, job_id, str(e))

        if faiss_index is not None:
            save_faiss_index(faiss_index, FAISS_INDEX_FRAMES)


if __name__ == "__main__":
    run_worker()