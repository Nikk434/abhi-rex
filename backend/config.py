from pathlib import Path

# project root
BASE_DIR = Path(__file__).resolve().parents[2]

# worker-owned job queue
WORKER_DATA_DIR = BASE_DIR / "worker" / "data"
JOBS_DB = WORKER_DATA_DIR / "jobs.db"

INGEST_META_DB = WORKER_DATA_DIR / "ingest_out" / "ingest_meta.db"
API_TITLE = "Ingest API"