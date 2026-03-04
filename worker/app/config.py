# app/config.py
from pathlib import Path

# project root = worker/
BASE_DIR = Path(__file__).resolve().parents[1]

# base data dirs
DATA_DIR = BASE_DIR / "data"
LOG_DIR = DATA_DIR / "logs"

# -------------------------
# JOB QUEUE (shared with API)
# -------------------------
JOBS_DB = DATA_DIR / "jobs.db"

# -------------------------
# INGEST OUTPUT (worker only)
# -------------------------
INGEST_OUT_DIR = DATA_DIR / "ingest_out"
FRAME_DIR = INGEST_OUT_DIR / "frames"

INGEST_META_DB = INGEST_OUT_DIR / "ingest_meta.db"

FAISS_INDEX_FRAMES = INGEST_OUT_DIR / "faiss_index_frames.bin"
FAISS_INDEX_FRAMES_FLAT = INGEST_OUT_DIR / "faiss_index_frames_flat.bin"
FAISS_INDEX_PROTOTYPES = INGEST_OUT_DIR / "faiss_index_prototypes.bin"

# -------------------------
# INGEST PARAMS
# -------------------------
BATCH_SIZE = 64

MIN_BRIGHTNESS = 10.0
MIN_ENTROPY = 3.0
MIN_LAPLACIAN = 20.0
MAX_BLACK_RATIO = 0.85

# -------------------------
# FAISS
# -------------------------
HNSW_M = 32
EF_CONSTRUCTION = 200

# -------------------------
# CLIP
# -------------------------
CLIP_MODEL = "ViT-B/32"

# -------------------------
# VIDEO
# -------------------------
SUPPORTED_EXTS = {".mp4", ".mkv", ".mov", ".avi"}


def ensure_dirs() -> None:
    """
    Create all required runtime directories.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    INGEST_OUT_DIR.mkdir(parents=True, exist_ok=True)
    FRAME_DIR.mkdir(parents=True, exist_ok=True)