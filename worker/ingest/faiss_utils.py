# ingest/faiss_utils.py
from pathlib import Path
import faiss

from app.config import HNSW_M, EF_CONSTRUCTION


def create_faiss_index(dim: int) -> faiss.Index:
    """
    Creates an HNSW index wrapped in IndexIDMap.
    """
    base = faiss.IndexHNSWFlat(dim, HNSW_M)
    base.hnsw.efConstruction = EF_CONSTRUCTION
    return faiss.IndexIDMap(base)


def load_faiss_index(path: Path):
    """
    Loads a FAISS index if it exists.
    Returns None if not found.
    """
    if not path.exists():
        return None
    return faiss.read_index(str(path))


def save_faiss_index(index: faiss.Index, path: Path):
    """
    Persists FAISS index to disk.
    """
    faiss.write_index(index, str(path))