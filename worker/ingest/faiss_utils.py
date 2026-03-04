# ingest/faiss_utils.py
import faiss
import numpy as np
from pathlib import Path
from app.config import HNSW_M, EF_CONSTRUCTION

def create_hnsw_index(dim: int):
    index = faiss.IndexHNSWFlat(dim, HNSW_M)
    index.hnsw.efConstruction = EF_CONSTRUCTION
    return faiss.IndexIDMap(index)


def create_flat_index(dim: int):
    return faiss.IndexIDMap(faiss.IndexFlatIP(dim))

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