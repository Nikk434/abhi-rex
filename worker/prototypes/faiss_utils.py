from pathlib import Path
import faiss

from app.config import HNSW_M, EF_CONSTRUCTION


def create_prototype_index(dim: int) -> faiss.Index:
    """
    Create FAISS index for prototype vectors.
    Uses HNSW + IDMap (same family as frame index).
    """
    base = faiss.IndexHNSWFlat(dim, HNSW_M)
    base.hnsw.efConstruction = EF_CONSTRUCTION
    return faiss.IndexIDMap(base)


def load_prototype_index(path: Path):
    """
    Load prototype FAISS index if present.
    Returns None if not found.
    """
    if not path.exists():
        return None
    return faiss.read_index(str(path))


def save_prototype_index(index: faiss.Index, path: Path):
    """
    Persist prototype FAISS index to disk.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(path))