# worker/prototypes/vectors.py
from typing import Dict, List, Tuple
import numpy as np
import faiss

from app.config import FAISS_INDEX_FRAMES

from app.config import FAISS_INDEX_FRAMES_FLAT
from ingest.faiss_utils import load_faiss_index

def load_vectors_by_content(
    conn,
    *,
    content_type: str,
    max_per_content: int,
) -> Dict[str, Tuple[np.ndarray, List[int]]]:
    """
    Load vectors from FAISS, grouped by content_id using episode_vectors table.

    Returns:
        {
            content_id: (
                vectors: np.ndarray [N, D],
                vector_ids: List[int]
            )
        }
    """

    # 1. Load FAISS frame index
    # index = faiss.read_index(str(FAISS_INDEX_FRAMES))
    index = load_faiss_index(FAISS_INDEX_FRAMES_FLAT)
    if index is None:
        return {}

    # 2. Get vector_id -> content_id mapping
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ev.vector_id, ev.content_id
        FROM episode_vectors ev
        JOIN content c ON c.content_id = ev.content_id
        WHERE c.type = ?
        ORDER BY ev.vector_id
        """,
        (content_type,),
    )

    rows = cur.fetchall()

    grouped: Dict[str, List[int]] = {}
    
    for content_id, blob in cur.fetchall():
        vec = np.frombuffer(blob, dtype="float32")
        grouped.setdefault(content_id, []).append(vec)

    result = {}
    for content_id, vecs in grouped.items():
        if max_per_content:
            vecs = vecs[:max_per_content]
        result[content_id] = (np.vstack(vecs), list(range(len(vecs))))

    return result