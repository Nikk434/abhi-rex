from typing import Dict, List, Tuple
import numpy as np
from sqlalchemy import text

from app.config import FAISS_INDEX_FRAMES_FLAT
from ingest.faiss_utils import load_faiss_index


def load_vectors_by_content(
    conn,
    *,
    content_type: str,
    max_per_content: int,
) -> Dict[str, Tuple[np.ndarray, List[int]]]:

    index = load_faiss_index(FAISS_INDEX_FRAMES_FLAT)
    if index is None:
        return {}

    rows = conn.execute(
        text(
            """
            SELECT ev.vector_id, ev.content_id
            FROM episode_vectors ev
            JOIN content c ON c.content_id = ev.content_id
            WHERE c.type = :content_type
            ORDER BY ev.vector_id
            """
        ),
        {"content_type": content_type},
    ).fetchall()

    grouped: Dict[str, List[np.ndarray]] = {}

    for row in rows:
        vector_id = row.vector_id
        content_id = row.content_id

        vec = index.reconstruct(vector_id)
        grouped.setdefault(content_id, []).append(vec)

    result = {}
    for content_id, vecs in grouped.items():
        if max_per_content:
            vecs = vecs[:max_per_content]

        arr = np.vstack(vecs)
        result[content_id] = (arr, list(range(len(vecs))))

    return result