from pathlib import Path
import numpy as np

from prototypes.vectors import load_vectors_by_content
from prototypes.cluster import select_prototypes
from prototypes.faiss_utils import (
    create_prototype_index,
    load_prototype_index,
    save_prototype_index,
)
from prototypes.db import insert_prototype_vector
from ingest.db import get_max_vector_id
from app.config import FAISS_INDEX_PROTOTYPES


def build_prototypes(
    conn,
    out_dir: Path,
    *,
    content_type: str = "episode",
    max_vectors_per_content: int = 2000,
    k: int = 64,
) -> int:
    """
    Build prototype vectors for given content type.

    Returns:
        Number of prototype vectors added
    """

    vectors_by_content = load_vectors_by_content(
        conn,
        content_type=content_type,
        max_per_content=max_vectors_per_content,
    )

    if not vectors_by_content:
        return 0

    proto_index_path = out_dir / PROTOTYPE_INDEX_FILENAME
    proto_index = load_prototype_index(proto_index_path)

    next_vector_id = get_max_vector_id(conn) + 1
    added = 0

    for content_id, (vectors, _) in vectors_by_content.items():
        if vectors.size == 0:
            continue

        prototypes = select_prototypes(vectors, k)

        if proto_index is None:
            proto_index = create_prototype_index(prototypes.shape[1])

        ids = np.arange(
            next_vector_id,
            next_vector_id + prototypes.shape[0],
            dtype=np.int64,
        )

        proto_index.add_with_ids(prototypes, ids)

        for vid in ids.tolist():
            insert_prototype_vector(
                conn,
                vector_id=int(vid),
                content_id=content_id,
            )

        next_vector_id += prototypes.shape[0]
        added += prototypes.shape[0]

    if proto_index is not None:
        save_prototype_index(proto_index, proto_index_path)

    return added