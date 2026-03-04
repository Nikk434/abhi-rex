import numpy as np
from sklearn.cluster import MiniBatchKMeans


def select_prototypes(
    vectors: np.ndarray,
    k: int,
) -> np.ndarray:
    """
    Select prototype vectors using clustering.

    Args:
        vectors: np.ndarray of shape [N, D]
        k: desired number of prototypes

    Returns:
        np.ndarray of shape [K, D]
    """
    n, dim = vectors.shape

    if n <= k:
        # Nothing to cluster, return originals
        return vectors.astype("float32")

    kmeans = MiniBatchKMeans(
        n_clusters=k,
        random_state=42,
        batch_size=min(1024, n),
        n_init="auto",
    )

    kmeans.fit(vectors)

    centers = kmeans.cluster_centers_
    return centers.astype("float32")