def insert_prototype_vector(
    conn,
    *,
    vector_id: int,
    content_id: str,
):
    """
    Register a prototype vector for a content.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prototype_vectors (vector_id, content_id)
        VALUES (?, ?)
        """,
        (vector_id, content_id),
    )