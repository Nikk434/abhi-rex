from sqlalchemy.orm import Session
from prototypes.models import PrototypeVector


def insert_prototype_vector(
    conn: Session,
    *,
    vector_id: int,
    content_id: str,
):
    row = PrototypeVector(
        vector_id=vector_id,
        content_id=content_id,
    )

    conn.add(row)
    conn.commit()