# prototypes/models.py

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class PrototypeVector(Base):
    __tablename__ = "prototype_vectors"

    vector_id = Column(Integer, primary_key=True, index=True)
    content_id = Column(String, primary_key=True, index=True)