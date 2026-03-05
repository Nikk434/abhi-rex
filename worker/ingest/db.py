# ingest/db.py
import json
from pathlib import Path
from typing import Optional
from sqlalchemy import Column, JSON
from sqlalchemy.orm import declarative_base

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Text,
    UniqueConstraint,
    PrimaryKeyConstraint,
    func,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

Base = declarative_base()


class Content(Base):
    __tablename__ = "content"

    # content_id = Column(String, primary_key=True)
    source_path = Column(String)
    show_id = Column(String)
    season = Column(Integer)
    episode_number = Column(Integer)
    title = Column(String)
    # type = Column(String)
    # metadata = Column(Text)
    # __tablename__ = "content"
    __table_args__ = {"schema": "ingest"}

    content_id = Column(String, primary_key=True)
    type = Column(String, nullable=False)

    metadata_json = Column("metadata", JSON)


class Frame(Base):
    __tablename__ = "frames"
    __table_args__ = (
        UniqueConstraint("content_id", "timestamp"),
    )

    frame_id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(String)
    timestamp = Column(Float)
    frame_path = Column(String)
    phash = Column(String)
    width = Column(Integer)
    height = Column(Integer)


class Vector(Base):
    __tablename__ = "vectors"

    vector_id = Column(Integer, primary_key=True)
    frame_id = Column(Integer)
    content_id = Column(String)


class EpisodeVector(Base):
    __tablename__ = "episode_vectors"
    __table_args__ = (
        PrimaryKeyConstraint("content_id", "vector_id"),
    )

    content_id = Column(String)
    vector_id = Column(Integer)


class Prototype(Base):
    __tablename__ = "prototypes"

    prototype_id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(String)
    vector_id = Column(Integer)


def init_db(session):
    engine = session.get_bind()
    Base.metadata.create_all(engine)


def insert_content(
    conn: Session,
    content_id: str,
    source_path: str,
    metadata: dict
):
    show_id = metadata.get("show_id")
    season = metadata.get("season")
    episode_number = metadata.get("episode") or metadata.get("episode_number")
    title = metadata.get("title")
    ctype = metadata.get("type")

    if ctype is None:
        if show_id is not None or season is not None or episode_number is not None:
            ctype = "episode"
        elif content_id.startswith("episode:"):
            ctype = "episode"
        else:
            ctype = "movie"

    metadata_json = json.dumps(metadata)

    existing = conn.get(Content, content_id)
    if existing:
        return

    row = Content(
        content_id=content_id,
        source_path=source_path,
        show_id=show_id,
        season=season,
        episode_number=episode_number,
        title=title,
        type=ctype,
        metadata=metadata_json
    )

    conn.add(row)
    conn.commit()


def insert_frame(
    conn: Session,
    content_id: str,
    timestamp: float,
    frame_path: str,
    phash: str,
    width: int,
    height: int
) -> int:

    row = conn.query(Frame).filter(
        Frame.content_id == content_id,
        Frame.timestamp == timestamp
    ).first()

    if row:
        return row.frame_id

    frame = Frame(
        content_id=content_id,
        timestamp=timestamp,
        frame_path=frame_path,
        phash=phash,
        width=width,
        height=height
    )

    conn.add(frame)
    conn.commit()
    conn.refresh(frame)

    return frame.frame_id


def add_vector_mapping(
    conn: Session,
    vector_id: int,
    frame_id: int,
    content_id: str
):

    row = Vector(
        vector_id=vector_id,
        frame_id=frame_id,
        content_id=content_id
    )

    conn.merge(row)
    conn.commit()


def add_episode_vector(
    conn: Session,
    content_id: str,
    vector_id: int
):

    exists = conn.query(EpisodeVector).filter(
        EpisodeVector.content_id == content_id,
        EpisodeVector.vector_id == vector_id
    ).first()

    if exists:
        return

    row = EpisodeVector(
        content_id=content_id,
        vector_id=vector_id
    )

    conn.add(row)
    conn.commit()


def get_max_vector_id(conn: Session) -> int:

    val = conn.query(func.max(Vector.vector_id)).scalar()

    if val is None:
        return -1

    return int(val)