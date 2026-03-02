# ingest/db.py
import sqlite3
import json
from pathlib import Path
from typing import Optional


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS content (
            content_id TEXT PRIMARY KEY,
            source_path TEXT,
            show_id TEXT,
            season INTEGER,
            episode_number INTEGER,
            title TEXT,
            type TEXT,
            metadata TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS frames (
            frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id TEXT,
            timestamp REAL,
            frame_path TEXT,
            phash TEXT,
            width INTEGER,
            height INTEGER,
            UNIQUE(content_id, timestamp)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vectors (
            vector_id INTEGER PRIMARY KEY,
            frame_id INTEGER,
            content_id TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS episode_vectors (
            content_id TEXT,
            vector_id INTEGER,
            PRIMARY KEY(content_id, vector_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prototypes (
            prototype_id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id TEXT,
            vector_id INTEGER
        );
    """)

    conn.commit()
    return conn


def insert_content(
    conn: sqlite3.Connection,
    content_id: str,
    source_path: str,
    metadata: dict
):
    cur = conn.cursor()

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

    cur.execute("""
        INSERT OR IGNORE INTO content(
            content_id, source_path, show_id,
            season, episode_number, title, type, metadata
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        content_id,
        source_path,
        show_id,
        season,
        episode_number,
        title,
        ctype,
        metadata_json
    ))

    conn.commit()


def insert_frame(
    conn: sqlite3.Connection,
    content_id: str,
    timestamp: float,
    frame_path: str,
    phash: str,
    width: int,
    height: int
) -> int:
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO frames(
            content_id, timestamp, frame_path, phash, width, height
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        content_id,
        timestamp,
        frame_path,
        phash,
        width,
        height
    ))

    conn.commit()

    cur.execute("""
        SELECT frame_id
        FROM frames
        WHERE content_id = ? AND timestamp = ?
    """, (content_id, timestamp))

    row = cur.fetchone()
    return int(row[0])


def add_vector_mapping(
    conn: sqlite3.Connection,
    vector_id: int,
    frame_id: int,
    content_id: str
):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO vectors(
            vector_id, frame_id, content_id
        )
        VALUES (?, ?, ?)
    """, (vector_id, frame_id, content_id))
    conn.commit()


def add_episode_vector(
    conn: sqlite3.Connection,
    content_id: str,
    vector_id: int
):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO episode_vectors(
            content_id, vector_id
        )
        VALUES (?, ?)
    """, (content_id, vector_id))
    conn.commit()


def get_max_vector_id(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT MAX(vector_id) FROM vectors")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return -1
    return int(row[0])