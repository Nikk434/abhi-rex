# ingest/core.py
import re
from pathlib import Path
from typing import Dict, Optional, Iterable, Tuple, List

import cv2
import numpy as np
from PIL import Image
import imagehash
from tqdm import tqdm

from app.config import (
    FRAME_DIR,
    FAISS_INDEX_FRAMES,
    BATCH_SIZE,
)

from ingest.video import detect_shots_and_keyframes, read_frame_at
from ingest.frame_filters import is_bad_frame_bgr
from ingest.clip_embedder import CLIPEmbedder
from ingest.faiss_utils import create_faiss_index
from ingest.db import (
    insert_content,
    insert_frame,
    add_vector_mapping,
    add_episode_vector,
    get_max_vector_id,
)


_EP_RE = re.compile(r"[sS](\d{1,2})[eE](\d{1,2})")


# -------------------------
# content id inference
# -------------------------
def infer_content_type_and_ids(
    path: str,
    provided_content_id: Optional[str],
    metadata: Dict,
):
    if provided_content_id:
        if "type" not in metadata:
            metadata["type"] = (
                "episode" if provided_content_id.startswith("episode:") else "movie"
            )
        return provided_content_id, metadata

    if all(k in metadata for k in ("show_id", "season", "episode")):
        cid = (
            f"episode:{metadata['show_id']}:"
            f"S{int(metadata['season']):02d}"
            f"E{int(metadata['episode']):02d}"
        )
        metadata["type"] = "episode"
        return cid, metadata

    fname = Path(path).stem
    match = _EP_RE.search(fname)
    if match:
        season, episode = map(int, match.groups())
        show_id = Path(path).parent.stem or fname
        metadata.update(
            {
                "show_id": show_id,
                "season": season,
                "episode": episode,
                "type": "episode",
            }
        )
        return f"episode:{show_id}:S{season:02d}E{episode:02d}", metadata

    metadata["type"] = "movie"
    return f"movie:{fname}", metadata


# -------------------------
# public entry
# -------------------------
def ingest_job(
    payload: Dict,
    conn,
    faiss_index,
    embedder: CLIPEmbedder,
    out_dir: Path,
    batch_size: int = BATCH_SIZE,
):
    jobs = payload.get("jobs") or [payload]

    FRAME_DIR.mkdir(parents=True, exist_ok=True)

    for job in jobs:
        video = job["video"]
        metadata = job.get("metadata", {})
        provided_cid = job.get("content_id")

        content_id, metadata = infer_content_type_and_ids(
            video, provided_cid, metadata
        )

        faiss_index = _process_single_video(
            video_path=video,
            content_id=content_id,
            metadata=metadata,
            conn=conn,
            faiss_index=faiss_index,
            embedder=embedder,
            batch_size=batch_size,
        )

    return faiss_index


# -------------------------
# single video ingest
# -------------------------
def _process_single_video(
    video_path: str,
    content_id: str,
    metadata: Dict,
    conn,
    faiss_index,
    embedder: CLIPEmbedder,
    batch_size: int,
):
    insert_content(conn, content_id, video_path, metadata)

    keyframes = detect_shots_and_keyframes(video_path)

    to_embed: List[Image.Image] = []
    to_meta: List[Tuple[int, str]] = []

    next_vector_id = get_max_vector_id(conn) + 1
    added = 0

    for ts, frame_num in tqdm(keyframes, desc=f"ingest:{content_id}"):
        img_bgr, timestamp = read_frame_at(video_path, frame_num)
        if img_bgr is None or is_bad_frame_bgr(img_bgr):
            continue

        img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
        pil = Image.fromarray(img_rgb)

        phash = str(imagehash.phash(pil))
        fname = f"{content_id.replace('/', '_')}_{int(timestamp * 1000)}.jpg"
        fpath = FRAME_DIR / fname
        pil.save(fpath, quality=85)

        w, h = pil.size
        frame_id = insert_frame(
            conn,
            content_id,
            float(timestamp),
            str(fpath),
            phash,
            w,
            h,
        )

        to_embed.append(pil)
        to_meta.append((frame_id, content_id))

        if len(to_embed) >= batch_size:
            faiss_index, next_vector_id, added = _flush_embeddings(
                to_embed,
                to_meta,
                conn,
                faiss_index,
                embedder,
                next_vector_id,
                added,
            )
            to_embed.clear()
            to_meta.clear()

    if to_embed:
        faiss_index, next_vector_id, added = _flush_embeddings(
            to_embed,
            to_meta,
            conn,
            faiss_index,
            embedder,
            next_vector_id,
            added,
        )

    print(f"[DONE] {content_id} -> {added} vectors")
    return faiss_index


# -------------------------
# embedding flush
# -------------------------
def _flush_embeddings(
    images: Iterable[Image.Image],
    meta: Iterable[Tuple[int, str]],
    conn,
    faiss_index,
    embedder: CLIPEmbedder,
    next_vector_id: int,
    added: int,
):
    embeddings = embedder.embed_pil_images(list(images))
    if embeddings.size == 0:
        return faiss_index, next_vector_id, added

    if faiss_index is None:
        faiss_index = create_faiss_index(embeddings.shape[1])

    ids = np.arange(
        next_vector_id,
        next_vector_id + embeddings.shape[0],
        dtype=np.int64,
    )

    faiss_index.add_with_ids(embeddings, ids)

    for vid, (frame_id, content_id) in zip(ids.tolist(), meta):
        add_vector_mapping(conn, vid, frame_id, content_id)
        add_episode_vector(conn, content_id, vid)

    return (
        faiss_index,
        next_vector_id + embeddings.shape[0],
        added + embeddings.shape[0],
    )