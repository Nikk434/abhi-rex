#!/usr/bin/env python3
"""
query_content.py (updated)

Query a video (or single image) against prototype index + ingestion DB and return top matching content
with human-friendly metadata (title, year) and absolute frame paths.

Usage examples:
  python query_content.py --out-dir ./ingest_out --video /path/to/video.mp4 --topk 5
  python query_content.py --out-dir ./ingest_out --image /path/to/frame.jpg --topk 5

Outputs:
 - Prints ranked results to console with title/year.
 - Writes results JSON to <out-dir>/query_results_with_meta.json
"""

import argparse
import sqlite3
from pathlib import Path
import json
import numpy as np
from tqdm import tqdm
import faiss
from PIL import Image
import cv2
import torch
import clip
import os

# filenames expected inside out-dir
DB_FILENAME = "ingest_meta.db"
PROTOTYPE_INDEX_FILENAME = "faiss_index_prototypes.bin"
FRAMES_SUBDIR = "frames"


# ---------------- CLIP embedder -------------------
class CLIPEmbedder:
    def __init__(self, model_name="ViT-B/32", device=None):
        # Auto-select GPU if available
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = device

        print(f"[CLIP] using device: {self.device}")

        # Load model
        self.model, self.preprocess = clip.load(model_name, device=self.device)
        self.model.eval()

        # Infer embedding dimension
        sample = self.preprocess(Image.new("RGB", (224, 224), (128, 128, 128))).unsqueeze(0).to(self.device)
        with torch.no_grad():
            v = self.model.encode_image(sample)
        self.dim = int(v.shape[-1])

        print(f"[CLIP] embedding dim = {self.dim}")

    def embed_pil(self, pil_images, batch_size=64):
        if not isinstance(pil_images, list):
            pil_images = [pil_images]

        toks = [self.preprocess(im).unsqueeze(0) for im in pil_images]
        out = []
        for i in range(0, len(toks), batch_size):
            batch = torch.cat(toks[i:i+batch_size], dim=0).to(self.device)
            with torch.no_grad():
                emb = self.model.encode_image(batch)
                emb = emb / emb.norm(dim=-1, keepdim=True)
            out.append(emb.cpu().numpy().astype("float32"))

        if len(out) == 0:
            return np.zeros((0, self.dim), dtype="float32")

        return np.vstack(out)

    def embed_paths(self, paths, batch_size=64):
        pil_imgs = []
        for p in paths:
            try:
                img = Image.open(p).convert("RGB")
            except Exception as e:
                print(f"[WARN] failed to open image {p}: {e}")
                continue
            pil_imgs.append(img)
        return self.embed_pil(pil_imgs, batch_size=batch_size)


# --------------- keyframe extraction ----------------
def detect_shots_keyframes(video_path):
    """Try PySceneDetect; fallback to uniform sample every 2s."""
    try:
        from scenedetect import VideoManager, SceneManager
        from scenedetect.detectors import ContentDetector
    except Exception:
        # fallback: uniform sampling every 2 seconds
        cap = cv2.VideoCapture(video_path)
        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        interval = int(max(1, fps * 2.0))
        frames = []
        for f in range(0, total, interval):
            frames.append((f / fps, f))
        cap.release()
        return frames

    video_manager = VideoManager([video_path])
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector())
    try:
        video_manager.start()
        scene_manager.detect_scenes(frame_source=video_manager)
        scene_list = scene_manager.get_scene_list()
    finally:
        video_manager.release()

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    keyframes = []
    for (start, end) in scene_list:
        start_f = start.get_frames()
        end_f = end.get_frames()
        mid = int((start_f + end_f) // 2)
        ts = mid / fps
        keyframes.append((ts, mid))
    cap.release()
    return keyframes


# ---------------- DB & index helpers ----------------
def load_db(out_dir):
    dbp = Path(out_dir) / DB_FILENAME
    if not dbp.exists():
        raise FileNotFoundError(f"DB not found at {dbp}")
    conn = sqlite3.connect(str(dbp))
    return conn


def load_prototype_index(out_dir):
    p = Path(out_dir) / PROTOTYPE_INDEX_FILENAME
    if not p.exists():
        raise FileNotFoundError(f"Prototype index not found at {p}")
    idx = faiss.read_index(str(p))
    return idx


def get_proto_to_content_map_and_metadata(conn):
    """
    Returns:
      proto_map: {proto_id: content_id}
      content_meta: {content_id: {title, year, type, source_path, metadata}}
    """
    cur = conn.cursor()
    cur.execute("SELECT prototype_id, content_id FROM prototypes")
    rows = cur.fetchall()
    proto_map = {int(r[0]): r[1] for r in rows}

    cur.execute("SELECT content_id, title, metadata, type, source_path FROM content")
    content_meta = {}
    for cid, title, metadata_json, ctype, source_path in cur.fetchall():
        meta = {}
        try:
            meta = json.loads(metadata_json) if metadata_json else {}
        except Exception:
            meta = {}
        display_title = title or meta.get("title") or cid
        year = meta.get("year") or meta.get("release_date") or None
        content_meta[cid] = {
            "title": display_title,
            "year": year,
            "type": ctype,
            "source_path": source_path,
            "metadata": meta,
        }
    return proto_map, content_meta


def get_frame_paths_for_content(conn, content_id, max_n=200):
    cur = conn.cursor()
    cur.execute("SELECT frame_path, timestamp FROM frames WHERE content_id=? LIMIT ?", (content_id, max_n))
    rows = cur.fetchall()
    return [{"path": r[0], "timestamp": r[1]} for r in rows]


# ---------------- ranking helpers ----------------
def agg_score_for_content_from_query_frames(conn, clip_embedder, query_embs, content_id, sample_n=200, batch_size=64):
    """
    For a candidate content_id:
      - fetch up to sample_n frame paths from DB,
      - embed them (batch),
      - compute cosine similarities vs each query_emb,
      - for each query_emb take the max similarity across sampled frames,
      - aggregate across query frames by mean of those maxima.
    Returns: aggregated_score, best_matches (list of dicts with path, timestamp, sim)
    """
    fps = get_frame_paths_for_content(conn, content_id, max_n=sample_n)
    if len(fps) == 0:
        return 0.0, []
    paths = [p["path"] for p in fps]
    frame_embs = clip_embedder.embed_paths(paths, batch_size=batch_size)  # shape (M, D)
    if frame_embs.shape[0] == 0:
        return 0.0, []
    sims = np.dot(query_embs, frame_embs.T)  # (Q, M)
    max_per_query = sims.max(axis=1)  # (Q,)
    agg = float(max_per_query.mean())
    # find top-3 frame matches overall
    flat_idx = np.argsort(sims.ravel())[::-1][:3]
    best_matches = []
    Q, M = sims.shape
    for fi in flat_idx:
        qi = fi // M
        fj = fi % M
        best_matches.append({
            "query_index": int(qi),
            "frame_path": paths[int(fj)],
            "timestamp": fps[int(fj)]["timestamp"],
            "similarity": float(sims[qi, fj])
        })
    return agg, best_matches


# ---------------- main flow ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True, help="ingest out dir (contains ingest_meta.db and faiss_index_prototypes.bin)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--video", help="path to query video")
    group.add_argument("--image", help="path to query image")
    parser.add_argument("--topk", type=int, default=5, help="top results to show")
    parser.add_argument("--proto-topk", type=int, default=20, help="prototype hits to collect per query frame")
    parser.add_argument("--re-rank-n", type=int, default=200, help="sample frames per candidate content for re-ranking")
    parser.add_argument("--batch-size", type=int, default=64, help="CLIP batch size")
    parser.add_argument("--model", type=str, default="ViT-B/32", help="CLIP model name")
    parser.add_argument("--max-query-frames", type=int, default=10, help="limit keyframes processed from the video (useful for long videos)")
    args = parser.parse_args()

    out_dir = args.out_dir
    topk = args.topk

    # load DB & prototype index
    conn = load_db(out_dir)
    proto_idx = load_prototype_index(out_dir)
    proto_map, content_meta = get_proto_to_content_map_and_metadata(conn)
    if len(proto_map) == 0:
        print("[ERROR] prototypes table empty. Build prototypes first.")
        return

    # CLIP embedder
    device = "cuda" if torch.cuda.is_available() else "cpu"
    clip_embedder = CLIPEmbedder(model_name=args.model, device=device)

    # prepare query frames
    query_pils = []
    query_timestamps = []
    if args.image:
        im = Image.open(args.image).convert("RGB")
        query_pils = [im]
        query_timestamps = [None]
    else:
        kf = detect_shots_keyframes(args.video)
        if args.max_query_frames and args.max_query_frames > 0:
            kf = kf[:args.max_query_frames]
        if len(kf) == 0:
            print("[WARN] no keyframes found, exiting")
            return
        cap = cv2.VideoCapture(args.video)
        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        for ts, frame_num in kf:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
            ret, frame = cap.read()
            if not ret:
                continue
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            pil = Image.fromarray(rgb)
            query_pils.append(pil)
            query_timestamps.append(ts)
        cap.release()
    if len(query_pils) == 0:
        print("[ERROR] no frames to query")
        return

    # embed query frames
    print(f"[INFO] Embedding {len(query_pils)} query frames ...")
    query_embs = clip_embedder.embed_pil(query_pils, batch_size=args.batch_size)  # shape (Q, D)

    # search prototype index for each query embedding
    Q = query_embs.shape[0]
    proto_candidates = {}  # content_id -> aggregated proto score
    for i in range(Q):
        q = query_embs[i].astype("float32").reshape(1, -1)
        try:
            Dists, Ids = proto_idx.search(q, args.proto_topk)
        except Exception as e:
            print("[ERROR] prototype index search failed:", e)
            return
        ids = Ids[0]
        dists = Dists[0]
        for dist, pid in zip(dists, ids):
            if int(pid) == -1:
                continue
            cid = proto_map.get(int(pid))
            if cid is None:
                continue
            proto_candidates[cid] = proto_candidates.get(cid, 0.0) + float(dist)

    if len(proto_candidates) == 0:
        print("[INFO] No prototype candidates found.")
        return

    candidate_list = sorted(proto_candidates.items(), key=lambda x: x[1], reverse=True)
    re_rank_candidate_count = max(10, args.topk * 5)
    candidate_list = candidate_list[:re_rank_candidate_count]
    candidate_ids = [c for c, _ in candidate_list]
    print(f"[INFO] Prototype candidate count for re-ranking: {len(candidate_ids)}")

    # re-rank
    final_scores = []  # (content_id, score, best_matches_list)
    for cid in tqdm(candidate_ids, desc="re-ranking"):
        agg_score, best_matches = agg_score_for_content_from_query_frames(conn, clip_embedder, query_embs, cid, sample_n=args.re_rank_n, batch_size=args.batch_size)
        final_scores.append((cid, agg_score, best_matches))

    # sort final results and keep topk
    final_scores = sorted(final_scores, key=lambda x: x[1], reverse=True)[:topk]

    # normalize scores (optional) for readable output
    if final_scores:
        scores = [s for (_, s, _) in final_scores]
        min_s, max_s = min(scores), max(scores)
        rng = max_s - min_s if (max_s - min_s) > 1e-6 else 1.0
    else:
        min_s, max_s, rng = 0.0, 0.0, 1.0

    # prepare output
    out = []
    print("\n=== TOP MATCHES ===")
    for rank, (cid, score, matches) in enumerate(final_scores, start=1):
        norm_score = float((score - min_s) / rng)
        meta = content_meta.get(cid, {"title": cid, "year": None, "type": None, "source_path": None, "metadata": {}})
        print(f"{rank}. {meta['title']} ({cid})  type={meta['type']}  score={norm_score:.4f}")
        if meta.get("year"):
            print(f"    year: {meta['year']}")
        if matches:
            for m in matches:
                frame_path_abs = os.path.abspath(m["frame_path"])
                print(f"   - match sim={m['similarity']:.4f}, path={frame_path_abs}, ts={m.get('timestamp')}")
        out.append({
            "rank": rank,
            "content_id": cid,
            "title": meta.get("title"),
            "year": meta.get("year"),
            "type": meta.get("type"),
            "score_raw": float(score),
            "score_norm": norm_score,
            "matches": [
                {
                    "query_index": m["query_index"],
                    "frame_path": os.path.abspath(m["frame_path"]),
                    "timestamp": m.get("timestamp"),
                    "similarity": m["similarity"]
                } for m in matches
            ],
            "metadata": meta.get("metadata", {})
        })

    out_path = Path(args.out_dir) / "query_results_with_meta.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {out_path}")

    conn.close()


if __name__ == "__main__":
    main()
