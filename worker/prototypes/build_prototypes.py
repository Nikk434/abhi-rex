# #!/usr/bin/env python3
# """
# build_prototypes_flexible.py

# Build prototypes for contents (movies / episodes / all) based on configuration.

# Usage examples:
#   # build prototypes for episodes only
#   python build_prototypes_flexible.py --out-dir ./ingest_out --content-type episode --k 64

#   # build for movies only with smaller K and smaller batch-size
#   python build_prototypes_flexible.py --out-dir ./ingest_out --content-type movie --k 32 --batch-size 32

#   # build for all contents, CPU-friendly
#   python build_prototypes_flexible.py --out-dir ./ingest_out --content-type all --k 32 --batch-size 8 --model ViT-B/32

# Notes:
#  - The script expects the ingestion DB and frames/index under out-dir:
#     - <out-dir>/ingest_meta.db
#     - <out-dir>/faiss_index_frames.bin (optional; used for reconstruct fallback)
#     - <out-dir>/frames/  (contains saved frame images from ingestion)
#  - It will produce:
#     - <out-dir>/faiss_index_prototypes.bin
#     - rows in prototypes table: prototypes(prototype_id, content_id, vector_id)
#  - If FAISS reconstruct is not supported, the script re-embeds saved frame images (slower).
# """

# import argparse
# import sqlite3
# from pathlib import Path
# import numpy as np
# from tqdm import tqdm
# import faiss
# from sklearn.cluster import KMeans
# import sys
# import math
# from PIL import Image
# import torch
# import clip
# import os

# # Defaults
# DEFAULT_OUTDIR = "ingest_out"
# DB_FILENAME = "ingest_meta.db"
# FRAME_INDEX_FILENAME = "faiss_index_frames.bin"
# PROTOTYPE_INDEX_FILENAME = "faiss_index_prototypes.bin"
# FRAMES_SUBDIR = "frames"

# def ensure_prototypes_table(conn):
#     cur = conn.cursor()
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS prototypes (
#             prototype_id INTEGER PRIMARY KEY,
#             content_id TEXT,
#             vector_id INTEGER
#         );
#     """)
#     conn.commit()

# def get_contents(conn, content_type):
#     cur = conn.cursor()
#     if content_type == "episode":
#         cur.execute("SELECT content_id FROM content WHERE type='episode'")
#     elif content_type == "movie":
#         cur.execute("SELECT content_id FROM content WHERE type='movie'")
#     else:
#         cur.execute("SELECT content_id FROM content")
#     return [r[0] for r in cur.fetchall()]

# def get_vector_ids_for_content(conn, content_id):
#     cur = conn.cursor()
#     cur.execute("SELECT vector_id FROM episode_vectors WHERE content_id=?", (content_id,))
#     rows = cur.fetchall()
#     if rows:
#         return [r[0] for r in rows]
#     cur.execute("SELECT vector_id FROM vectors WHERE content_id=?", (content_id,))
#     rows = cur.fetchall()
#     return [r[0] for r in rows]

# def get_frame_paths_for_content(conn, content_id):
#     cur = conn.cursor()
#     cur.execute("SELECT frame_path FROM frames WHERE content_id=?", (content_id,))
#     rows = cur.fetchall()
#     return [r[0] for r in rows]

# def create_proto_index(dim, M=32, efConstruction=200):
#     idx = faiss.IndexHNSWFlat(dim, M)
#     idx.hnsw.efConstruction = efConstruction
#     return faiss.IndexIDMap(idx)

# def get_next_proto_id(conn):
#     cur = conn.cursor()
#     cur.execute("SELECT MAX(vector_id) FROM prototypes")
#     row = cur.fetchone()
#     if row is None or row[0] is None:
#         return 1
#     return int(row[0]) + 1

# def insert_prototype_row(conn, proto_id, content_id):
#     cur = conn.cursor()
#     cur.execute("INSERT OR REPLACE INTO prototypes(prototype_id, content_id, vector_id) VALUES (?, ?, ?)",
#                 (int(proto_id), content_id, int(proto_id)))
#     conn.commit()

# class CLIPEmbedderFallback:
#     def __init__(self, model_name="ViT-B/32", device=None):
#         self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
#         print(f"[CLIP] device: {self.device}")
#         self.model, self.preprocess = clip.load(model_name, device=self.device)
#         self.model.eval()
#         # infer dim
#         sample = self.preprocess(Image.new("RGB", (224,224), (128,128,128))).unsqueeze(0).to(self.device)
#         with torch.no_grad():
#             v = self.model.encode_image(sample)
#         self.dim = int(v.shape[-1])
#         print(f"[CLIP] embedding dim: {self.dim}")

#     def embed_paths(self, paths, batch_size=64):
#         if len(paths) == 0:
#             return np.zeros((0, self.dim), dtype="float32")
#         out = []
#         batch_imgs = []
#         for p in paths:
#             try:
#                 im = Image.open(p).convert("RGB")
#             except Exception as e:
#                 print(f"[WARN] failed to load image {p}: {e}")
#                 continue
#             batch_imgs.append(self.preprocess(im).unsqueeze(0))
#             if len(batch_imgs) >= batch_size:
#                 xb = torch.cat(batch_imgs, dim=0).to(self.device)
#                 with torch.no_grad():
#                     emb = self.model.encode_image(xb)
#                     emb = emb / emb.norm(dim=-1, keepdim=True)
#                 out.append(emb.cpu().numpy().astype("float32"))
#                 batch_imgs = []
#         if batch_imgs:
#             xb = torch.cat(batch_imgs, dim=0).to(self.device)
#             with torch.no_grad():
#                 emb = self.model.encode_image(xb)
#                 emb = emb / emb.norm(dim=-1, keepdim=True)
#             out.append(emb.cpu().numpy().astype("float32"))
#         if len(out) == 0:
#             return np.zeros((0, self.dim), dtype="float32")
#         return np.vstack(out)

# def load_frame_index(out_dir):
#     path = Path(out_dir) / FRAME_INDEX_FILENAME
#     if not path.exists():
#         return None
#     try:
#         idx = faiss.read_index(str(path))
#         return idx
#     except Exception as e:
#         print(f"[WARN] failed to load frame index: {e}")
#         return None

# def build_prototypes_flexible(
#     out_dir: Path,
#     content_type: str = "episode",
#     k: int = 64,
#     max_per_content: int = 2000,
#     batch_size: int = 64,
#     model: str = "ViT-B/32",
#     proto_m: int = 32,
#     force_reembed: bool = False,
# ) -> int:
#     db_path = out_dir / DB_FILENAME
#     proto_index_path = out_dir / PROTOTYPE_INDEX_FILENAME
#     frames_dir = out_dir / FRAMES_SUBDIR

#     if not db_path.exists():
#         print("[ERROR] DB not found at", db_path)
#         return -1  # or raise FileNotFoundError
#     conn = sqlite3.connect(str(db_path))

#     ensure_prototypes_table(conn)
#     contents = get_contents(conn, content_type)
#     if len(contents) == 0:
#         print("[INFO] no content of requested type found. Exiting.")
#         conn.close()
#         return 0

#     print(f"[INFO] Found {len(contents)} contents of type '{content_type}'")

#     frame_index = load_frame_index(out_dir)
#     if frame_index is not None:
#         print(f"[INFO] Loaded frame index. dim={getattr(frame_index,'d',None)} ntotal={getattr(frame_index,'ntotal',None)}")
#     else:
#         print("[INFO] No usable frame FAISS index found; will fallback to re-embedding frames for all contents.")

#     clip_embedder = CLIPEmbedderFallback(model_name=model, device=None)
#     dim = getattr(frame_index, "d", None) or clip_embedder.dim
#     print(f"[INFO] Using embedding dim = {dim}")

#     if proto_index_path.exists():
#         try:
#             proto_index = faiss.read_index(str(proto_index_path))
#             print("[INFO] loaded existing prototype index from disk")
#         except Exception as e:
#             print("[WARN] failed to load existing prototype index; creating new one:", e)
#             proto_index = create_proto_index(dim, M=proto_m)
#     else:
#         proto_index = create_proto_index(dim, M=proto_m)

#     next_proto_id = get_next_proto_id(conn)
#     print(f"[INFO] next prototype id starts at {next_proto_id}")

#     for content_id in tqdm(contents, desc="contents"):
#         try:
#             vector_ids = get_vector_ids_for_content(conn, content_id)
#         except Exception:
#             vector_ids = []
#         embeddings = None

#         if (not force_reembed) and frame_index is not None and len(vector_ids) > 0:
#             vids = vector_ids if len(vector_ids) <= max_per_content else vector_ids[:max_per_content]
#             reconstructed = []
#             reconstruct_failed = False
#             for vid in vids:
#                 try:
#                     v = frame_index.reconstruct(int(vid))
#                     reconstructed.append(np.array(v, dtype="float32"))
#                 except Exception:
#                     reconstruct_failed = True
#                     break
#             if not reconstruct_failed and len(reconstructed) > 0:
#                 embeddings = np.vstack(reconstructed).astype("float32")

#         if embeddings is None:
#             frame_paths = get_frame_paths_for_content(conn, content_id)
#             if len(frame_paths) == 0:
#                 print(f"[WARN] no frame images found for {content_id}, skipping")
#                 continue
#             if len(frame_paths) > max_per_content:
#                 rng = np.random.default_rng(seed=42)
#                 frame_paths = list(np.array(frame_paths)[rng.choice(len(frame_paths), max_per_content, replace=False)])
#             embeddings = clip_embedder.embed_paths(frame_paths, batch_size=batch_size)
#             if embeddings.shape[0] == 0:
#                 print(f"[WARN] re-embedding produced no embeddings for {content_id}, skipping")
#                 continue

#         nvec = embeddings.shape[0]
#         K = min(k, nvec)
#         if nvec <= K:
#             centroids = embeddings / (np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-10)
#         else:
#             kmeans = KMeans(n_clusters=K, random_state=0, n_init=10)
#             kmeans.fit(embeddings)
#             centroids = kmeans.cluster_centers_.astype("float32")
#             centroids = centroids / (np.linalg.norm(centroids, axis=1, keepdims=True) + 1e-10)

#         proto_ids = np.arange(next_proto_id, next_proto_id + centroids.shape[0]).astype(np.int64)
#         try:
#             proto_index.add_with_ids(centroids, proto_ids)
#         except Exception as e:
#             print(f"[ERROR] failed to add prototypes for {content_id}: {e}")
#             continue

#         for pid in proto_ids:
#             insert_prototype_row(conn, int(pid), content_id)

#         next_proto_id += centroids.shape[0]

#     try:
#         faiss.write_index(proto_index, str(proto_index_path))
#         print(f"[INFO] saved prototype index to {proto_index_path}")
#     except Exception as e:
#         print("[WARN] failed to save prototype index:", e)

#     conn.close()
#     print("[DONE]")
#     return next_proto_id  # return final proto count/id as a useful signal


# def main():
#     p = argparse.ArgumentParser()
#     p.add_argument("--out-dir", default=DEFAULT_OUTDIR)
#     p.add_argument("--content-type", choices=["all", "movie", "episode"], default="episode")
#     p.add_argument("--k", type=int, default=64)
#     p.add_argument("--max-per-content", type=int, default=2000)
#     p.add_argument("--batch-size", type=int, default=64)
#     p.add_argument("--model", type=str, default="ViT-B/32")
#     p.add_argument("--proto-m", type=int, default=32)
#     p.add_argument("--force-reembed", action="store_true")
#     args = p.parse_args()

#     build_prototypes_flexible(
#         out_dir=Path(args.out_dir),
#         content_type=args.content_type,
#         k=args.k,
#         max_per_content=args.max_per_content,
#         batch_size=args.batch_size,
#         model=args.model,
#         proto_m=args.proto_m,
#         force_reembed=args.force_reembed,
#     )
from pathlib import Path
import numpy as np
from tqdm import tqdm
import faiss
from sklearn.cluster import KMeans
from PIL import Image
import torch
import clip

from sqlalchemy import Float, create_engine, Column, Integer, String, JSON, Table, MetaData, func, select, insert
from sqlalchemy.orm import sessionmaker

# =========================
# CONFIG
# =========================
DATABASE_URL = "postgresql+psycopg2://postgres:nik_admin_434@localhost:5432/postgres"
DEFAULT_OUTDIR = "ingest_out"
FRAME_INDEX_FILENAME = "faiss_index_frames.bin"
PROTOTYPE_INDEX_FILENAME = "faiss_index_prototypes.bin"
FRAMES_SUBDIR = "frames"

# =========================
# DATABASE SETUP
# =========================
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)
metadata = MetaData()

# tables
prototypes = Table(
    "prototypes",
    metadata,
    Column("prototype_id", Integer, primary_key=True),
    Column("content_id", String),
    Column("vector_id", Integer),
)

content = Table(
    "content",
    metadata,
    Column("content_id", String, primary_key=True),
    Column("source_path", String),
    Column("show_id",String),
    Column("season",Integer),
    Column("episode_number",Integer),
    Column("title",String),
    Column("type", String),
    Column("metadata", JSON),
)

frames = Table(
    "frames",
    metadata,
    Column("frame_id", Integer, primary_key=True),
    Column("content_id", String),
    Column("timestamp", Float),           # in seconds
    Column("frame_path", String),
    Column("phash", String),
    Column("width", Integer),
    Column("height", Integer),
)

metadata.create_all(engine)

# =========================
# HELPERS
# =========================
def create_proto_index(dim, M=32, efConstruction=200):
    idx = faiss.IndexHNSWFlat(dim, M)
    idx.hnsw.efConstruction = efConstruction
    return faiss.IndexIDMap(idx)

def get_contents(session, content_type):
    if content_type == "episode":
        stmt = select(content.c.content_id).where(func.lower(content.c.type) == "episode")
    elif content_type == "movie":
        stmt = select(content.c.content_id).where(func.lower(content.c.type) == "movie")
    else:
        stmt = select(content.c.content_id)
    return [r[0] for r in session.execute(stmt)]

def get_vector_ids_for_content(session, content_id):
    stmt = select(prototypes.c.vector_id).where(prototypes.c.content_id == content_id)
    return [r[0] for r in session.execute(stmt)]

def get_frame_paths_for_content(session, content_id):
    stmt = select(frames.c.frame_path).where(frames.c.content_id == content_id)
    return [r[0] for r in session.execute(stmt)]

def get_next_proto_id(session):
    stmt = select(prototypes.c.vector_id)
    result = session.execute(stmt).all()
    if not result:
        return 1
    return max(r[0] for r in result if r[0] is not None) + 1

def insert_prototype_row(session, proto_id, content_id):
    stmt = insert(prototypes).values(
        prototype_id=int(proto_id),
        content_id=content_id,
        vector_id=int(proto_id),
    ).on_conflict_do_update(
        index_elements=[prototypes.c.prototype_id],
        set_={"content_id": content_id, "vector_id": int(proto_id)},
    )
    session.execute(stmt)
    session.commit()

class CLIPEmbedderFallback:
    def __init__(self, model_name="ViT-B/32", device=None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        print(f"[CLIP] device: {self.device}")
        self.model, self.preprocess = clip.load(model_name, device=self.device)
        self.model.eval()
        sample = self.preprocess(Image.new("RGB", (224,224), (128,128,128))).unsqueeze(0).to(self.device)
        with torch.no_grad():
            v = self.model.encode_image(sample)
        self.dim = int(v.shape[-1])
        print(f"[CLIP] embedding dim: {self.dim}")

    def embed_paths(self, paths, batch_size=64):
        if len(paths) == 0:
            return np.zeros((0, self.dim), dtype="float32")
        out = []
        batch_imgs = []
        for p in paths:
            try:
                im = Image.open(p).convert("RGB")
            except Exception as e:
                print(f"[WARN] failed to load image {p}: {e}")
                continue
            batch_imgs.append(self.preprocess(im).unsqueeze(0))
            if len(batch_imgs) >= batch_size:
                xb = torch.cat(batch_imgs, dim=0).to(self.device)
                with torch.no_grad():
                    emb = self.model.encode_image(xb)
                    emb = emb / emb.norm(dim=-1, keepdim=True)
                out.append(emb.cpu().numpy().astype("float32"))
                batch_imgs = []
        if batch_imgs:
            xb = torch.cat(batch_imgs, dim=0).to(self.device)
            with torch.no_grad():
                emb = self.model.encode_image(xb)
                emb = emb / emb.norm(dim=-1, keepdim=True)
            out.append(emb.cpu().numpy().astype("float32"))
        if len(out) == 0:
            return np.zeros((0, self.dim), dtype="float32")
        return np.vstack(out)

def load_frame_index(out_dir):
    path = Path(out_dir) / FRAME_INDEX_FILENAME
    if not path.exists():
        return None
    try:
        idx = faiss.read_index(str(path))
        return idx
    except Exception as e:
        print(f"[WARN] failed to load frame index: {e}")
        return None

# =========================
# MAIN FUNCTION
# =========================
def build_prototypes_flexible(
    out_dir: Path,
    content_type: str = "episode",
    k: int = 64,
    max_per_content: int = 2000,
    batch_size: int = 64,
    model: str = "ViT-B/32",
    proto_m: int = 32,
    force_reembed: bool = False,
) -> int:

    session = SessionLocal()

    proto_index_path = out_dir / PROTOTYPE_INDEX_FILENAME
    frames_dir = out_dir / FRAMES_SUBDIR

    contents = get_contents(session, content_type)
    if len(contents) == 0:
        print("[INFO] no content of requested type found. Exiting.")
        session.close()
        return 0

    print(f"[INFO] Found {len(contents)} contents of type '{content_type}'")

    frame_index = load_frame_index(out_dir)
    if frame_index is not None:
        print(f"[INFO] Loaded frame index. dim={getattr(frame_index,'d',None)} ntotal={getattr(frame_index,'ntotal',None)}")
    else:
        print("[INFO] No usable frame FAISS index found; will fallback to re-embedding frames for all contents.")

    clip_embedder = CLIPEmbedderFallback(model_name=model, device=None)
    dim = getattr(frame_index, "d", None) or clip_embedder.dim
    print(f"[INFO] Using embedding dim = {dim}")

    if proto_index_path.exists():
        try:
            proto_index = faiss.read_index(str(proto_index_path))
            print("[INFO] loaded existing prototype index from disk")
        except Exception as e:
            print("[WARN] failed to load existing prototype index; creating new one:", e)
            proto_index = create_proto_index(dim, M=proto_m)
    else:
        proto_index = create_proto_index(dim, M=proto_m)

    next_proto_id = get_next_proto_id(session)
    print(f"[INFO] next prototype id starts at {next_proto_id}")

    for content_id in tqdm(contents, desc="contents"):
        vector_ids = get_vector_ids_for_content(session, content_id)
        embeddings = None

        if (not force_reembed) and frame_index is not None and len(vector_ids) > 0:
            vids = vector_ids if len(vector_ids) <= max_per_content else vector_ids[:max_per_content]
            reconstructed = []
            reconstruct_failed = False
            for vid in vids:
                try:
                    v = frame_index.reconstruct(int(vid))
                    reconstructed.append(np.array(v, dtype="float32"))
                except Exception:
                    reconstruct_failed = True
                    break
            if not reconstruct_failed and len(reconstructed) > 0:
                embeddings = np.vstack(reconstructed).astype("float32")

        if embeddings is None:
            frame_paths = get_frame_paths_for_content(session, content_id)
            if len(frame_paths) == 0:
                print(f"[WARN] no frame images found for {content_id}, skipping")
                continue
            if len(frame_paths) > max_per_content:
                rng = np.random.default_rng(seed=42)
                frame_paths = list(np.array(frame_paths)[rng.choice(len(frame_paths), max_per_content, replace=False)])
            embeddings = clip_embedder.embed_paths(frame_paths, batch_size=batch_size)
            if embeddings.shape[0] == 0:
                print(f"[WARN] re-embedding produced no embeddings for {content_id}, skipping")
                continue

        nvec = embeddings.shape[0]
        K = min(k, nvec)
        if nvec <= K:
            centroids = embeddings / (np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-10)
        else:
            kmeans = KMeans(n_clusters=K, random_state=0, n_init=10)
            kmeans.fit(embeddings)
            centroids = kmeans.cluster_centers_.astype("float32")
            centroids = centroids / (np.linalg.norm(centroids, axis=1, keepdims=True) + 1e-10)

        proto_ids = np.arange(next_proto_id, next_proto_id + centroids.shape[0]).astype(np.int64)
        try:
            proto_index.add_with_ids(centroids, proto_ids)
        except Exception as e:
            print(f"[ERROR] failed to add prototypes for {content_id}: {e}")
            continue

        for pid in proto_ids:
            insert_prototype_row(session, int(pid), content_id)

        next_proto_id += centroids.shape[0]

    try:
        faiss.write_index(proto_index, str(proto_index_path))
        print(f"[INFO] saved prototype index to {proto_index_path}")
    except Exception as e:
        print("[WARN] failed to save prototype index:", e)

    session.close()
    print("[DONE]")
    return next_proto_id