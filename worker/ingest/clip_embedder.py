# ingest/clip_embedder.py
from typing import List

import torch
import numpy as np
from PIL import Image
import clip

from app.config import CLIP_MODEL


class CLIPEmbedder:
    def __init__(self, model_name: str = CLIP_MODEL, device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        print(f"[CLIP] using device: {self.device}")

        self.model, self.preprocess = clip.load(model_name, device=self.device)
        self.model.eval()

        # infer embedding dimension safely
        dummy = self.preprocess(
            Image.new("RGB", (224, 224), (128, 128, 128))
        ).unsqueeze(0).to(self.device)

        with torch.no_grad():
            vec = self.model.encode_image(dummy)

        self.dim = vec.shape[-1]
        print(f"[CLIP] embedding dim = {self.dim}")

    def embed_pil_images(self, images: List[Image.Image]) -> np.ndarray:
        """
        Takes a list of PIL images and returns float32 normalized embeddings.
        """
        if not images:
            return np.empty((0, self.dim), dtype="float32")

        tensors = [
            self.preprocess(img).unsqueeze(0)
            for img in images
        ]

        batch = torch.cat(tensors, dim=0).to(self.device)

        with torch.no_grad():
            embeddings = self.model.encode_image(batch)
            embeddings = embeddings / embeddings.norm(dim=-1, keepdim=True)

        return embeddings.cpu().numpy().astype("float32")