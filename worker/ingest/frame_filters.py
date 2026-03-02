# ingest/frame_filters.py
import cv2
import numpy as np

from app.config import (
    MIN_BRIGHTNESS,
    MIN_ENTROPY,
    MIN_LAPLACIAN,
    MAX_BLACK_RATIO,
)


def is_bad_frame_bgr(img_bgr: np.ndarray) -> bool:
    """
    Returns True if frame should be discarded.
    Expects BGR image (OpenCV format).
    """
    if img_bgr is None:
        return True

    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    h, w = gray.shape

    if h == 0 or w == 0:
        return True

    mean_brightness = float(gray.mean())

    hist = cv2.calcHist([gray], [0], None, [256], [0, 256]).ravel()
    hist = hist / (hist.sum() + 1e-9)
    entropy = - (hist * np.log2(hist + 1e-9)).sum()

    laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()

    black_ratio = (gray < 10).sum() / (h * w)

    if mean_brightness < MIN_BRIGHTNESS:
        return True

    if entropy < MIN_ENTROPY:
        return True

    if laplacian_var < MIN_LAPLACIAN:
        return True

    if black_ratio > MAX_BLACK_RATIO:
        return True

    return False