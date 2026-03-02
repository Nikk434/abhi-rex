# ingest/video.py
import cv2
from typing import List, Tuple, Optional


def detect_shots_and_keyframes(video_path: str) -> List[Tuple[float, int]]:
    """
    Returns a list of (timestamp_seconds, frame_number)
    Uses scenedetect if available, otherwise falls back to uniform sampling.
    """
    try:
        from scenedetect import VideoManager, SceneManager
        from scenedetect.detectors import ContentDetector
    except Exception:
        # fallback: uniform sampling every ~2 seconds
        cap = cv2.VideoCapture(video_path)
        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        interval = int(max(1, fps * 2.0))
        print("VIDEO OPENED:", cap.isOpened())
        print("FPS:", cap.get(cv2.CAP_PROP_FPS))
        print("FRAME COUNT:", cap.get(cv2.CAP_PROP_FRAME_COUNT))

        keyframes = []
        for f in range(0, total_frames, interval):
            keyframes.append((f / fps, f))

        cap.release()
        return keyframes

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


def read_frame_at(
    video_path: str,
    frame_num: int
) -> Tuple[Optional[any], Optional[float]]:
    """
    Reads a frame at an absolute frame number.
    Returns (frame_bgr, timestamp_seconds)
    """
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0

    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
    ret, frame = cap.read()
    cap.release()

    if not ret:
        return None, None

    timestamp = frame_num / fps
    return frame, timestamp