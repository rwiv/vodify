import os
import sys

from .schema.video_schema import VideoPlatform, VideoDownloadContext
from .video_downloader import VideoDownloader
from .batch.video_download_config import VideoDownloadConfig
from .batch.video_download_executor import VideoDownloadExecutor

targets = [
    "batch",
    "chzzk",
    "schema",
    "soop",
    "ytdl",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
