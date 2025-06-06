import os
import sys

from .schema.video_schema import VideoPlatform, VideoDownloadContext
from .video_downloader import VideoDownloader
from .batch.video_download_config import VideoDownloadConfig
from .batch.video_download_executor import VideoDownloadExecutor
from .chzzk.chzzk_video_client_2 import ChzzkVideoClient2
from .chzzk.chzzk_video_client_1 import ChzzkVideoClient1
from .soop.soop_video_client import SoopVideoClient
from .soop.soop_hls_url_extractor import SoopHlsUrlExtractor

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
