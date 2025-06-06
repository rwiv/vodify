import os
import sys

from .downloader import HlsDownloader
from .hls_url_extractor import HlsUrlExtractor
from .merge import merge_ts, convert_vid, remux_to_mp4
from .parser import parse_master_playlist, parse_media_playlist, Resolution
from .utils import merge_intersected_strings

targets = [
    "downloader",
    "hls_url_extractor",
    "merge",
    "parser",
    "utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
