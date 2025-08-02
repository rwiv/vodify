import os
import sys

from .hls_downloader import HlsDownloader
from .hls_url_extractor import HlsUrlExtractor
from .hls_merge import merge_ts
from .hls_parser import parse_master_playlist, parse_media_playlist, Resolution

targets = [
    "downloader",
    "url_extractor",
    "merge",
    "parser",
    "utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
