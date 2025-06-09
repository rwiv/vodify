import os
import sys

from .encoding_request import EncodingRequest
from .encoding_resolver import resolve_command
from .progress_parser import ProgressInfo, parse_encoding_progress
from .video_encoder import VideoEncoder

targets = [
    "encoding_request",
    "encoding_resolver",
    "progress_parser",
    "video_encoder",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
