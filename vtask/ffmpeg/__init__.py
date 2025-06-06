import os
import sys

from .ffmpeg_remuxing import remux_video, concat_streams, concat_by_list
from .ffprobe_stream import get_info

targets = [
    "ffmpeg_remuxing",
    "ffmpeg_utils",
    "ffmpeg_stream",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
