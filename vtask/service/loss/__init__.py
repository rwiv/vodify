import os
import sys

from .frame_loss_config import KeyFrameLossConfig, AllFrameLossConfig, read_fl_config
from .frame_loss_inspector import FrameLossInspector
from .frame_loss_inspector_key import KeyFrameLossInspector
from .frame_loss_inspector_all import AllFrameLossInspector

targets = [
    "frame_loss_config",
    "frame_loss_inspector",
    "frame_loss_inspector_all",
    "frame_loss_inspector_key",
    "frame_loss_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
