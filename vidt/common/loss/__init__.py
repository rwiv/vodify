import os
import sys

from .loss_config import LossCheckSizeConfig, read_loss_config
from .loss_inspector import LossInspector
from .loss_inspector_time import TimeLossInspector
from .loss_inspector_size import SizeLossInspector
from .loss_executor import LossExecutor

targets = [
    "loss_config",
    "loss_inspector",
    "loss_inspector_all",
    "loss_inspector_key",
    "loss_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
