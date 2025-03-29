import os
import sys

from .stdl_constrants import *
from .stdl_types import StdlDoneMsg, StdlDoneStatus, StdlPlatformType

targets = [
    "stdl_constants",
    "stdl_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
