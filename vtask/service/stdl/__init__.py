import os
import sys

from .stdl_muxer import StdlMuxer
from .stdl_types import StdlDoneMsg, StdlDoneStatus, StdlPlatformType
from .stdl_helper import StdlHelper
from .stdl_helper_utils import create_stdl_helper
from .stdl_helper_s3 import StdlS3Helper
from .stdl_helper_local import StdlLocalHelper

targets = [
    "stdl_constants",
    "stdl_mux_helper",
    "stdl_mux_helper_local",
    "stdl_mux_helper_s3",
    "stdl_mux_helper_utils",
    "stdl_muxer",
    "stdl_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
