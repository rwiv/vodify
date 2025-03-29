import os
import sys

from .stdl_muxer import StdlMuxer
from .stdl_helper import StdlHelper
from .stdl_helper_utils import create_stdl_helper
from .stdl_helper_s3 import StdlS3Helper
from .stdl_helper_local import StdlLocalHelper

targets = [
    "stdl_helper",
    "stdl_helper_local",
    "stdl_helper_s3",
    "stdl_helper_utils",
    "stdl_muxer",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
