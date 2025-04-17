import os
import sys

from .stdl_transcoder import StdlTranscoder
from .stdl_transcoder_seg import StdlSegmentedMuxer
from .helper.stdl_helper import StdlHelper
from .helper.stdl_helper_utils import create_stdl_helper
from .helper.stdl_helper_s3 import StdlS3Helper
from .helper.stdl_helper_local import StdlLocalHelper


targets = [
    "stdl_helper",
    "stdl_helper_local",
    "stdl_helper_s3",
    "stdl_helper_utils",
    "stdl_transcoder",
    "stdl_transcoder_seg",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
