import os
import sys

from .stdl_transcoder import StdlTranscoder, StdlDoneTaskResult
from .accessor.stdl_accessor import StdlAccessor
from .accessor.stdl_accessor_utils import create_stdl_helper
from .accessor.stdl_accessor_s3 import StdlS3Accessor
from .accessor.stdl_accessor_local import StdlLocalAccessor


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
