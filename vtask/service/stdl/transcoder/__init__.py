import os
import sys

from .stdl_transcoder import StdlTranscoder, StdlDoneTaskResult
from .segment_accessor.stdl_segment_accessor import StdlSegmentAccessor
from .segment_accessor.stdl_segment_accessor_s3 import StdlS3SegmentAccessor
from .segment_accessor.stdl_segment_accessor_local import StdlLocalSegmentAccessor
from .segment_accessor.stdl_segment_accessor_utils import create_stdl_accessor


targets = [
    "segment_accessor",
    "stdl_transcoder",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
