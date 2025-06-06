import os
import sys

from .accessor.stdl_segment_accessor_local import StdlLocalSegmentAccessor
from .accessor.stdl_segment_accessor_s3 import StdlS3SegmentAccessor
from .accessor.stdl_segment_accessor_utils import create_stdl_accessor
from .archiver.stdl_archive_executor import StdlArchiveExecutor
from .archiver.stdl_archiver import StdlArchiver, ArchiveTarget
from .common.stdl_done_queue import StdlDoneQueue
from .loss.stdl_loss_inspector import StdlLossInspector
from .schema.stdl_constrants import STDL_INCOMPLETE_DIR_NAME
from .schema.stdl_types import StdlDoneMsg, StdlDoneStatus, StdlPlatformType, StdlSegmentsInfo
from .transcoder.stdl_transcoder import StdlTranscoder, StdlDoneTaskResult

targets = [
    "archiver",
    "common",
    "loss",
    "schema",
    "transcoder",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
