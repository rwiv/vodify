import os
import sys

from .accessor.segment_accessor_local import LocalSegmentAccessor
from .accessor.segment_accessor_s3 import S3SegmentAccessor
from .accessor.segment_accessor_utils import create_recnode_accessor
from .archiver.recnode_archive_executor import RecnodeArchiveExecutor
from .archiver.recnode_archiver import RecnodeArchiver, ArchiveTarget
from .common.recnode_msg_queue import RecnodeMsgQueue
from .schema.recnode_constrants import RECNODE_INCOMPLETE_DIR_NAME
from .schema.recnode_types import RecnodeMsg, RecnodeDoneStatus, RecnodePlatformType, RecnodeSegmentsInfo
from .transcoder.recnode_transcoder import RecnodeTranscoder, RecnodeDoneTaskResult

targets = [
    "accessor",
    "archiver",
    "common",
    "schema",
    "transcoder",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
