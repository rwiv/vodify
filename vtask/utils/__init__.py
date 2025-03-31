import os
import sys

from .file import check_dir, read_dir_recur
from .s3 import S3Client
from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from .s3_utils import disable_warning_log
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter
from .redis_queue import RedisQueue

targets = [
    "file",
    "object_writer",
    "s3",
    "s3_responses",
    "s3_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
