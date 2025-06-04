import os
import sys

from .file import check_dir, ensure_dir, read_dir_recur, move_directory_not_recur
from .http import get_headers
from .s3 import S3Client
from .s3_async import S3AsyncClient
from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from .s3_utils import disable_warning_log
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter
from .redis.redis_queue import RedisQueue
from .redis.reids_map import RedisMap
from .yaml import write_yaml_file

targets = [
    "file",
    "object_writer",
    "s3",
    "s3_async",
    "s3_responses",
    "s3_utils",
    "redis",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
