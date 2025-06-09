import os
import sys

from .file import (
    stem,
    write_file,
    check_dir,
    ensure_dir,
    read_dir_recur,
    move_directory_not_recur,
    listdir_recur,
    rmtree,
    move_file,
    copy_file,
    copy_file2,
    open_tar,
    utime,
)
from .http import get_headers, fetch_text, fetch_json
from .limiter import nio_limiter
from .s3_async import S3AsyncClient
from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter
from .process import check_which, check_which_async, check_returncode, run_process, exec_process
from .redis.redis_queue import RedisQueue
from .redis.redis_map import RedisMap
from .stats import avg
from .time import cur_duration
from .yaml import write_yaml_file

targets = [
    "file",
    "http",
    "limiter",
    "object_writer",
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
