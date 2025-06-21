import os
import sys

from .file import (
    stem,
    write_file,
    check_dir,
    check_dir_async,
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
from .process import check_which, check_which_async, check_returncode, run_process, exec_process
from .stats import avg
from .time import cur_duration
from .yaml import write_yaml_file

targets = [
    "file",
    "http",
    "limiter",
    "process",
    "stats",
    "time",
    "yaml",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
