import os
import sys

from .fs_config import FsConfig, read_fs_config
from .fs_types import FsType
from .fs_constants import LOCAL_FILE_NAME
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter

targets = [
    "fs_config",
    "fs_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
