import os
import sys

from .s3_client import S3AsyncClient
from .s3_types import S3Config, S3ListResponse
from .s3_utils import create_client

targets = [
    "s3_client",
    "s3_types",
    "s3_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
