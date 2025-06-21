import os
import sys

from .redis_queue_sync import RedisQueue
from .redis_string_sync import RedisString
from .redis_types import RedisConfig

targets = [
    "redis_errors",
    "redis_queue_async",
    "redis_queue_sync",
    "redis_string_async",
    "redis_string_sync",
    "redis_types",
    "redis_utils_async",
    "redis_utils_sync",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
