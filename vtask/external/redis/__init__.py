import os
import sys

from .redis_queue import RedisQueue
from .redis_map import RedisMap

targets = [
    "redis_errors",
    "redis_queue",
    "redis_map",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
