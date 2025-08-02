import os
import sys

from .redis_queue import RedisQueue
from .redis_string import RedisString
from .redis_types import RedisConfig
from .redis_utils import create_app_redis_client, create_celery_redis_client

targets = [
    "redis_errors",
    "redis_queue",
    "redis_string",
    "redis_types",
    "redis_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
