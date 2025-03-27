import os
import sys

from .env_batch import BatchEnv, UntfConfig
from .env_celery import CeleryEnv, get_celery_env
from .env_common_configs import AmqpConfig, RedisConfig
from .env_server import ServerEnv, get_server_env
from .env_worker import WorkerEnv, get_worker_env

targets = [
    "env_celery",
    "env_common_configs",
    "env_server",
    "env_worker",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
