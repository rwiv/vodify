import os
import sys

from .celery_app import app
from .celery_app_run import run
from .celery_tasks import recnode_transcode
from .celery_utils import *
from .celery_redis_broker_client import CeleryRedisBrokerClient
from .celery_constants import *

targets = [
    # "celery_app",  # Commented out for import in flower
    "celery_app_run",
    "celery_constants",
    "celery_redis_broker_client",
    "celery_task_types",
    # "celery_tasks",  # Commented out for import in celery app
    "celery_utils",
    "celery_worker_deps",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
