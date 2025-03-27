import os
import sys

from .celery_app import app
from .celery_app_run import run
from .celery_tasks import stdl_done_local, stdl_done_remote
from .celery_utils import get_running_tasks, find_active_workers, shutdown_workers
from .celery_redis_broker_client import CeleryRedisBrokerClient
from .celery_constants import LOCAL_WORKER_NAME, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME

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
