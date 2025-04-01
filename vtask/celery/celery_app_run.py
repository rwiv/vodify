from .celery_app import app
from .celery_worker_deps import deps
from .celery_constants import (
    DEFAULT_WORKER_NAME,
    DEFAULT_QUEUE_NAME,
    IO_LFS_WORKER_NAME,
    IO_LFS_QUEUE_NAME,
    IO_NET_WORKER_NAME,
    IO_NET_QUEUE_NAME,
    CPU_SINGLE_WORKER_NAME,
    CPU_SINGLE_QUEUE_NAME,
    CPU_MULTIPLE_WORKER_NAME,
    CPU_MULTIPLE_QUEUE_NAME,
)


def run():
    worker_name = deps.read_env().worker.name
    if worker_name == DEFAULT_WORKER_NAME:
        queue_name = DEFAULT_QUEUE_NAME
    elif worker_name == IO_LFS_WORKER_NAME:
        queue_name = IO_LFS_QUEUE_NAME
    elif worker_name == IO_NET_WORKER_NAME:
        queue_name = IO_NET_QUEUE_NAME
    elif worker_name == CPU_SINGLE_WORKER_NAME:
        queue_name = CPU_SINGLE_QUEUE_NAME
    elif worker_name == CPU_MULTIPLE_WORKER_NAME:
        queue_name = CPU_MULTIPLE_QUEUE_NAME
    else:
        raise ValueError(f"Unknown worker name: {worker_name}")
    host_name = f"celery@{worker_name}"
    app.worker_main(["worker", "--loglevel=info", f"--hostname={host_name}", f"--queues={queue_name}"])
