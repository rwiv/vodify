from .celery_app import app
from .celery_worker_deps import deps
from .celery_constants import LOCAL_WORKER_NAME, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME


def run():
    worker_name = deps.read_env().worker.name
    if worker_name == LOCAL_WORKER_NAME:
        queue_name = LOCAL_QUEUE_NAME
    else:
        queue_name = REMOTE_QUEUE_NAME
    host_name = f"celery@{worker_name}"
    app.worker_main(["worker", "--loglevel=info", f"--hostname={host_name}", f"--queues={queue_name}"])
