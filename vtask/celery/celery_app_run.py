import logging

from pyutils import log

from .celery_app import app
from .celery_worker_deps import deps


def run():
    log.set_level(logging.DEBUG)
    env = deps.read_env()
    app.worker_main(
        [
            "worker",
            "--loglevel=info",
            f"--hostname={f"celery@{env.worker.name}"}",
            f"--queues={env.worker.queues}",
        ]
    )
