import os
import sys

from .cron_job import CronJob
from .job_spec import Job

targets = [
    "cron_job",
    "job_spec",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
