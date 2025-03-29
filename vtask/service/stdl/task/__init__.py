import os
import sys

from .stdl_message_manager import StdlMessageManager
from .stdl_periodic_job import StdlPeriodicJob
from .stdl_message_helper import StdlMessageHelper

targets = [
    "stdl_job",
    "stdl_message_helper",
    "stdl_message_manager",
    "stdl_periodic_job",
    "stdl_task_requester",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
