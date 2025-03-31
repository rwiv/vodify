import os
import sys

from .stdl_done_helper import StdlDoneHelper
from .stdl_done_queue import StdlDoneQueue

targets = [
    "stdl_done_job",
    "stdl_message_helper",
    "stdl_message_manager",
    "stdl_task_requester",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
