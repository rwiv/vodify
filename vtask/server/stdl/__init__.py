import os
import sys

from .stdl_controller import StdlController
from .stdl_task_register_job import StdlTaskRegisterJob
from .stdl_task_registrar import StdlTaskRegistrar

targets = [
    "stdl_controller",
    "stdl_done_job",
    "stdl_task_requester",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
