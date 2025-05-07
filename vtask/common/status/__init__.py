import os
import sys

from .task_status_repository import TaskStatusRepository, TaskStatus


targets = [
    "task_status_repository",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
