import os
import sys

from .stdl_done_queue import StdlDoneQueue

targets = [
    "stdl_message_queue",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
