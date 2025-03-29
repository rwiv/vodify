import os
import sys

from .stdl_constants import STDL_DONE_QUEUE
from .stdl_controller import StdlController
from .stdl_listener import StdlListener
from .stdl_periodic_task import StdlPeriodicTask
from .stdl_done_message_manager import StdlDoneMessageManager

targets = ["stdl_constants", "stdl_controller", "stdl_listener"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
