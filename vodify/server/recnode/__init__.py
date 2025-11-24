import os
import sys

from .recnode_controller import RecnodeController
from .recnode_task_register_job import RecnodeTaskRegisterJob
from .recnode_msg_consume_job import RecnodeMsgConsumeJob
from .recnode_task_registrar import RecnodeTaskRegistrar

targets = [
    "recnode_controller",
    "recnode_done_job",
    "recnode_task_requester",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
