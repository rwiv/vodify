import os
import sys

from .stdl_message_manager import StdlMessageManager

targets = [
    "stdl_constants",
    "stdl_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
