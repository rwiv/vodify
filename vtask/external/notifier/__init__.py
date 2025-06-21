import os
import sys

from .notifier import Notifier, MockNotifier
from .notifier_untf import UntfConfig, UntfNotifier
from .notifier_utils import create_notifier

targets = [
    "notifier",
    "notifier_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
