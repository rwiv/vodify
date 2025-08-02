import os
import sys

from .sqs_client import SQSAsyncClient, SQSConfig

targets = [
    "sqs_client",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
