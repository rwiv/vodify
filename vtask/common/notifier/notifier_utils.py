from .notifier import UntfNotifier, MockNotifier
from ..env import UntfConfig


def create_notifier(env: str, conf: UntfConfig):
    if env == "prod":
        return UntfNotifier(conf)
    else:
        return MockNotifier()
