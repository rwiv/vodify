from .notifier import MockNotifier
from .notifier_untf import UntfNotifier, UntfConfig


def create_notifier(env: str, conf: UntfConfig):
    if env == "prod":
        return UntfNotifier(conf)
    else:
        return MockNotifier()
