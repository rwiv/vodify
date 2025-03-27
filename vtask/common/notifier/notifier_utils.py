from .notifier import UntfNotifier, MockNotifier
from ..env import BatchEnv


def create_notifier(env: BatchEnv):
    if env.env == "prod":
        return UntfNotifier(env.untf)
    else:
        return MockNotifier()
