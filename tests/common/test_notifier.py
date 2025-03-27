from pyutils import load_dotenv, path_join, find_project_root

load_dotenv(path_join(find_project_root(), "dev", ".env-batch-dev"))

from vtask.common.env.env_batch import get_batch_env
from vtask.common.notifier import UntfNotifier

env = get_batch_env()


def test_untf():
    print(env)
    notifier = UntfNotifier(env.untf)
    notifier.notify(env.untf.topic, "test")
