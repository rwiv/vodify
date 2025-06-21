from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.common.status import TaskStatusRepository, TaskStatus
from vtask.env import get_celery_env

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

celery_env = get_celery_env()

repo = TaskStatusRepository(celery_env.redis)


def test_repository():
    task_uname = "test_task"
    assert repo.exists(task_uname=task_uname) == False
    assert repo.get(task_uname=task_uname) is None
    repo.set_pending(task_uname=task_uname)
    assert repo.exists(task_uname=task_uname)
    assert repo.get(task_uname=task_uname) == TaskStatus.PENDING
    repo.delete(task_uname=task_uname)
    assert repo.exists(task_uname=task_uname) == False
    assert repo.get(task_uname=task_uname) is None
