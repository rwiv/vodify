import pytest

from tests.testutils.test_utils_misc import load_test_dotenv
from vidt.common.status import TaskStatusRepository, TaskStatus
from vidt.env import get_celery_env

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

celery_env = get_celery_env()

repo = TaskStatusRepository(celery_env.redis)


@pytest.mark.asyncio
async def test_repository():
    task_uname = "test_task"
    assert not await repo.exists(task_uname=task_uname)
    assert await repo.get(task_uname=task_uname) is None
    await repo.set_pending(task_uname=task_uname)
    assert await repo.exists(task_uname=task_uname)
    assert await repo.get(task_uname=task_uname) == TaskStatus.PENDING
    await repo.delete(task_uname=task_uname)
    assert not await repo.exists(task_uname=task_uname)
    assert await repo.get(task_uname=task_uname) is None
