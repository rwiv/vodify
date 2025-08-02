import json
import os
from pathlib import Path

import pytest
from pydantic import BaseModel
from pyutils import path_join, find_project_root, dirpath

from tests.testutils.test_utils_misc import load_test_dotenv
from vidt.common.status import TaskStatusRepository
from vidt.env import get_celery_env
from vidt.stdl import StdlDoneMsg, StdlMsgQueue

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

celery_env = get_celery_env()

queue = StdlMsgQueue(celery_env.redis)
task_status_repository = TaskStatusRepository(celery_env.redis)


@pytest.mark.asyncio
async def test_publish():
    src_file_path = path_join(find_project_root(), "dev", "archive", "stdl_done.json")
    if not Path(dirpath(src_file_path)).exists():
        os.makedirs(dirpath(src_file_path), exist_ok=True)

    await publish_by_archive(src_file_path, queue)


@pytest.mark.asyncio
async def test_clear_queue():
    await queue.clear_queue()


class StdlQueueState(BaseModel):
    queue_items: list[StdlDoneMsg]


async def publish_by_archive(src_path: str, stdl_queue: StdlMsgQueue):
    with open(src_path, "r") as f:
        message_dicts = json.loads(f.read())
    state = StdlQueueState(**message_dicts)
    for msg in state.queue_items:
        task_uname = f"{msg.platform.value}:{msg.uid}:{msg.video_name}"
        await task_status_repository.delete(task_uname=task_uname)
        await stdl_queue.push(msg)
