import json
import os
from pathlib import Path

import pytest
from pydantic import BaseModel
from pyutils import path_join, find_project_root, dirpath

from tests.testutils.test_utils_misc import load_test_dotenv
from vodify.common.status import TaskStatusRepository
from vodify.env import get_celery_env
from vodify.recnode import RecnodeMsg, RecnodeMsgQueue

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

celery_env = get_celery_env()

queue = RecnodeMsgQueue(celery_env.redis)
task_status_repository = TaskStatusRepository(celery_env.redis)


@pytest.mark.asyncio
async def test_publish():
    src_file_path = path_join(find_project_root(), "dev", "archive", "recnode_done.json")
    if not Path(dirpath(src_file_path)).exists():
        os.makedirs(dirpath(src_file_path), exist_ok=True)

    await publish_by_archive(src_file_path, queue)


@pytest.mark.asyncio
async def test_clear_queue():
    await queue.clear_queue()


class RecnodeQueueState(BaseModel):
    queue_items: list[RecnodeMsg]


async def publish_by_archive(src_path: str, recnode_queue: RecnodeMsgQueue):
    with open(src_path, "r") as f:
        message_dicts = json.loads(f.read())
    state = RecnodeQueueState(**message_dicts)
    for msg in state.queue_items:
        task_uname = f"{msg.platform.value}:{msg.uid}:{msg.video_name}"
        await task_status_repository.delete(task_uname=task_uname)
        await recnode_queue.push(msg)
