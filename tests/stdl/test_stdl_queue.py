import json
import os
from pathlib import Path

from pydantic import BaseModel
from pyutils import path_join, find_project_root, dirpath

from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.common.env import get_celery_env
from vtask.stdl import StdlDoneMsg, StdlDoneQueue

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

celery_env = get_celery_env()

queue = StdlDoneQueue(celery_env.redis)


def test_publish():
    src_file_path = path_join(find_project_root(), "dev", "archive", "stdl_done.json")
    if not Path(dirpath(src_file_path)).exists():
        os.makedirs(dirpath(src_file_path), exist_ok=True)

    publish_by_archive(src_file_path, queue)


def test_clear_queue():
    queue.clear_queue()


class StdlQueueState(BaseModel):
    queue_items: list[StdlDoneMsg]


def publish_by_archive(src_path: str, done_queue: StdlDoneQueue):
    with open(src_path, "r") as f:
        message_dicts = json.loads(f.read())
    state = StdlQueueState(**message_dicts)
    for msg in state.queue_items:
        done_queue.push(msg)
