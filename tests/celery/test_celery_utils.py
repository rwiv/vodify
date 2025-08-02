import asyncio

import pytest
from pyutils import load_dotenv, path_join, find_project_root

# load_dotenv(path_join(find_project_root(), "dev", ".env-worker-dev"))
load_dotenv(path_join(find_project_root(), "dev", ".env-worker-prod"))

from vidt.celery import app, get_running_tasks, find_active_worker_names


def test_inspect_tasks():
    print()
    for task in get_running_tasks(app):
        print(task)

    # active_worker_dict = get_task_map(app)
    # if not active_worker_dict:
    #     return
    # for work_name, tasks in active_worker_dict.items():
    #     print(work_name)
    #     for task in tasks:
    #         print(json.dumps(task))
    #         print(TaskInfo.from_dict(task))

    # task_id = ""
    # result = get_result(app, task_id)
    # print(result)
    # done_result: StdlDoneTaskResult = result.result
    # print(done_result["message"])


@pytest.mark.asyncio
async def test_inspect_worker_names():
    print()
    for worker in await asyncio.to_thread(find_active_worker_names, app):
        print(worker)
