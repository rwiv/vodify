from celery import Celery
from celery.result import AsyncResult

from .celery_task_types import TaskDict, TaskResult, TaskInfo


def find_active_worker_names(app: Celery) -> list[str]:
    inspector = app.control.inspect()
    active_workers = inspector.ping()  # 연결된 모든 worker의 상태를 가져옴
    if not active_workers:
        return []
    else:
        return list(active_workers.keys())


def shutdown_workers(app: Celery):
    app.control.broadcast("shutdown")


def get_running_tasks(app: Celery) -> list[TaskInfo]:
    task_dict = app.control.inspect().active()
    if not task_dict:
        return []
    return get_tasks(task_dict)


def get_prefetched_tasks(app: Celery) -> list[TaskInfo]:
    task_dict = app.control.inspect().reserved()
    if not task_dict:
        return []
    return get_tasks(task_dict)


def get_tasks(task_dict: dict) -> list[TaskInfo]:
    result = []
    for worker_name in task_dict.keys():
        tasks: list[TaskDict] = task_dict[worker_name]
        for task in tasks:
            result.append(TaskInfo.from_dict(task))
    return result


def get_result(app: Celery, task_id: str) -> TaskResult:
    return TaskResult.from_async_result(AsyncResult(task_id, app=app))
