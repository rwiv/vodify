from celery import Celery
from celery.result import AsyncResult

from .celery_task_types import TaskDict, TaskResult, TaskInfo


def find_active_workers(app: Celery):
    inspector = app.control.inspect()
    active_workers = inspector.ping()  # 연결된 모든 worker의 상태를 가져옴
    if not active_workers:
        return []
    return list(active_workers.keys())


def shutdown_workers(app: Celery):
    app.control.broadcast("shutdown")


def get_running_tasks(app: Celery) -> list[TaskInfo]:
    worker_dict = app.control.inspect().active()
    if not worker_dict:
        return []
    result = []
    for worker_name in worker_dict.keys():
        tasks: list[TaskDict] = worker_dict[worker_name]
        for task in tasks:
            result.append(TaskInfo.from_dict(task))
    return result


def get_result(app: Celery, task_id: str) -> TaskResult:
    return TaskResult.from_async_result(AsyncResult(task_id, app=app))
