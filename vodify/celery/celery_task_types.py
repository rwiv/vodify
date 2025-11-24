from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TypedDict, Optional, Any

from celery.result import AsyncResult


class TaskState(Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    RETRY = "RETRY"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"


class DeliveryInfo(TypedDict):
    exchange: str
    routing_key: str
    priority: int
    redelivered: bool


class TaskDict(TypedDict):
    id: str
    name: str
    args: list[dict]
    kwargs: dict
    type: str
    hostname: str
    time_start: float | None
    acknowledged: bool
    delivery_info: DeliveryInfo
    worker_pid: int


@dataclass
class TaskInfo:
    task_id: str
    task_name: str  # e.g. "vodify.recnode.transcode"
    task_args: list[Any]
    task_kwargs: dict
    worker_name: str  # e.g. "celery@worker-1"
    worker_pid: int
    started_at: datetime | None
    acknowledged: bool

    @staticmethod
    def from_dict(task_dict: TaskDict) -> "TaskInfo":
        started_at = None
        time_start = task_dict.get("time_start")
        if time_start is not None:
            started_at = datetime.fromtimestamp(time_start)
        return TaskInfo(
            task_id=task_dict["id"],
            task_name=task_dict["name"],
            task_args=task_dict["args"],
            task_kwargs=task_dict["kwargs"],
            worker_name=task_dict["hostname"],
            worker_pid=task_dict["worker_pid"],
            started_at=started_at,
            acknowledged=task_dict["acknowledged"],
        )


@dataclass
class TaskResult:
    task_id: str
    state: TaskState
    result: Any
    ignored: bool
    retries: Optional[Any]
    date_done: datetime

    @staticmethod
    def from_async_result(result: AsyncResult) -> "TaskResult":
        if result.task_id is None:
            raise ValueError("Task ID is required")
        return TaskResult(
            task_id=result.task_id,
            state=TaskState(result.state),
            result=result.result,
            ignored=result.ignored,
            retries=result.retries,
            date_done=result.date_done,
        )
