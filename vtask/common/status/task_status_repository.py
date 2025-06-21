from enum import Enum

from pyutils import log
from redis import Redis

from ...env import RedisConfig
from ...external.redis import RedisMap


REDIS_TASK_STATUS_KEY_PREFIX = "vtask:task:status"


class TaskStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class TaskStatusRepository:
    def __init__(
        self,
        conf: RedisConfig,
        start_ex_sec: int = 3 * 24 * 60 * 60,  # 3 days
        done_ex_sec: int = 3 * 24 * 60 * 60,  # 3 days
    ):
        self.__prefix = REDIS_TASK_STATUS_KEY_PREFIX
        self.__redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
        self.__map = RedisMap(client=self.__redis)
        self.__start_ex_sec = start_ex_sec
        self.__done_ex_sec = done_ex_sec

    def check(self, task_uname: str) -> dict | None:
        task_status = self.get(task_uname=task_uname)
        if task_status == TaskStatus.PENDING:
            message = "Task already pending"
            log.debug(message, {"task_uname": task_uname})
            return {"message": message, "task_uname": task_uname}
        elif task_status == TaskStatus.SUCCESS:
            message = "Task already completed"
            log.debug(message, {"task_uname": task_uname})
            return {"message": message, "task_uname": task_uname}
        elif task_status == TaskStatus.FAILURE:
            log.debug(f"Retry failed task", {"task_uname": task_uname})
            return None

    def set_pending(self, task_uname: str):
        if self.exists(task_uname=task_uname):
            raise ValueError(f"Task {task_uname} already exists")
        self.__map.set(key=self.__get_key(task_uname=task_uname), value=TaskStatus.PENDING.value, ex=self.__start_ex_sec)

    def set_success(self, task_uname: str):
        exists = self.get(task_uname=task_uname)
        if exists is None:
            raise ValueError(f"Task {task_uname} does not exist")
        if exists != TaskStatus.PENDING:
            raise ValueError(f"Task {task_uname} is not pending")
        self.__map.set(key=self.__get_key(task_uname=task_uname), value=TaskStatus.SUCCESS.value, ex=self.__done_ex_sec)

    def set_failure(self, task_uname: str):
        exists = self.get(task_uname=task_uname)
        if exists is None:
            raise ValueError(f"Task {task_uname} does not exist")
        if exists != TaskStatus.PENDING:
            raise ValueError(f"Task {task_uname} is not pending")
        self.__map.set(key=self.__get_key(task_uname=task_uname), value=TaskStatus.FAILURE.value, ex=self.__done_ex_sec)

    def get(self, task_uname: str) -> TaskStatus | None:
        text = self.__map.get(key=self.__get_key(task_uname=task_uname))
        if text is None:
            return None
        else:
            return TaskStatus(text)

    def exists(self, task_uname: str) -> bool:
        return self.__map.exists(key=self.__get_key(task_uname=task_uname))

    def delete(self, task_uname: str):
        self.__map.delete(key=self.__get_key(task_uname=task_uname))

    def __get_key(self, task_uname: str) -> str:
        return f"{self.__prefix}:{task_uname}"
