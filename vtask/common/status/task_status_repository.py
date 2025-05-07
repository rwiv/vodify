from enum import Enum

from redis import Redis

from ...common.env import RedisConfig
from ...utils import RedisMap


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

    def set_pending(self, task_uname: str):
        key = f"{self.__prefix}:{task_uname}"
        self.__map.set(key=key, value=TaskStatus.PENDING.value, ex=self.__start_ex_sec)

    def set_success(self, task_uname: str):
        key = f"{self.__prefix}:{task_uname}"
        self.__map.set(key=key, value=TaskStatus.SUCCESS.value, ex=self.__done_ex_sec)

    def set_failure(self, task_uname: str):
        key = f"{self.__prefix}:{task_uname}"
        self.__map.set(key=key, value=TaskStatus.FAILURE.value, ex=self.__done_ex_sec)

    def get(self, task_uname: str) -> TaskStatus | None:
        key = f"{self.__prefix}:{task_uname}"
        text = self.__map.get(key=key)
        if text is None:
            return None
        else:
            return TaskStatus(text)

    def exists(self, task_uname: str) -> bool:
        key = f"{self.__prefix}:{task_uname}"
        return self.__map.exists(key=key)

    def delete(self, task_uname: str):
        key = f"{self.__prefix}:{task_uname}"
        self.__map.delete(key=key)
