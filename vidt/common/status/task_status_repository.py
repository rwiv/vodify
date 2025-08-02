from enum import Enum

from pyutils import log

from ...external.redis import RedisString, RedisConfig, create_redis_client

REDIS_TASK_STATUS_KEY_PREFIX = "vidt:task:status"


class TaskStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class TaskStatusRepository:
    def __init__(
        self,
        redis_conf: RedisConfig,
        start_ex_sec: int = 3 * 24 * 60 * 60,  # 3 days
        done_ex_sec: int = 3 * 24 * 60 * 60,  # 3 days
    ):
        self.__prefix = REDIS_TASK_STATUS_KEY_PREFIX
        self.__str = RedisString(client=create_redis_client(redis_conf))
        self.__start_ex_sec = start_ex_sec
        self.__done_ex_sec = done_ex_sec

    async def check(self, task_uname: str) -> dict | None:
        task_status = await self.get(task_uname=task_uname)
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

    async def set_pending(self, task_uname: str):
        if await self.exists(task_uname=task_uname):
            raise ValueError(f"Task {task_uname} already exists")
        await self.__str.set(
            key=self.__get_key(task_uname=task_uname),
            value=TaskStatus.PENDING.value,
            ex=self.__start_ex_sec,
        )

    async def set_success(self, task_uname: str):
        exists = await self.get(task_uname=task_uname)
        if exists is None:
            raise ValueError(f"Task {task_uname} does not exist")
        if exists != TaskStatus.PENDING:
            raise ValueError(f"Task {task_uname} is not pending")
        await self.__str.set(
            key=self.__get_key(task_uname=task_uname),
            value=TaskStatus.SUCCESS.value,
            ex=self.__done_ex_sec,
        )

    async def set_failure(self, task_uname: str):
        exists = await self.get(task_uname=task_uname)
        if exists is None:
            raise ValueError(f"Task {task_uname} does not exist")
        if exists != TaskStatus.PENDING:
            raise ValueError(f"Task {task_uname} is not pending")
        await self.__str.set(
            key=self.__get_key(task_uname=task_uname),
            value=TaskStatus.FAILURE.value,
            ex=self.__done_ex_sec,
        )

    async def get(self, task_uname: str) -> TaskStatus | None:
        text = await self.__str.get(key=self.__get_key(task_uname=task_uname))
        if text is None:
            return None
        else:
            return TaskStatus(text)

    async def exists(self, task_uname: str) -> bool:
        return await self.__str.exists(key=self.__get_key(task_uname=task_uname))

    async def delete(self, task_uname: str):
        await self.__str.delete(key=self.__get_key(task_uname=task_uname))

    def __get_key(self, task_uname: str) -> str:
        return f"{self.__prefix}:{task_uname}"
