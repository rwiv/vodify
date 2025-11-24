import asyncio

from .stdl_task_registrar import StdlTaskRegistrar
from ...celery import CeleryRedisBrokerClient, find_active_worker_names, app
from ...common.job import Job
from ...external.redis import RedisConfig
from ...stdl import StdlDoneMsg, StdlMsgQueue

STDL_TASK_REGISTER_JOB_NAME = "stdl_task_register_job"


class StdlTaskRegisterJob(Job):
    def __init__(
        self,
        redis_conf: RedisConfig,
        registrar: StdlTaskRegistrar,
        celery_redis: CeleryRedisBrokerClient,
        received_task_threshold: int = 1,
    ):
        super().__init__(name=STDL_TASK_REGISTER_JOB_NAME)

        self.__queue = StdlMsgQueue(redis_conf)
        self.__registrar = registrar
        self.__celery_redis = celery_redis

        self.received_task_threshold = received_task_threshold

    async def run(self):
        workers = await asyncio.to_thread(find_active_worker_names, app)
        if len(workers) == 0:
            return

        msg: StdlDoneMsg | None = await self.__queue.get()
        if msg is None:
            return

        queue_name = self.__registrar.resolve_queue(msg)
        received_tasks = await self.__celery_redis.get_received_tasks(queue_name)
        if len(received_tasks) < self.received_task_threshold:
            self.__registrar.register(msg, queue_name)
            await self.__queue.pop()
