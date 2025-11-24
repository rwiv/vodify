import asyncio

from .recnode_task_registrar import RecnodeTaskRegistrar
from ...celery import CeleryRedisBrokerClient, find_active_worker_names, app
from ...common.job import Job
from ...external.redis import RedisConfig
from ...recnode import RecnodeMsg, RecnodeMsgQueue

RECNODE_TASK_REGISTER_JOB_NAME = "recnode_task_register_job"


class RecnodeTaskRegisterJob(Job):
    def __init__(
        self,
        redis_conf: RedisConfig,
        registrar: RecnodeTaskRegistrar,
        celery_redis: CeleryRedisBrokerClient,
        received_task_threshold: int = 1,
    ):
        super().__init__(name=RECNODE_TASK_REGISTER_JOB_NAME)

        self.__queue = RecnodeMsgQueue(redis_conf)
        self.__registrar = registrar
        self.__celery_redis = celery_redis

        self.received_task_threshold = received_task_threshold

    async def run(self):
        workers = await asyncio.to_thread(find_active_worker_names, app)
        if len(workers) == 0:
            return

        msg: RecnodeMsg | None = await self.__queue.get()
        if msg is None:
            return

        queue_name = self.__registrar.resolve_queue(msg)
        received_tasks = await self.__celery_redis.get_received_tasks(queue_name)
        if len(received_tasks) < self.received_task_threshold:
            self.__registrar.register(msg, queue_name)
            await self.__queue.pop()
