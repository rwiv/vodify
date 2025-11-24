import json

from .stdl_task_registrar import StdlTaskRegistrar
from ...common.job import Job
from ...external.redis import RedisConfig
from ...external.sqs import SQSAsyncClient
from ...stdl import StdlDoneMsg, StdlMsgQueue, StdlDoneStatus

STDL_MSG_CONSUME_JOB_NAME = "stdl_msg_consume_job"


class StdlMsgConsumeJob(Job):
    def __init__(
        self,
        redis_conf: RedisConfig,
        sqs: SQSAsyncClient,
        registrar: StdlTaskRegistrar,
    ):
        super().__init__(name=STDL_MSG_CONSUME_JOB_NAME)
        self.__sqs = sqs
        self.__queue = StdlMsgQueue(redis_conf)
        self.__registrar = registrar

    async def run(self):
        messages = await self.__sqs.receive()

        for stdl_msg in [StdlDoneMsg(**json.loads(msg.body)) for msg in messages]:
            if stdl_msg.status == StdlDoneStatus.COMPLETE:
                await self.__queue.push(stdl_msg)
            elif stdl_msg.status == StdlDoneStatus.CANCELED:
                queue_name = self.__registrar.resolve_queue(stdl_msg)
                self.__registrar.register(stdl_msg, queue_name)

        if len(messages) > 0:
            await self.__sqs.delete(messages)
