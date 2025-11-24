import json

from .recnode_task_registrar import RecnodeTaskRegistrar
from ...common.job import Job
from ...external.redis import RedisConfig
from ...external.sqs import SQSAsyncClient
from ...recnode import RecnodeMsg, RecnodeMsgQueue, RecnodeDoneStatus

RECNODE_MSG_CONSUME_JOB_NAME = "recnode_msg_consume_job"


class RecnodeMsgConsumeJob(Job):
    def __init__(
        self,
        redis_conf: RedisConfig,
        sqs: SQSAsyncClient,
        registrar: RecnodeTaskRegistrar,
    ):
        super().__init__(name=RECNODE_MSG_CONSUME_JOB_NAME)
        self.__sqs = sqs
        self.__queue = RecnodeMsgQueue(redis_conf)
        self.__registrar = registrar

    async def run(self):
        messages = await self.__sqs.receive()

        for recnode_msg in [RecnodeMsg(**json.loads(msg.body)) for msg in messages]:
            if recnode_msg.status == RecnodeDoneStatus.COMPLETE:
                await self.__queue.push(recnode_msg)
            elif recnode_msg.status == RecnodeDoneStatus.CANCELED:
                queue_name = self.__registrar.resolve_queue(recnode_msg)
                self.__registrar.register(recnode_msg, queue_name)

        if len(messages) > 0:
            await self.__sqs.delete(messages)
