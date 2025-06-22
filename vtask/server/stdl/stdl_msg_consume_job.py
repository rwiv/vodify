import json

from ...common.job import Job
from ...external.redis import RedisConfig
from ...external.sqs import SQSAsyncClient
from ...stdl import StdlDoneMsg, StdlMsgQueue

STDL_MSG_CONSUME_JOB_NAME = "stdl_msg_consume_job"


class StdlMsgConsumeJob(Job):
    def __init__(self, redis_conf: RedisConfig, sqs: SQSAsyncClient, request_delay_sec: float = 3):
        super().__init__(name=STDL_MSG_CONSUME_JOB_NAME)
        self.__sqs = sqs
        self.__queue = StdlMsgQueue(redis_conf)
        self.request_delay_sec = request_delay_sec

    async def run(self):
        messages = await self.__sqs.receive()

        for stdl_msg in [StdlDoneMsg(**json.loads(msg.body)) for msg in messages]:
            await self.__queue.push(stdl_msg)

        if len(messages) > 0:
            await self.__sqs.delete(messages)
