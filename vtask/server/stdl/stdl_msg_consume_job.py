import asyncio
import json

from ...common.job import Job
from ...external.sqs import SQSAsyncClient
from ...stdl import StdlDoneMsg, StdlMsgQueue

STDL_MSG_CONSUME_JOB_NAME = "stdl_msg_consume_job"


class StdlMsgConsumeJob(Job):
    def __init__(self, sqs: SQSAsyncClient, queue: StdlMsgQueue, request_delay_sec: float = 3):
        super().__init__(name=STDL_MSG_CONSUME_JOB_NAME)

        self.__sqs = sqs
        self.__queue = queue
        self.request_delay_sec = request_delay_sec

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        bodies, handles = await self.__sqs.receive()

        for stdl_msg in [StdlDoneMsg(**json.loads(body)) for body in bodies]:
            await self.__queue.push(stdl_msg)

        if len(handles) > 0:
            await self.__sqs.delete_batch(handles)
