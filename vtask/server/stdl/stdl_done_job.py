import time

from .stdl_task_requester import StdlTaskRequester
from ...celery import CeleryRedisBrokerClient
from ...common.amqp import AmqpHelper, AmqpRedisMigrator
from ...common.job import Job
from ...service.stdl.common import StdlDoneQueue
from ...service.stdl.schema import STDL_DONE_QUEUE, StdlDoneMsg

STDL_DONE_JOB_NAME = "stdl_done_job"


class StdlDoneJob(Job):
    def __init__(
        self,
        queue: StdlDoneQueue,
        amqp: AmqpHelper,
        requester: StdlTaskRequester,
        celery_redis: CeleryRedisBrokerClient,
        received_task_threshold: int = 1,
        request_delay_sec: float = 3,
    ):
        super().__init__(name=STDL_DONE_JOB_NAME)

        self.__queue = queue
        self.__amqp = amqp
        self.__amqp_queue_name = STDL_DONE_QUEUE
        self.__mig = AmqpRedisMigrator(
            amqp=self.__amqp,
            queue=self.__queue.redis_queue,
            amqp_queue_name=self.__amqp_queue_name,
        )
        self.__requester = requester
        self.__celery_redis = celery_redis

        self.received_task_threshold = received_task_threshold
        self.request_delay_sec = request_delay_sec

    def run(self):
        self.__mig.push_all_amqp_messages()
        for _ in range(self.__queue.size()):
            msg: StdlDoneMsg | None = self.__queue.pop()
            if msg is None:
                raise Exception("stdl_done_queue is empty")

            queue_name = self.__requester.resolve_queue(msg)

            received_tasks = self.__celery_redis.get_received_tasks(queue_name)
            if len(received_tasks) >= self.received_task_threshold:
                self.__queue.push(msg)
            else:
                self.__requester.request_done(msg, queue_name)
                # Wait until the requested Celery task is scheduled
                #   If you do not wait, tasks that have not yet been scheduled will remain in the Celery queue.
                #   As a result, the request for that task may be skipped, causing its execution to be pushed to the end of the queue.
                time.sleep(self.request_delay_sec)
