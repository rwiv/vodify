from .stdl_task_requester import StdlTaskRequester
from ...service.stdl.task import StdlDoneQueue
from ...service.stdl.schema import STDL_DONE_QUEUE
from ...celery import CeleryRedisBrokerClient
from ...common.amqp import AmqpHelper, AmqpRedisMigrator
from ...common.job import Job


STDL_DONE_JOB_NAME = "stdl_done_job"


class StdlDoneJob(Job):
    def __init__(
        self,
        queue: StdlDoneQueue,
        amqp: AmqpHelper,
        requester: StdlTaskRequester,
        celery_redis: CeleryRedisBrokerClient,
        received_task_threshold: int = 1,
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

    def run(self):
        self.__mig.push_all_amqp_messages()
        for _ in range(self.__queue.size()):
            stdl_msg = self.__queue.pop()
            if stdl_msg is None:
                raise Exception("stdl_done_queue is empty")

            queue_name = self.__requester.resolve_queue(stdl_msg)

            received_tasks = self.__celery_redis.get_received_tasks(queue_name)
            if len(received_tasks) >= self.received_task_threshold:
                self.__queue.push(stdl_msg)
            else:
                self.__requester.request_done(stdl_msg, queue_name)
