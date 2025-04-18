from pyutils import log

from .stdl_task_requester import StdlTaskRequester
from ...celery import CeleryRedisBrokerClient, find_active_worker_names, app
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
        workers = find_active_worker_names(app)
        if len(workers) == 0:
            return

        self.__mig.push_all_amqp_messages()
        msg: StdlDoneMsg | None = self.__queue.get()
        if msg is None:
            return

        queue_name = self.__requester.resolve_queue(msg)

        received_tasks = self.__celery_redis.get_received_tasks(queue_name)
        if len(received_tasks) < self.received_task_threshold:
            self.__requester.request_done(msg, queue_name)
            self.__queue.pop()
