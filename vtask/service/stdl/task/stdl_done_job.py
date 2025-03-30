from .stdl_message_manager import StdlMessageManager
from .stdl_task_requester import StdlTaskRequester
from ....celery import CeleryRedisBrokerClient
from ....common.job import Job


class StdlDoneJob(Job):
    def __init__(
        self,
        msg_manager: StdlMessageManager,
        requester: StdlTaskRequester,
        celery_redis: CeleryRedisBrokerClient,
        received_task_threshold: int = 1,
    ):
        super().__init__("stdl_done_job")

        self.__msg_manager = msg_manager
        self.__requester = requester
        self.__celery_redis = celery_redis

        self.received_task_threshold = received_task_threshold

    def run(self):
        self.__msg_manager.push_all()
        while True:
            stdl_msg = self.__msg_manager.pop()
            if stdl_msg is None:
                break

            queue_name = self.__requester.resolve_queue(stdl_msg)

            received_tasks = self.__celery_redis.get_received_tasks(queue_name)
            if len(received_tasks) >= self.received_task_threshold:
                self.__msg_manager.publish(stdl_msg)
            else:
                self.__requester.request_done(stdl_msg, queue_name)
