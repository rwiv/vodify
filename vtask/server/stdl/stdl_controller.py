from fastapi import APIRouter

from .stdl_task_requester import StdlTaskRequester
from ...common.job import CronJob
from ...service.stdl.common import StdlDoneQueue
from ...service.stdl.schema import StdlDoneMsg, StdlDoneStatus


class StdlController:
    def __init__(self, queue: StdlDoneQueue, cron: CronJob, requester: StdlTaskRequester):
        self.__queue = queue
        self.__cron = cron
        self.__requester = requester

        self.router = APIRouter(prefix="/api/stdl")
        self.router.add_api_route("/stats", self.get_stats, methods=["GET"])
        self.router.add_api_route("/listening/start", self.start_listening, methods=["POST"])
        self.router.add_api_route("/listening/stop", self.stop_listening, methods=["POST"])
        self.router.add_api_route("/command/cancel-extract", self.extract_cancel_requests, methods=["POST"])
        self.router.add_api_route(
            "/command/cancel-convert", self.convert_to_cancel_by_video_name, methods=["POST"]
        )

    def get_stats(self):
        return {
            "listening": self.__cron.is_running(),
            "queue": self.__queue.list(),
            "queue_size": self.__queue.size(),
        }

    def start_listening(self):
        self.__cron.start()

    def stop_listening(self):
        self.__cron.stop()

    def extract_cancel_requests(self):
        for _ in range(self.__queue.size()):
            msg: StdlDoneMsg | None = self.__queue.pop()
            if msg is None:
                raise Exception("stdl_done_queue is empty")

            if msg.status == StdlDoneStatus.CANCELED:
                queue_name = self.__requester.resolve_queue(msg)
                self.__requester.request_done(msg, queue_name)
            else:
                self.__queue.push(msg)

    def convert_to_cancel_by_video_name(self, video_name: str):
        for _ in range(self.__queue.size()):
            msg: StdlDoneMsg | None = self.__queue.pop()
            if msg is None:
                raise Exception("stdl_done_queue is empty")

            if msg.video_name != video_name:
                new_msg = msg.model_copy()
                new_msg.status = StdlDoneStatus.CANCELED
                queue_name = self.__requester.resolve_queue(new_msg)
                self.__requester.request_done(new_msg, queue_name)
            else:
                self.__queue.push(msg)
