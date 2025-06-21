import json

from fastapi import APIRouter

from .stdl_task_registrar import StdlTaskRegistrar
from ...common.job import CronJob
from ...stdl import StdlDoneMsg, StdlDoneStatus, StdlDoneQueue


class StdlController:
    def __init__(self, queue: StdlDoneQueue, cron: CronJob, registrar: StdlTaskRegistrar):
        self.__queue = queue
        self.__cron = cron
        self.__registrar = registrar

        self.router = APIRouter(prefix="/api/stdl")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/stats", self.get_stats, methods=["GET"])
        self.router.add_api_route("/tasks", self.push_task, methods=["POST"])
        self.router.add_api_route("/listening/start", self.start_listening, methods=["POST"])
        self.router.add_api_route("/listening/stop", self.stop_listening, methods=["POST"])
        self.router.add_api_route("/command/cancel-extract", self.extract_cancel_requests, methods=["POST"])
        self.router.add_api_route(
            "/command/cancel-convert/{video_name}", self.convert_to_cancel_by_video_name, methods=["POST"]
        )

    def health(self):
        return {"status": "UP"}

    def get_stats(self):
        items = self.__queue.list_items()
        items.reverse()
        return {
            "listening": self.__cron.is_running(),
            "queue_size": self.__queue.size(),
            "queue_items": items,
        }

    def start_listening(self):
        self.__cron.start()

    def stop_listening(self):
        self.__cron.stop()

    def push_task(self, msg: StdlDoneMsg):
        if msg.status == StdlDoneStatus.COMPLETE:
            self.__queue.push(msg)
        elif msg.status == StdlDoneStatus.CANCELED:
            queue_name = self.__registrar.resolve_queue(msg)
            self.__registrar.register(msg, queue_name)
        return "ok"

    def extract_cancel_requests(self):
        for value in self.__queue.redis_queue.list_items():
            msg = StdlDoneMsg(**json.loads(value))
            if msg.status == StdlDoneStatus.CANCELED:
                queue_name = self.__registrar.resolve_queue(msg)
                self.__queue.redis_queue.remove_by_value(value)
                self.__registrar.register(msg, queue_name)
        return "ok"

    def convert_to_cancel_by_video_name(self, video_name: str):
        for value in self.__queue.redis_queue.list_items():
            msg = StdlDoneMsg(**json.loads(value))
            if msg.video_name == video_name:
                new_msg = msg.model_copy()
                new_msg.status = StdlDoneStatus.CANCELED
                queue_name = self.__registrar.resolve_queue(new_msg)
                self.__queue.redis_queue.remove_by_value(value)
                self.__registrar.register(new_msg, queue_name)
                return new_msg
