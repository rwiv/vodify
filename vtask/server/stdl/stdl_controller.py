from fastapi import APIRouter

from ...common.job import CronJob
from ...service.stdl.common import StdlDoneQueue


class StdlController:
    def __init__(self, queue: StdlDoneQueue, cron: CronJob):
        self.queue = queue
        self.cron = cron

        self.router = APIRouter(prefix="/api/stdl")
        self.router.add_api_route("/stats", self.get_stats, methods=["GET"])
        self.router.add_api_route("/listening/start", self.start_listening, methods=["POST"])
        self.router.add_api_route("/listening/stop", self.stop_listening, methods=["POST"])

    def get_stats(self):
        return {
            "consuming": self.cron.is_running(),
            "queue": self.queue.list(),
        }

    def start_listening(self):
        self.cron.start()

    def stop_listening(self):
        self.cron.stop()
