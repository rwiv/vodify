from fastapi import APIRouter

from .celery import CeleryController
from .stdl import StdlController, StdlDoneJob, StdlTaskRequester
from ..celery import CeleryRedisBrokerClient
from ..common.env import get_server_env, get_celery_env
from ..common.job import CronJob
from ..service.stdl.common import StdlDoneQueue


class DefaultController:
    def __init__(self):
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])

    def health(self):
        return {"status": "UP"}


class ServerDependencyManager:
    def __init__(self):
        self.env = get_server_env()
        self.celery_env = get_celery_env()

        default_controller = DefaultController()
        self.default_router = default_controller.router

        celery_redis_broker = CeleryRedisBrokerClient(self.celery_env.redis)
        celery_controller = CeleryController(celery_redis_broker)
        self.celery_router = celery_controller.router

        stdl_requester = StdlTaskRequester()
        stdl_queue = StdlDoneQueue(self.celery_env.redis)
        stdl_job = StdlDoneJob(stdl_queue, stdl_requester, celery_redis_broker)
        self.stdl_cron = CronJob(job=stdl_job, interval_sec=5, unstoppable=True)
        stdl_controller = StdlController(stdl_queue, self.stdl_cron, stdl_requester)
        self.stdl_router = stdl_controller.router
