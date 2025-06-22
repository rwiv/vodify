from fastapi import APIRouter

from .celery import CeleryController
from .stdl import StdlController, StdlTaskRegistrar, StdlTaskRegisterJob, StdlMsgConsumeJob
from ..celery import CeleryRedisBrokerClient
from ..common.job import CronJob
from ..env import get_server_env, get_celery_env
from ..external.sqs import SQSAsyncClient


class DefaultController:
    def __init__(self):
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])

    def health(self):
        return {"status": "UP"}


class ServerDependencyManager:
    def __init__(self):
        self.server_env = get_server_env()
        self.celery_env = get_celery_env()
        redis_conf = self.celery_env.redis

        # default
        default_controller = DefaultController()
        self.default_router = default_controller.router

        # celery
        celery_redis_broker = CeleryRedisBrokerClient(self.celery_env.redis)
        celery_controller = CeleryController(celery_redis_broker)
        self.celery_router = celery_controller.router

        # stdl
        stdl_requester = StdlTaskRegistrar()

        stdl_register_job = StdlTaskRegisterJob(redis_conf, stdl_requester, celery_redis_broker)
        self.stdl_register_cron = CronJob(job=stdl_register_job, interval_sec=5, unstoppable=True)

        stdl_consume_job = StdlMsgConsumeJob(redis_conf, SQSAsyncClient(self.server_env.sqs))
        self.stdl_consume_cron = CronJob(job=stdl_consume_job, interval_sec=1, unstoppable=True)

        stdl_controller = StdlController(redis_conf, self.stdl_register_cron, stdl_requester)
        self.stdl_router = stdl_controller.router
