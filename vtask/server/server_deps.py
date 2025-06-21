from fastapi import APIRouter

from .celery import CeleryController
from .stdl import StdlController, StdlTaskRegistrar, StdlTaskRegisterJob, StdlMsgConsumeJob
from ..celery import CeleryRedisBrokerClient
from ..common.job import CronJob
from ..env import get_server_env, get_celery_env
from ..external.sqs import SQSAsyncClient
from ..stdl import StdlMsgQueue


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

        default_controller = DefaultController()
        self.default_router = default_controller.router

        celery_redis_broker = CeleryRedisBrokerClient(self.celery_env.redis)
        celery_controller = CeleryController(celery_redis_broker)
        self.celery_router = celery_controller.router

        stdl_requester = StdlTaskRegistrar()
        stdl_queue = StdlMsgQueue(self.celery_env.redis)

        stdl_register_job = StdlTaskRegisterJob(stdl_queue, stdl_requester, celery_redis_broker)
        self.stdl_register_cron = CronJob(job=stdl_register_job, interval_sec=5, unstoppable=True)

        sqs_client = SQSAsyncClient(self.server_env.sqs)
        stdl_consume_job = StdlMsgConsumeJob(sqs_client, stdl_queue)
        self.stdl_consume_cron = CronJob(job=stdl_consume_job, interval_sec=1, unstoppable=True)

        stdl_controller = StdlController(stdl_queue, self.stdl_register_cron, stdl_requester)
        self.stdl_router = stdl_controller.router
