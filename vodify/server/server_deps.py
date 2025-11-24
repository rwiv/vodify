from fastapi import APIRouter

from .celery import CeleryController
from .recnode import RecnodeController, RecnodeTaskRegistrar, RecnodeTaskRegisterJob, RecnodeMsgConsumeJob
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

        # recnode
        recnode_registrar = RecnodeTaskRegistrar()

        recnode_register_job = RecnodeTaskRegisterJob(redis_conf, recnode_registrar, celery_redis_broker)
        self.recnode_register_cron = CronJob(job=recnode_register_job, interval_sec=5, unstoppable=True)

        recnode_consume_job = RecnodeMsgConsumeJob(redis_conf, SQSAsyncClient(self.server_env.sqs), recnode_registrar)
        self.recnode_consume_cron = CronJob(job=recnode_consume_job, interval_sec=1, unstoppable=True)

        recnode_controller = RecnodeController(redis_conf, self.recnode_register_cron, recnode_registrar)
        self.recnode_router = recnode_controller.router
