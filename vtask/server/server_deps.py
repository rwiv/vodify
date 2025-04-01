from .celery import CeleryController
from .stdl import StdlController, StdlDoneJob, StdlTaskRequester
from ..celery import CeleryRedisBrokerClient
from ..common.amqp import AmqpHelperBlocking, AmqpHelperMock
from ..common.env import get_server_env, get_celery_env
from ..common.job import CronJob
from ..service.stdl.common import StdlDoneQueue


class ServerDependencyManager:
    def __init__(self):
        self.env = get_server_env()
        self.celery_env = get_celery_env()
        self.amqp = self.create_amqp()

        celery_redis_broker = CeleryRedisBrokerClient(self.celery_env.redis)
        celery_controller = CeleryController(celery_redis_broker)
        self.celery_router = celery_controller.router

        stdl_requester = StdlTaskRequester()
        stdl_queue = StdlDoneQueue(self.celery_env.redis)
        stdl_job = StdlDoneJob(stdl_queue, self.amqp, stdl_requester, celery_redis_broker)
        stdl_cron = CronJob(stdl_job, interval_sec=5)  # TODO: update
        stdl_controller = StdlController(stdl_queue, stdl_cron)
        self.stdl_router = stdl_controller.router

    def create_amqp(self):
        if self.env.env == "prod":
            return AmqpHelperBlocking(self.env.amqp)
        else:
            return AmqpHelperMock()


deps = ServerDependencyManager()
