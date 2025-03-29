from .celery import CeleryController
from .stdl import StdlController, StdlListener
from ..celery import CeleryRedisBrokerClient
from ..common.amqp import AmqpHelperBlocking, AmqpHelperMock
from ..common.env import get_server_env, get_celery_env
from ..service.stdl.schema import STDL_DONE_QUEUE


class ServerDependencyManager:
    def __init__(self):
        self.env = get_server_env()
        self.celery_env = get_celery_env()
        self.amqp = self.create_amqp()

        redis_broker = CeleryRedisBrokerClient(self.celery_env.redis)
        celery_controller = CeleryController(redis_broker)
        self.celery_router = celery_controller.router

        stdl_listener = StdlListener(self.amqp, STDL_DONE_QUEUE)
        stdl_controller = StdlController(stdl_listener)
        self.stdl_router = stdl_controller.router

    def create_amqp(self):
        if self.env.env == "prod":
            return AmqpHelperBlocking(self.env.amqp)
        else:
            return AmqpHelperMock()


deps = ServerDependencyManager()
