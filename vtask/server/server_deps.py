from .stdl import StdlController, StdlListener, STDL_DONE_QUEUE
from ..celery import CeleryController, RedisBrokerClient
from ..common.amqp import AmqpBlocking, AmqpMock
from ..common.env import get_server_env, get_celery_env


class ServerDependencyManager:
    def __init__(self):
        self.env = get_server_env()
        self.celery_env = get_celery_env()
        self.amqp = self.create_amqp()

        self.redis_broker = RedisBrokerClient(self.celery_env.redis)
        self.__celery_controller = CeleryController(self.redis_broker)
        self.celery_router = self.__celery_controller.router

        self.__stdl_controller = StdlController()
        self.stdl_router = self.__stdl_controller.router
        self.stdl_listener = StdlListener(self.amqp, STDL_DONE_QUEUE)

    def create_amqp(self):
        if self.env.env == "prod":
            return AmqpBlocking(self.env.amqp)
        else:
            return AmqpMock()


deps = ServerDependencyManager()
