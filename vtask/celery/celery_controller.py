from fastapi import APIRouter

from .celery_redis_broker_client import RedisBrokerClient
from .celery_app import app
from .celery_utils import (
    find_active_workers,
    shutdown_workers,
    get_running_tasks,
)
from .celery_constants import LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME


class CeleryController:
    def __init__(self, redis_broker: RedisBrokerClient):
        self.redis_broker = redis_broker
        self.router = APIRouter(prefix="/api/celery")
        self.router.add_api_route("/shutdown", self.shutdown, methods=["POST"])
        self.router.add_api_route("/workers", self.get_workers, methods=["GET"])
        self.router.add_api_route("/tasks/running", self.get_running_tasks, methods=["GET"])
        self.router.add_api_route("/tasks/received", self.get_received_tasks, methods=["GET"])

    def shutdown(self):
        shutdown_workers(app)
        return "ok"

    def get_workers(self):
        return find_active_workers(app)

    def get_running_tasks(self):
        return get_running_tasks(app)

    def get_received_tasks(self):
        default = "celery"
        local = LOCAL_QUEUE_NAME
        remote = REMOTE_QUEUE_NAME
        return {
            default: self.redis_broker.get_received_task_bodies(default),
            local: self.redis_broker.get_received_task_bodies(local),
            remote: self.redis_broker.get_received_task_bodies(remote),
        }
