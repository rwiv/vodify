import json

from fastapi import APIRouter

from ...celery import (
    app,
    CeleryRedisBrokerClient,
    shutdown_workers,
    find_active_workers,
    get_running_tasks,
    get_prefetched_tasks,
)


class CeleryController:
    def __init__(self, redis_broker: CeleryRedisBrokerClient):
        self.redis_broker = redis_broker
        self.router = APIRouter(prefix="/api/celery")
        self.router.add_api_route("/shutdown", self.shutdown, methods=["POST"])
        self.router.add_api_route("/workers", self.get_workers, methods=["GET"])
        self.router.add_api_route("/tasks/running", self.get_running_tasks, methods=["GET"])
        self.router.add_api_route("/tasks/prefetched", self.get_prefetched_tasks, methods=["GET"])
        self.router.add_api_route(
            "/tasks/queued/{queue_name}/bodies", self.get_queue_tasks_bodies, methods=["GET"]
        )
        self.router.add_api_route(
            "/tasks/queued/{queue_name}/args", self.get_queue_tasks_args, methods=["GET"]
        )

    def shutdown(self):
        shutdown_workers(app)
        return "ok"

    def get_workers(self):
        return find_active_workers(app)

    def get_running_tasks(self):
        return get_running_tasks(app)

    def get_prefetched_tasks(self):
        return get_prefetched_tasks(app)

    def get_queue_tasks_bodies(self, queue_name: str):
        bodies = self.redis_broker.get_received_task_bodies(queue_name=queue_name)
        return {
            "count": len(bodies),
            "bodies": bodies,
        }

    def get_queue_tasks_args(self, queue_name: str):
        bodies = self.redis_broker.get_received_task_bodies(queue_name=queue_name)
        return {
            "count": len(bodies),
            "args": [json.dumps(body["args"]) for body in bodies],
        }
