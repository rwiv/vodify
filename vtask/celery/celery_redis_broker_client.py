import base64
import json
from typing import Any

from pydantic import BaseModel
from redis import Redis

from ..common.env import RedisConfig


class CeleryTaskHeaders(BaseModel):
    lang: str
    task: str
    id: str
    retries: int
    root_id: str
    parent_id: str | None
    ignore_result: bool


class CeleryTaskProperties(BaseModel):
    correlation_id: str
    reply_to: str
    delivery_mode: int
    delivery_info: dict
    priority: int
    body_encoding: str
    delivery_tag: str


class CeleryTaskInfo(BaseModel):
    body: str
    headers: CeleryTaskHeaders
    properties: CeleryTaskProperties

    def get_parsed_body(self) -> dict[str, Any]:
        decoded = json.loads(self.__decode_body())
        if not isinstance(decoded, list) or len(decoded) < 2:
            raise ValueError("Expected list data")
        return {
            "args": decoded[0],
            "kwargs": decoded[1],
        }

    def __decode_body(self) -> str:
        if self.properties.body_encoding != "base64":
            raise ValueError("Expected base64 encoding")
        return base64.b64decode(self.body).decode("utf-8")


class CeleryRedisBrokerClient:
    def __init__(self, conf: RedisConfig):
        self.__redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)

    def get_received_task_bodies(self, queue_name: str):
        tasks = self.__redis.lrange(queue_name, 0, -1)
        bodies = []
        for task in tasks:  # type: ignore
            info = CeleryTaskInfo(**json.loads(task.decode()))
            bodies.append(info.get_parsed_body())
        return {
            "count": len(bodies),
            "bodies": bodies,
        }
