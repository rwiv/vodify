import json

from redis import Redis

from ..schema import StdlDoneMsg
from ....common.env import RedisConfig
from ....utils import RedisQueue


REDIS_STDL_DONE_LIST_KEY = "vtask:stdl:done"


class StdlDoneQueue:
    def __init__(self, conf: RedisConfig):
        self.__key = REDIS_STDL_DONE_LIST_KEY
        redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
        self.redis_queue = RedisQueue(redis=redis, key=self.__key)

    def push(self, value: StdlDoneMsg):
        self.redis_queue.push(value.model_dump_json(by_alias=True))

    def get(self) -> StdlDoneMsg | None:
        txt = self.redis_queue.get()
        if txt is None:
            return None
        return StdlDoneMsg(**json.loads(txt))

    def pop(self) -> StdlDoneMsg | None:
        txt = self.redis_queue.pop()
        if txt is None:
            return None
        return StdlDoneMsg(**json.loads(txt))

    def list_items(self) -> list[StdlDoneMsg]:
        messages = self.redis_queue.list_items()
        return [StdlDoneMsg(**json.loads(msg)) for msg in messages]

    def size(self) -> int:
        return self.redis_queue.size()

    def empty(self) -> bool:
        return self.redis_queue.empty()

    def clear_queue(self):
        self.redis_queue.clear_queue()
