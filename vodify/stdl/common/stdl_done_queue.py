import json

from ..schema.stdl_types import StdlDoneMsg
from ...external.redis import RedisConfig, RedisQueue, create_app_redis_client

REDIS_STDL_DONE_LIST_KEY = "vodify:stdl:done"


class StdlMsgQueue:
    def __init__(self, conf: RedisConfig):
        self.__key = REDIS_STDL_DONE_LIST_KEY
        self.redis_queue = RedisQueue(redis=create_app_redis_client(conf), key=self.__key)

    async def push(self, value: StdlDoneMsg):
        await self.redis_queue.push(value.model_dump_json(by_alias=True))

    async def get(self) -> StdlDoneMsg | None:
        txt = await self.redis_queue.get()
        if txt is None:
            return None
        return StdlDoneMsg(**json.loads(txt))

    async def pop(self) -> StdlDoneMsg | None:
        txt = await self.redis_queue.pop()
        if txt is None:
            return None
        return StdlDoneMsg(**json.loads(txt))

    async def list_items(self) -> list[StdlDoneMsg]:
        messages = await self.redis_queue.list_items()
        return [StdlDoneMsg(**json.loads(msg)) for msg in messages]

    async def size(self) -> int:
        return await self.redis_queue.size()

    async def empty(self) -> bool:
        return await self.redis_queue.empty()

    async def clear_queue(self):
        await self.redis_queue.clear()
