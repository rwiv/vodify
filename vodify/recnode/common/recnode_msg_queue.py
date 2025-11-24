import json

from ..schema.recnode_types import RecnodeMsg
from ...external.redis import RedisConfig, RedisQueue, create_app_redis_client

REDIS_RECNODE_MSG_LIST_KEY = "vodify:recnode:msg"


class RecnodeMsgQueue:
    def __init__(self, conf: RedisConfig):
        self.__key = REDIS_RECNODE_MSG_LIST_KEY
        self.redis_queue = RedisQueue(redis=create_app_redis_client(conf), key=self.__key)

    async def push(self, value: RecnodeMsg):
        await self.redis_queue.push(value.model_dump_json(by_alias=True))

    async def get(self) -> RecnodeMsg | None:
        txt = await self.redis_queue.get()
        if txt is None:
            return None
        return RecnodeMsg(**json.loads(txt))

    async def pop(self) -> RecnodeMsg | None:
        txt = await self.redis_queue.pop()
        if txt is None:
            return None
        return RecnodeMsg(**json.loads(txt))

    async def list_items(self) -> list[RecnodeMsg]:
        messages = await self.redis_queue.list_items()
        return [RecnodeMsg(**json.loads(msg)) for msg in messages]

    async def size(self) -> int:
        return await self.redis_queue.size()

    async def empty(self) -> bool:
        return await self.redis_queue.empty()

    async def clear_queue(self):
        await self.redis_queue.clear()
