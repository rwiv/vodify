import uuid

from redis.asyncio import Redis


class RedisQueue:
    def __init__(self, redis: Redis, key: str):
        self.__redis = redis
        self.__key = key

    async def push(self, value: str) -> int:  # return size
        return await self.__redis.lpush(self.__key, value)  # type: ignore

    async def pop(self) -> str | None:
        return await self.__redis.rpop(self.__key)  # type: ignore

    async def get(self):
        return await self.__redis.lindex(self.__key, -1)  # type: ignore

    async def get_by_index(self, idx: int) -> str | None:
        return await self.__redis.lindex(self.__key, idx)  # type: ignore

    async def list_items(self) -> list[str]:
        return await self.__redis.lrange(self.__key, 0, -1)  # type: ignore

    # Using index may cause concurrency issues
    # Prefer using remove_by_value() if possible
    async def remove_by_idx(self, idx: int):
        to_be_deleted = str(uuid.uuid4())
        await self.__redis.lset(self.__key, idx, to_be_deleted)  # type: ignore
        await self.__redis.lrem(self.__key, 1, to_be_deleted)  # type: ignore

    async def remove_by_value(self, value: str):
        return await self.__redis.lrem(self.__key, 1, value)  # type: ignore

    async def empty(self) -> bool:
        return await self.size() == 0

    async def size(self) -> int:
        return await self.__redis.llen(self.__key)  # type: ignore

    async def clear(self) -> int:  # deleted count
        return await self.__redis.delete(self.__key)
