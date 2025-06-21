import uuid

from redis import Redis


class RedisQueue:
    def __init__(self, redis: Redis, key: str):
        self.__redis = redis
        self.__key = key

    def push(self, value: str):
        self.__redis.lpush(self.__key, value)

    def pop(self) -> str | None:
        return self.__redis.rpop(self.__key)  # type: ignore

    def get(self) -> str | None:
        return self.__redis.lindex(self.__key, -1)  # type: ignore

    def get_by_index(self, idx: int) -> str | None:
        return self.__redis.lindex(self.__key, idx)  # type: ignore

    def list_items(self) -> list[str]:
        return self.__redis.lrange(self.__key, 0, -1)  # type: ignore

    # Using index may cause concurrency issues
    # Prefer using remove_by_value() if possible
    def remove_by_idx(self, idx: int):
        to_be_deleted = str(uuid.uuid4())
        self.__redis.lset(self.__key, idx, to_be_deleted)
        self.__redis.lrem(self.__key, 1, to_be_deleted)

    def remove_by_value(self, value: str):
        self.__redis.lrem(self.__key, 1, value)

    def empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        return self.__redis.llen(self.__key)  # type: ignore

    def clear(self):
        return self.__redis.delete(self.__key)
