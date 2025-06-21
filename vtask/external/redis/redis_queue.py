import uuid

from redis import Redis

from .redis_errors import RedisError


class RedisQueue:
    def __init__(self, redis: Redis, key: str):
        self.__redis = redis
        self.__key = key

    def push(self, value: str):
        self.__redis.lpush(self.__key, value.encode("utf-8"))

    def pop(self) -> str | None:
        value = self.__redis.rpop(self.__key)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data")
        return value.decode("utf-8")

    def get(self):
        value = self.__redis.lindex(self.__key, -1)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data")
        return value.decode("utf-8")

    def get_by_index(self, idx: int) -> str | None:
        value = self.__redis.lindex(self.__key, idx)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data")
        return value.decode("utf-8")

    def list_items(self) -> list[str]:
        items = self.__redis.lrange(self.__key, 0, -1)
        if not isinstance(items, list):
            raise RedisError("Expected list data")
        return [item.decode("utf-8") for item in items]

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
        result = self.__redis.llen(self.__key)
        if not isinstance(result, int):
            raise RedisError("Expected int data")
        return result

    def clear(self):
        result = self.__redis.delete(self.__key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data")
        if result != 1:
            raise RedisError("Failed to delete key")
