from redis import Redis

from .redis_errors import RedisError


class RedisMap:
    def __init__(self, client: Redis):
        self.__redis = client

    def set(self, key: str, value: str, nx: bool = False, xx: bool = False, ex: int | None = None):
        result = self.__redis.set(name=key, value=value, nx=nx, xx=xx, ex=ex)
        if result is None:
            return
        if not isinstance(result, bool):
            print(result)
            raise RedisError("Expected boolean data")
        if not result:
            raise RedisError("Failed to set value")

    def get(self, key: str) -> str | None:
        result = self.__redis.get(key)
        if result is None:
            return None
        if not isinstance(result, bytes):
            raise RedisError("Expected bytes data")
        return result.decode("utf-8")

    def delete(self, key: str):
        result = self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data")
        if result != 1:
            raise RedisError("Failed to delete key")

    def exists(self, key: str) -> bool:
        result = self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data")
        return result == 1
