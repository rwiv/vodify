from redis import Redis

from .redis_errors import RedisError


class RedisString:
    def __init__(self, client: Redis):
        self.__redis = client

    def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        xx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:  # return True if set
        ok = self.__redis.set(name=key, value=value, nx=nx, xx=xx, ex=ex, px=px)
        if ok is None:
            return False
        if not isinstance(ok, bool):
            raise RedisError("Expected boolean data")
        return ok

    def get(self, key: str) -> str | None:
        return self.__redis.get(key)  # type: ignore

    def mget(self, keys: list[str]) -> list[str | None]:
        results = self.__redis.mget(keys=keys)
        if not isinstance(results, list):
            raise RedisError("Expected list data")
        return results

    def delete(self, key: str) -> int:
        return self.__redis.delete(key)  # type: ignore

    def exists(self, key: str) -> bool:
        result = self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data")
        return result == 1
