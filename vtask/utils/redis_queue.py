from redis import Redis


class RedisQueue:
    def __init__(self, redis: Redis, key: str):
        self.__redis = redis
        self.__key = key

    def push(self, value: str):
        self.__redis.lpush(self.__key, value.encode("utf-8"))

    def pop(self) -> str | None:
        txt = self.__redis.rpop(self.__key)
        if txt is None:
            return None
        if not isinstance(txt, bytes):
            raise ValueError("Expected bytes data")
        return txt.decode("utf-8")

    def get(self):
        txt = self.__redis.lindex(self.__key, -1)
        if txt is None:
            return None
        if not isinstance(txt, bytes):
            raise ValueError("Expected bytes data")
        return txt.decode("utf-8")

    def list(self) -> list[str]:
        items = self.__redis.lrange(self.__key, 0, -1)
        if not isinstance(items, list):
            raise ValueError("Expected list data")
        return [item.decode("utf-8") for item in items]

    def empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        result = self.__redis.llen(self.__key)
        if not isinstance(result, int):
            raise ValueError("Expected int data")
        return result

    def clear_queue(self):
        self.__redis.delete(self.__key)
