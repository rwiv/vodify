from redis.asyncio import Redis

from .redis_types import RedisConfig


def create_async_redis_client(conf: RedisConfig) -> Redis:
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=0,
        decode_responses=True,
    )
