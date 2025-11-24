from redis.asyncio import Redis

from .redis_types import RedisConfig


def create_app_redis_client(conf: RedisConfig) -> Redis:
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=0,
        decode_responses=True,
    )


def create_celery_redis_client(conf: RedisConfig) -> Redis:
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=1,
        decode_responses=True,
    )
