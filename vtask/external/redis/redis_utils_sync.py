from redis import Redis

from .redis_types import RedisConfig


def create_sync_redis_client(conf: RedisConfig):
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=0,
        decode_responses=True,
    )
