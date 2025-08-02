import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))
# load_dotenv(path_join(find_project_root(), "dev", ".env-server-prod"))

from vidt.celery import IO_NET_QUEUE_NAME
from vidt.external.redis import create_redis_client
from vidt.celery import CeleryRedisBrokerClient
from vidt.env import get_celery_env

env = get_celery_env()
conf = env.redis

QUEUE_NAME = "celery"
redis_client = create_redis_client(conf)
client = CeleryRedisBrokerClient(env.redis)


@pytest.mark.asyncio
async def test_all_redis_keys():
    print()
    for key in await get_all_keys(redis_client, "*"):
        print(key)


@pytest.mark.asyncio
async def test_queue():
    print()
    print(await client.get_received_task_bodies(IO_NET_QUEUE_NAME))


async def get_all_keys(client: Redis, pattern: str) -> list[str]:
    keys = []
    async for key in client.scan_iter(pattern):
        keys.append(key)
    return keys
