import pytest

from tests.testutils.test_utils_misc import load_test_dotenv
from vidt.env import get_celery_env
from vidt.external.redis import RedisQueue, create_redis_client

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

key = "vidt:test:list"
celery_env = get_celery_env()
conf = celery_env.redis
queue = RedisQueue(create_redis_client(conf), key=key)


@pytest.mark.asyncio
async def test_remove_by_idx():
    print()
    await queue.push("test1")
    await queue.push("test2")
    await queue.push("test3")

    for i, value in enumerate(await queue.list_items()):
        if value == "test2":
            await queue.remove_by_idx(i)

    for value in await queue.list_items():
        print(value)

    await queue.clear()


@pytest.mark.asyncio
async def test_remove_by_value():
    print()
    await queue.push("test1")
    await queue.push("test2")
    await queue.push("test3")

    for value in await queue.list_items():
        if value == "test2":
            await queue.remove_by_value(value)

    for value in await queue.list_items():
        print(value)

    await queue.clear()
