from redis import Redis

from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.env import get_celery_env
from vtask.external.redis import RedisQueue

load_test_dotenv(".env-worker-dev")
# load_test_dotenv(".env-worker-prod")

key = "vtask:test:list"
celery_env = get_celery_env()
conf = celery_env.redis
redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
queue = RedisQueue(redis, key=key)


def test_remove_by_idx():
    print()
    queue.push("test1")
    queue.push("test2")
    queue.push("test3")

    for i, value in enumerate(queue.list_items()):
        if value == "test2":
            queue.remove_by_idx(i)

    for value in queue.list_items():
        print(value)

    queue.clear()


def test_remove_by_value():
    print()
    queue.push("test1")
    queue.push("test2")
    queue.push("test3")

    for value in queue.list_items():
        if value == "test2":
            queue.remove_by_value(value)

    for value in queue.list_items():
        print(value)

    queue.clear()
