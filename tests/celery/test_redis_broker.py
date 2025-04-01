import redis
from pyutils import load_dotenv, path_join, find_project_root
from redis import Redis

from vtask.celery.celery_constants import DEFAULT_QUEUE_NAME

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))

from vtask.celery import CeleryRedisBrokerClient
from vtask.common.env import get_celery_env

env = get_celery_env()
conf = env.redis

QUEUE_NAME = "celery"
redis_client = redis.Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
client = CeleryRedisBrokerClient(env.redis)


def test_all_redis_keys():
    print()
    for key in get_all_keys(redis_client, "*"):
        print(key)


def test_queue():
    print()
    print(client.get_received_task_bodies(DEFAULT_QUEUE_NAME))


def get_all_keys(client: Redis, pattern: str) -> list[str]:
    keys = []
    for key in client.scan_iter(pattern):
        if isinstance(key, bytes):
            keys.append(key.decode())
        elif isinstance(key, str):
            keys.append(key)
        else:
            raise ValueError(f"Unknown key type: {type(key)}")
    return keys
