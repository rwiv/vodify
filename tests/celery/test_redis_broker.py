import redis
from pyutils import load_dotenv, path_join, find_project_root
from redis import Redis

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))

from vtask.celery import RedisBrokerClient, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from vtask.common.env import get_celery_env

env = get_celery_env()
conf = env.redis

QUEUE_NAME = "celery"
redis_client = redis.Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
client = RedisBrokerClient(env.redis)


def test_all_redis_keys():
    print()
    for key in get_all_keys(redis_client, "*"):
        print(key)


def test_queue():
    print()
    print("local")
    for task in client.get_received_task_bodies(LOCAL_QUEUE_NAME):
        print(task.get_parsed_body())
    print("remote")
    for task in client.get_received_task_bodies(REMOTE_QUEUE_NAME):
        print(task.get_parsed_body())


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
