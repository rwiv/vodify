from celery import Celery

from ..env import get_celery_env

env = get_celery_env()

redis_url = f"redis://:{env.redis.password}@{env.redis.host}:{env.redis.port}/1"

app = Celery(
    "vidt",
    broker=redis_url,
    backend=redis_url,
    include=["vidt.celery.celery_tasks"],
)

app.conf.update(
    result_expires=86400,  # 1 day
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Seoul",
    worker_concurrency=env.worker_concurrency,
    worker_prefetch_multiplier=1,
    tasks_acks_late=False,
    broker_transport_options={"visibility_timeout": env.visibility_timeout_sec},
)
