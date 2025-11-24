import os

from pydantic import BaseModel, constr, conint

from .env_configs import read_redis_config
from ..external.redis import RedisConfig

DEFAULT_CELERY_WORKER_CONCURRENCY = 1
DEFAULT_CELERY_VISIBILITY_TIMEOUT = 7200  # 2 hours


class CeleryEnv(BaseModel):
    env: constr(min_length=1)
    worker_concurrency: conint(ge=1)
    visibility_timeout_sec: conint(ge=1)
    redis: RedisConfig


def get_celery_env() -> CeleryEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    worker_concurrency = os.getenv("CELERY_WORKER_CONCURRENCY") or None
    if worker_concurrency is None:
        worker_concurrency = DEFAULT_CELERY_WORKER_CONCURRENCY

    visibility_timeout_sec = os.getenv("CELERY_VISIBILITY_TIMEOUT_SEC") or None
    if visibility_timeout_sec is None:
        visibility_timeout_sec = DEFAULT_CELERY_VISIBILITY_TIMEOUT

    return CeleryEnv(
        env=env,
        worker_concurrency=worker_concurrency,
        visibility_timeout_sec=visibility_timeout_sec,
        redis=read_redis_config(),
    )
