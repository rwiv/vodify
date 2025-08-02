import os

from pydantic import BaseModel, conint, constr

from ..external.notifier import UntfConfig
from ..external.redis import RedisConfig


class AmqpConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    username: constr(min_length=1)
    password: constr(min_length=1)


def read_amqp_config():
    return AmqpConfig(
        host=os.getenv("AMQP_HOST"),
        port=os.getenv("AMQP_PORT"),  # type: ignore
        username=os.getenv("AMQP_USERNAME"),
        password=os.getenv("AMQP_PASSWORD"),
    )


def read_redis_config():
    return RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),  # type: ignore
        password=os.getenv("REDIS_PASSWORD"),
    )


def read_untf_env():
    return UntfConfig(
        endpoint=os.getenv("UNTF_ENDPOINT"),
        api_key=os.getenv("UNTF_API_KEY"),
        topic=os.getenv("UNTF_TOPIC"),
    )
