import os

from pydantic import BaseModel, conint, constr


class AmqpConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    username: constr(min_length=1)
    password: constr(min_length=1)


def read_amqp_config():
    return AmqpConfig(
        host=os.getenv("AMQP_HOST"),
        port=int(os.getenv("AMQP_PORT")),  # type: ignore
        username=os.getenv("AMQP_USERNAME"),
        password=os.getenv("AMQP_PASSWORD"),
    )


class RedisConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    password: constr(min_length=1)


def read_redis_config():
    return RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),  # type: ignore
        password=os.getenv("REDIS_PASSWORD"),
    )
