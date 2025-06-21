import os

from pydantic import BaseModel, conint, constr

from ..external.sqs import SQSConfig


class ServerConfig(BaseModel):
    port: conint(ge=1)


class ServerEnv(BaseModel):
    env: constr(min_length=1)
    server: ServerConfig
    sqs: SQSConfig


def get_server_env() -> ServerEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    server_config = ServerConfig(port=int(os.getenv("SERVER_PORT")))  # type: ignore
    sqs = SQSConfig(
        access_key=os.getenv("SQS_ACCESS_KEY"),  # type: ignore
        secret_key=os.getenv("SQS_SECRET_KEY"),  # type: ignore
        region_name=os.getenv("SQS_REGION_NAME"),  # type: ignore
        queue_url=os.getenv("SQS_QUEUE_URL"),  # type: ignore
    )

    return ServerEnv(
        env=env,
        server=server_config,
        sqs=sqs,
    )
