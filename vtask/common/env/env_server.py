import os

from pydantic import BaseModel, conint, constr


class ServerConfig(BaseModel):
    port: conint(ge=1)


class ServerEnv(BaseModel):
    env: constr(min_length=1)
    server: ServerConfig


def get_server_env() -> ServerEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    server_config = ServerConfig(port=int(os.getenv("SERVER_PORT")))  # type: ignore

    return ServerEnv(
        env=env,
        server=server_config,
    )
