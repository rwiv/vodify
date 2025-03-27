import os
from enum import Enum

from pydantic import BaseModel, constr


class BatchCommand(Enum):
    FRAME_LOSS = "frame_loss"


class UntfConfig(BaseModel):
    endpoint: constr(min_length=1)
    api_key: constr(min_length=1)
    topic: constr(min_length=1)


class BatchEnv(BaseModel):
    env: constr(min_length=1)
    command: BatchCommand
    fl_config_path: constr(min_length=1)
    untf: UntfConfig


def get_batch_env() -> BatchEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    untf = UntfConfig(
        endpoint=os.getenv("UNTF_ENDPOINT"),
        api_key=os.getenv("UNTF_API_KEY"),
        topic=os.getenv("UNTF_TOPIC"),
    )

    return BatchEnv(
        env=env,
        command=BatchCommand(os.getenv("BATCH_COMMAND")),
        fl_config_path=os.getenv("FL_CONFIG_PATH"),
        untf=untf,
    )
