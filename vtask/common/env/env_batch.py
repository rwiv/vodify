import os
from enum import Enum

from pydantic import BaseModel, constr


class BatchCommand(Enum):
    LOSS = "loss"
    VIDEO = "video"
    ARCHIVE = "archive"


class UntfConfig(BaseModel):
    endpoint: constr(min_length=1)
    api_key: constr(min_length=1)
    topic: constr(min_length=1)


class BatchEnv(BaseModel):
    env: constr(min_length=1)
    command: BatchCommand
    loss_config_path: constr(min_length=1) | None = None
    video_download_config_path: constr(min_length=1) | None = None
    archive_config_path: constr(min_length=1) | None = None
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
        video_download_config_path=os.getenv("VIDEO_DOWNLOAD_CONFIG_PATH") or None,
        loss_config_path=os.getenv("LOSS_CONFIG_PATH") or None,
        archive_config_path=os.getenv("ARCHIVE_CONFIG_PATH") or None,
        untf=untf,
    )
