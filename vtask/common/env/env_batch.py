import os
from enum import Enum

from pydantic import BaseModel, constr

from .env_configs import UntfConfig, read_untf_env


class BatchCommand(Enum):
    LOSS = "loss"
    VIDEO = "video"
    ARCHIVE = "archive"
    ENCODING = "encoding"


class BatchEnv(BaseModel):
    env: constr(min_length=1)
    command: BatchCommand
    loss_config_path: constr(min_length=1) | None = None
    video_download_config_path: constr(min_length=1) | None = None
    archive_config_path: constr(min_length=1) | None = None
    encoding_config_path: constr(min_length=1) | None = None
    untf: UntfConfig


def get_batch_env() -> BatchEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    return BatchEnv(
        env=env,
        command=BatchCommand(os.getenv("BATCH_COMMAND")),
        video_download_config_path=os.getenv("VIDEO_DOWNLOAD_CONFIG_PATH") or None,
        loss_config_path=os.getenv("LOSS_CONFIG_PATH") or None,
        archive_config_path=os.getenv("ARCHIVE_CONFIG_PATH") or None,
        untf=read_untf_env(),
    )
