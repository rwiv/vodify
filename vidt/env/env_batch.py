import os

from pydantic import BaseModel, constr

from .env_configs import read_untf_env
from ..external.notifier import UntfConfig


class BatchEnv(BaseModel):
    env: constr(min_length=1)
    loss_config_path: constr(min_length=1) | None = None
    archive_config_path: constr(min_length=1) | None = None
    untf: UntfConfig


def get_batch_env() -> BatchEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    return BatchEnv(
        env=env,
        loss_config_path=os.getenv("LOSS_CONFIG_PATH") or None,
        archive_config_path=os.getenv("ARCHIVE_CONFIG_PATH") or None,
        untf=read_untf_env(),
    )
