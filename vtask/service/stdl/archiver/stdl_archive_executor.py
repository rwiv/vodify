from pathlib import Path

import yaml
from pydantic import BaseModel

from .stdl_archiver import ArchiveTarget, StdlArchiver
from ....common.env import BatchEnv
from ....common.fs import S3Config
from ....utils import S3Client


class StdlArchiveConfig(BaseModel):
    out_dir_path: str
    s3_config: S3Config
    targets: list[ArchiveTarget]


def read_archive_config(config_path: str) -> StdlArchiveConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return StdlArchiveConfig(**yaml.load(text, Loader=yaml.FullLoader))


class StdlArchiveExecutor:
    def __init__(self, env: BatchEnv):
        conf_path = env.archive_config_path
        if conf_path is None:
            raise ValueError("archive_config_path is required")
        conf = read_archive_config(conf_path)
        s3_client = S3Client(conf.s3_config)
        self.archiver = StdlArchiver(s3_client, conf.out_dir_path)
        self.targets = conf.targets

    def run(self):
        self.archiver.archive(self.targets)
