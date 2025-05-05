from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel

from .stdl_archiver import ArchiveTarget, StdlArchiver
from ....common.env import BatchEnv
from ....common.fs import S3Config
from ....common.notifier import create_notifier
from ....utils import S3Client


class ArchiveMode(Enum):
    ARCHIVE = "archive"
    TRANSCODE = "transcode"


class StdlArchiveConfig(BaseModel):
    mode: ArchiveMode
    out_dir_path: str
    tmp_dir_path: str
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
        self.conf = read_archive_config(conf_path)
        self.notifier = create_notifier(env=env.env, conf=env.untf)
        self.archiver = StdlArchiver(
            s3_client=S3Client(self.conf.s3_config),
            notifier=self.notifier,
            out_dir_path=self.conf.out_dir_path,
            tmp_dir_path=self.conf.tmp_dir_path,
        )
        self.targets = self.conf.targets

    def run(self):
        if self.conf.mode == ArchiveMode.ARCHIVE:
            self.archiver.archive(self.targets)
        elif self.conf.mode == ArchiveMode.TRANSCODE:
            self.archiver.transcode()
        else:
            raise ValueError(f"Unknown command: {self.conf.mode}")
