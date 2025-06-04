import asyncio
from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel

from .stdl_archiver import ArchiveTarget, StdlArchiver
from ....common.env import BatchEnv
from ....common.fs import S3Config
from ....common.notifier import create_notifier


class ArchiveMode(Enum):
    DOWNLOAD = "download"
    TRANSCODE_LOCAL = "transcode_local"
    TRANSCODE_S3 = "transcode_s3"


class StdlArchiveConfig(BaseModel):
    mode: ArchiveMode
    out_dir_path: str
    tmp_dir_path: str
    s3_config: S3Config
    archive: bool
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
            s3_conf=self.conf.s3_config,
            notifier=self.notifier,
            out_dir_path=self.conf.out_dir_path,
            tmp_dir_path=self.conf.tmp_dir_path,
            is_archive=self.conf.archive,
        )
        self.targets = self.conf.targets

    def run(self):
        if self.conf.mode == ArchiveMode.DOWNLOAD:
            asyncio.run(self.archiver.download(self.targets))
        elif self.conf.mode == ArchiveMode.TRANSCODE_LOCAL:
            asyncio.run(self.archiver.transcode_by_local())
        elif self.conf.mode == ArchiveMode.TRANSCODE_S3:
            asyncio.run(self.archiver.transcode_by_s3(self.targets))
        else:
            raise ValueError(f"Unknown command: {self.conf.mode}")
