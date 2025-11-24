from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel

from .recnode_archiver import ArchiveTarget, RecnodeArchiver
from ...env import BatchEnv
from ...external.notifier import create_notifier
from ...external.s3 import S3AsyncClient, S3Config


class ArchiveMode(Enum):
    DOWNLOAD = "download"
    TRANSCODE_LOCAL = "transcode_local"
    TRANSCODE_S3 = "transcode_s3"


class ArchiveConfig(BaseModel):
    mode: ArchiveMode
    out_dir_path: str
    tmp_dir_path: str
    s3_config: S3Config
    archive: bool
    min_read_timeout_sec: float
    read_timeout_threshold: float
    network_mbit: float
    network_buf_size: int
    network_retry_limit: int
    video_size_limit_gb: int
    targets: list[ArchiveTarget]


def read_archive_config(config_path: str) -> ArchiveConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return ArchiveConfig(**yaml.load(text, Loader=yaml.FullLoader))


class RecnodeArchiveExecutor:
    def __init__(self, env: BatchEnv):
        conf_path = env.archive_config_path
        if conf_path is None:
            raise ValueError("archive_config_path is required")
        self.conf = read_archive_config(conf_path)
        self.notifier = create_notifier(env=env.env, conf=env.untf)
        self.archiver = RecnodeArchiver(
            s3_client=S3AsyncClient(
                conf=self.conf.s3_config,
                network_mbit=self.conf.network_mbit,
                network_buf_size=self.conf.network_buf_size,
                retry_limit=self.conf.network_retry_limit,
                min_read_timeout_sec=self.conf.min_read_timeout_sec,
                read_timeout_threshold=self.conf.read_timeout_threshold,
            ),
            tmp_dir_path=self.conf.tmp_dir_path,
            out_dir_path=self.conf.out_dir_path,
            is_archive=self.conf.archive,
            video_size_limit_gb=self.conf.video_size_limit_gb,
            notifier=self.notifier,
        )
        self.targets = self.conf.targets

    async def run(self):
        if self.conf.mode == ArchiveMode.DOWNLOAD:
            await self.archiver.download(self.targets)
        elif self.conf.mode == ArchiveMode.TRANSCODE_LOCAL:
            await self.archiver.transcode_by_local()
        elif self.conf.mode == ArchiveMode.TRANSCODE_S3:
            await self.archiver.transcode_by_s3(self.targets)
        else:
            raise ValueError(f"Unknown command: {self.conf.mode}")
