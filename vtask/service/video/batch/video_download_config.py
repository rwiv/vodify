from pathlib import Path

import yaml
from pydantic import BaseModel, constr, Field

from ..schema.video_schema import VideoDownloadContext


class VideoDownloadConfig(BaseModel):
    urls: list[str]
    is_m3u8_url: bool = Field(alias="isM3u8Url")
    out_dir_path: constr(min_length=1) = Field(alias="outDirPath")
    context: VideoDownloadContext


def read_video_download_config(config_path: str) -> VideoDownloadConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return VideoDownloadConfig(**yaml.load(text, Loader=yaml.FullLoader))
