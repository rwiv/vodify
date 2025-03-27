from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel, constr, Field


class FrameLossCommand(Enum):
    KEY_FRAME = "keyFrame"
    ALL = "all"


class KeyFrameLossConfig(BaseModel):
    threshold_sec: float = Field(alias="thresholdSec", default=1.5)


class AllFrameLossConfig(BaseModel):
    threshold_byte: int = Field(alias="thresholdByte", default=50)
    list_capacity: int = Field(alias="listCapacity", default=60)
    weight_sec: float = Field(alias="weightSec", default=1)


class FrameLossConfig(BaseModel):
    command: FrameLossCommand
    src_dir_path: constr(min_length=1) = Field(alias="srcDirPath")
    out_dir_path: constr(min_length=1) = Field(alias="outDirPath")
    key_frame: KeyFrameLossConfig = Field(alias="keyFrame")
    all: AllFrameLossConfig


def read_fl_config(config_path: str) -> FrameLossConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return FrameLossConfig(**yaml.load(text, Loader=yaml.FullLoader))
