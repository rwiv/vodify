from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel, constr, Field


class LossCommand(Enum):
    KEY = "key"
    ALL = "all"


class LossMethod(Enum):
    INSPECT = "inspect"
    ANALYZE = "analyze"


class KeyFrameLossConfig(BaseModel):
    threshold_sec: float = Field(alias="thresholdSec", default=1.1)


class AllFrameLossConfig(BaseModel):
    threshold_byte: int = Field(alias="thresholdByte", default=50)
    list_capacity: int = Field(alias="listCapacity", default=60)
    weight_sec: float = Field(alias="weightSec", default=1)


class LossConfig(BaseModel):
    command: LossCommand
    method: LossMethod
    archive_csv: bool = Field(alias="archiveCsv")
    src_dir_path: constr(min_length=1) = Field(alias="srcDirPath")
    out_dir_path: constr(min_length=1) = Field(alias="outDirPath")
    key: KeyFrameLossConfig
    all: AllFrameLossConfig


def read_loss_config(config_path: str) -> LossConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return LossConfig(**yaml.load(text, Loader=yaml.FullLoader))
