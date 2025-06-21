import yaml
from pydantic import BaseModel, constr

from .fs_types import FsType
from ...external.s3 import S3Config


class FsConfig(BaseModel):
    name: constr(min_length=1)
    type: FsType
    s3: S3Config | None = None


class FsConfigYaml(BaseModel):
    configs: list[FsConfig]


def read_fs_config(config_path: str) -> list[FsConfig]:
    with open(config_path, "r") as file:
        text = file.read()
    return FsConfigYaml(**yaml.load(text, Loader=yaml.FullLoader)).configs
