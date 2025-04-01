import yaml
from pydantic import BaseModel, Field
from pyutils import path_join, find_project_root

from vtask.service.stdl.schema import StdlDoneMsg


class StdlTestConfig(BaseModel):
    out_dir_path: str
    done_messages: list[StdlDoneMsg] = Field(alias="doneMessages")


class TestConfig(BaseModel):
    chunks_path: str
    local_base_dir_path: str
    tmp_dir_path: str
    stdl: StdlTestConfig


def read_test_conf():
    with open(path_join(find_project_root(), "dev", "test_conf.yaml"), "r") as file:
        test_conf = yaml.load(file.read(), Loader=yaml.FullLoader)
    return TestConfig(**test_conf)
