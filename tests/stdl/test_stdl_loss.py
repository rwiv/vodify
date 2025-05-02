import os

import yaml
from pydantic import BaseModel, Field
from pyutils import path_join, find_project_root

from vtask.service.stdl.loss import StdlLossInspector

base_path = path_join(find_project_root(), "dev", "loss_stdl")


class YamlFile(BaseModel):
    missing_segments: list[int] = Field(alias="missingSegments")


def test_loss_check():
    inspector = StdlLossInspector()

    for file_name in os.listdir(path_join(base_path, "src")):
        with open(path_join(base_path, "src", file_name), "r") as file:
            text = yaml.load(file.read(), Loader=yaml.FullLoader)
            result = inspector.inspect(YamlFile(**text).missing_segments)
            os.makedirs(path_join(base_path, "out"), exist_ok=True)
            with open(path_join(base_path, "out", file_name), "w") as dst_file:
                dst_file.write(yaml.dump(result.to_out_dict(), allow_unicode=True))
