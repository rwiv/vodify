import yaml
from pyutils import path_join, find_project_root

from vtask.service.loss import (
    TimeLossInspector,
    SizeLossInspector,
    SizeFrameLossConfig,
    TimeFrameLossConfig,
)


def test_inspect_time():
    vid_path = path_join(find_project_root(), "dev", "assets", "source.mp4")
    csv_path = path_join(find_project_root(), "dev", "out", "source.csv")
    yaml_path = path_join(find_project_root(), "dev", "out", "source.yaml")

    inspector = TimeLossInspector(TimeFrameLossConfig())
    result = inspector.inspect(vid_path, csv_path)

    with open(yaml_path, "w") as file:
        file.write(yaml.dump(result.model_dump(by_alias=True), allow_unicode=True))


def test_inspect_size():
    vid_path = path_join(find_project_root(), "dev", "assets", "encoded.mp4")
    csv_path = path_join(find_project_root(), "dev", "out", "encoded.csv")
    yaml_path = path_join(find_project_root(), "dev", "out", "encoded.yaml")

    inspector = SizeLossInspector(SizeFrameLossConfig())
    result = inspector.inspect(vid_path, csv_path)

    with open(yaml_path, "w") as file:
        file.write(yaml.dump(result.model_dump(by_alias=True), allow_unicode=True))
