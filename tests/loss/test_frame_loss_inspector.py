import yaml
from pyutils import path_join, find_project_root

from vtask.service.loss import TimeLossInspector, SizeLossInspector


def test_analyze_by_key_frames():
    csv_path = path_join(find_project_root(), "dev", "test", "assets", "loss", "all_enc.csv")

    inspector = TimeLossInspector(keyframe_only=False)
    result = inspector.analyze(csv_path)

    yaml_txt = yaml.dump(result.to_out_dict())
    print(yaml_txt)


def test_inspect_by_time():
    vid_path = path_join(find_project_root(), "dev", "test", "assets", "loss", "source.mp4")
    csv_path = path_join(find_project_root(), "dev", "test", "out", "source.csv")
    yaml_path = path_join(find_project_root(), "dev", "test", "out", "source.yaml")

    keyframe_only = True
    # keyframe_only = False

    inspector = TimeLossInspector(keyframe_only=keyframe_only)
    result = inspector.inspect(vid_path, csv_path)

    print(result)

    with open(yaml_path, "w") as file:
        file.write(yaml.dump(result.to_out_dict(), allow_unicode=True))


def test_inspect_by_size():
    vid_path = path_join(find_project_root(), "dev", "test", "assets", "loss", "encoded.mp4")
    csv_path = path_join(find_project_root(), "dev", "test", "out", "encoded.csv")
    yaml_path = path_join(find_project_root(), "dev", "test", "out", "encoded.yaml")

    inspector = SizeLossInspector()
    result = inspector.inspect(vid_path, csv_path)

    with open(yaml_path, "w") as file:
        file.write(yaml.dump(result.to_out_dict(), allow_unicode=True))
