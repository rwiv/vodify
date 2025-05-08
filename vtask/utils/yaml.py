from typing import Any

import yaml


def write_yaml_file(data: Any, out_file_path: str):
    with open(out_file_path, "w") as file:
        file.write(yaml.dump(data, allow_unicode=True))
