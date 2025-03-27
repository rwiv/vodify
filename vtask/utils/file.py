import os
from pathlib import Path


def check_dir(base_path: str):
    dir_path = Path(base_path).resolve().parent
    if not dir_path.exists():
        os.makedirs(dir_path, exist_ok=True)


def read_dir_recur(dir_path: str):
    paths = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            paths.append(os.path.join(root, file))
    return paths
