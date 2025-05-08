import os
import shutil
import time
from pathlib import Path

from pyutils import path_join


def check_dir(base_path: str):
    dir_path = Path(base_path).resolve().parent
    if not dir_path.exists():
        os.makedirs(dir_path, exist_ok=True)


def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    return dir_path


def read_dir_recur(dir_path: str):
    paths = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            paths.append(os.path.join(root, file))
    return paths


def move_directory_not_recur(src_dir_path: str, dst_dir_path: str):
    move_start = time.time()
    os.makedirs(dst_dir_path, exist_ok=True)
    for file_path in os.listdir(src_dir_path):
        if Path(file_path).is_dir():
            raise ValueError("only supports files")
        shutil.move(src=path_join(src_dir_path, file_path), dst=path_join(dst_dir_path, file_path))
    return time.time() - move_start
