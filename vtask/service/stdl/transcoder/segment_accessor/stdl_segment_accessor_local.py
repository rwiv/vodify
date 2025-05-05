import os
import shutil
from pathlib import Path

from pyutils import path_join

from .stdl_segment_accessor import StdlSegmentAccessor
from ...schema import StdlSegmentsInfo
from .....common.fs import FsType


class StdlLocalSegmentAccessor(StdlSegmentAccessor):
    def __init__(self, local_incomplete_dir_path: str):
        super().__init__(FsType.LOCAL)
        self.src_incomplete_dir_path = local_incomplete_dir_path

    def get_paths(self, info: StdlSegmentsInfo) -> list[str]:
        dir_path = path_join(
            self.src_incomplete_dir_path, info.platform_name, info.channel_id, info.video_name
        )
        return [path_join(dir_path, file_name) for file_name in os.listdir(dir_path)]

    def get_size_sum(self, info: StdlSegmentsInfo) -> int:
        dir_path = path_join(
            self.src_incomplete_dir_path, info.platform_name, info.channel_id, info.video_name
        )
        if not os.path.exists(dir_path):
            return 0
        size_sum = 0
        for file_name in os.listdir(dir_path):
            file_path = path_join(dir_path, file_name)
            if not os.path.isfile(file_path):
                raise ValueError(f"Source path {file_path} is not a file.")
            size_sum += os.path.getsize(file_path)
        return size_sum

    def copy(self, paths: list[str], dest_dir_path: str):
        for src_file_path in paths:
            if not os.path.isfile(src_file_path):
                raise ValueError(f"Source path {src_file_path} is not a file.")
            out_file_path = path_join(dest_dir_path, Path(src_file_path).name)
            shutil.copy(src_file_path, out_file_path)

    def clear(self, info: StdlSegmentsInfo):
        platform_dir_path = path_join(self.src_incomplete_dir_path, info.platform_name)
        channel_dir_path = path_join(platform_dir_path, info.channel_id)
        video_dir_path = path_join(channel_dir_path, info.video_name)
        if not Path(video_dir_path).exists():
            return
        shutil.rmtree(video_dir_path)
        if len(os.listdir(channel_dir_path)) == 0:
            os.rmdir(channel_dir_path)
        if len(os.listdir(platform_dir_path)) == 0:
            os.rmdir(platform_dir_path)
