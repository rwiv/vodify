import os
import shutil
from pathlib import Path

from pyutils import path_join

from .stdl_accessor import StdlAccessor
from ...schema import StdlSegmentsInfo
from .....common.fs import FsType


class StdlLocalAccessor(StdlAccessor):
    def __init__(self, local_incomplete_dir_path: str):
        super().__init__(FsType.LOCAL)
        self.src_incomplete_dir_path = local_incomplete_dir_path

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

    def copy(self, info: StdlSegmentsInfo, dest_dir_path: str):
        src_video_dir_path = path_join(
            self.src_incomplete_dir_path, info.platform_name, info.channel_id, info.video_name
        )
        os.makedirs(src_video_dir_path, exist_ok=True)

        for file_name in os.listdir(src_video_dir_path):
            src_file_path = path_join(src_video_dir_path, file_name)
            if not os.path.isfile(src_file_path):
                raise ValueError(f"Source path {src_file_path} is not a file.")
            shutil.copy(src_file_path, path_join(dest_dir_path, file_name))

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
