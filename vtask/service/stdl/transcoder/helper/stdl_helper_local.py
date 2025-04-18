import os
import shutil
from pathlib import Path

from pyutils import path_join

from .stdl_helper import StdlHelper
from ...schema import StdlSegmentsInfo
from .....common.fs import FsType


class StdlLocalHelper(StdlHelper):
    def __init__(self, incomplete_dir_path: str):
        super().__init__(FsType.LOCAL)
        self.incomplete_dir_path = incomplete_dir_path

    def move(self, info: StdlSegmentsInfo):
        pass

    def clear(self, info: StdlSegmentsInfo):
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        dir_path = path_join(self.incomplete_dir_path, channel_id, video_name)
        if platform_name is not None:
            dir_path = path_join(self.incomplete_dir_path, platform_name, channel_id, video_name)
        if Path(dir_path).exists():
            shutil.rmtree(dir_path)
        self.__clear_incomplete_dir(channel_id)

    def __clear_incomplete_dir(self, channel_id: str, platform_name: str | None = None):
        channel_dir_path = path_join(self.incomplete_dir_path, channel_id)
        if platform_name is not None:
            channel_dir_path = path_join(self.incomplete_dir_path, platform_name, channel_id)

        if len(os.listdir(channel_dir_path)) == 0:
            os.rmdir(channel_dir_path)

        if platform_name is not None:
            platform_dir_path = path_join(self.incomplete_dir_path, platform_name)
            if len(os.listdir(platform_dir_path)) == 0:
                os.rmdir(platform_dir_path)
