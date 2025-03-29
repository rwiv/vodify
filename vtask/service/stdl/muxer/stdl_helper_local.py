import os
import shutil
from pathlib import Path

from pyutils import path_join

from .stdl_helper import StdlHelper
from ....common.fs import FsType


class StdlLocalHelper(StdlHelper):
    def __init__(self, incomplete_dir_path: str):
        super().__init__(FsType.LOCAL)
        self.incomplete_dir_path = incomplete_dir_path

    def move(self, uid: str, video_name: str):
        pass

    def clear(self, uid: str, video_name: str):
        dir_path = path_join(self.incomplete_dir_path, uid, video_name)
        if Path(dir_path).exists():
            shutil.rmtree(dir_path)
        self.__clear_incomplete_dir(uid)

    def __clear_incomplete_dir(self, uid: str):
        incomplete_uid_dir_path = path_join(self.incomplete_dir_path, uid)
        if len(os.listdir(incomplete_uid_dir_path)) == 0:
            os.rmdir(incomplete_uid_dir_path)
