from abc import ABC, abstractmethod

from ....common.fs import FsType


class StdlHelper(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def move(self, uid: str, video_name: str):
        pass

    @abstractmethod
    def clear(self, uid: str, video_name: str):
        pass
