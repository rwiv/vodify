from abc import ABC, abstractmethod

from .....common.fs import FsType


class StdlHelper(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def move(self, channel_id: str, video_name: str, platform_name: str | None = None):
        pass

    @abstractmethod
    def clear(self, channel_id: str, video_name: str, platform_name: str | None = None):
        pass
