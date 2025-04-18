from abc import ABC, abstractmethod

from ...schema import StdlSegmentsInfo
from .....common.fs import FsType


class StdlHelper(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def move(self, info: StdlSegmentsInfo):
        pass

    @abstractmethod
    def clear(self, info: StdlSegmentsInfo):
        pass
