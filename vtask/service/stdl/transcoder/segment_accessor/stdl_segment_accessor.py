from abc import ABC, abstractmethod

from ...schema import StdlSegmentsInfo
from .....common.fs import FsType


class StdlSegmentAccessor(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def get_paths(self, info: StdlSegmentsInfo) -> list[str]:
        pass

    @abstractmethod
    def get_size_sum(self, info: StdlSegmentsInfo) -> int:
        pass

    @abstractmethod
    def copy(self, paths: list[str], dest_dir_path: str):
        pass

    @abstractmethod
    def clear(self, info: StdlSegmentsInfo):
        pass
