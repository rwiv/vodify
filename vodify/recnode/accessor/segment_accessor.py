from abc import ABC, abstractmethod

from ..schema.recnode_types import RecnodeSegmentsInfo
from ...common.fs import FsType


class SegmentAccessor(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    async def get_paths(self, info: RecnodeSegmentsInfo) -> list[str]:
        pass

    @abstractmethod
    async def get_size_sum(self, info: RecnodeSegmentsInfo) -> int:
        pass

    @abstractmethod
    async def copy(self, paths: list[str], dest_dir_path: str):
        pass

    @abstractmethod
    async def clear_by_info(self, info: RecnodeSegmentsInfo):
        pass

    @abstractmethod
    async def clear_by_paths(self, paths: list[str]):
        pass
