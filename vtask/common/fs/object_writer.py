from abc import ABC, abstractmethod

import aiofiles
from aiofiles import os as aios
from pyutils import filename, dirpath

from .fs_types import FsType
from ...external.s3 import S3AsyncClient


class ObjectWriter(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def normalize_base_path(self, base_path: str) -> str:
        pass

    @abstractmethod
    async def write(self, path: str, data: bytes) -> None:
        pass


class LocalObjectWriter(ObjectWriter):
    def __init__(self, chunk_size: int = 4096):
        super().__init__(FsType.LOCAL)
        self.chunk_size = chunk_size

    def normalize_base_path(self, base_path: str) -> str:
        return base_path

    async def write(self, path: str, data: bytes) -> None:
        if not await aios.path.exists(dirpath(path)):
            await aios.makedirs(dirpath(path), exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)


class S3ObjectWriter(ObjectWriter):
    def __init__(self, s3_client: S3AsyncClient):
        super().__init__(FsType.S3)
        self.__s3 = s3_client

    def normalize_base_path(self, base_path: str) -> str:
        return filename(base_path)

    async def write(self, path: str, data: bytes):
        return await self.__s3.write(path, data)
