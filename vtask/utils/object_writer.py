import os
from abc import ABC, abstractmethod
from io import BufferedReader
from pathlib import Path

from pyutils import filename, dirpath

from vtask.common.fs import FsType, S3Config
from vtask.utils import S3Client


class ObjectWriter(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def normalize_base_path(self, base_path: str) -> str:
        pass

    @abstractmethod
    def write(self, path: str, data: bytes | BufferedReader) -> None:
        pass


class LocalObjectWriter(ObjectWriter):
    def __init__(self, chunk_size: int = 4096):
        super().__init__(FsType.LOCAL)
        self.chunk_size = chunk_size

    def normalize_base_path(self, base_path: str) -> str:
        return base_path

    def write(self, path: str, data: bytes | BufferedReader) -> None:
        if not Path(dirpath(path)).exists():
            os.makedirs(dirpath(path), exist_ok=True)
        with open(path, "wb") as f:
            if isinstance(data, bytes):
                f.write(data)
            elif isinstance(data, BufferedReader):
                while True:
                    chunk = data.read(self.chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")


class S3ObjectWriter(ObjectWriter):
    def __init__(self, conf: S3Config):
        super().__init__(FsType.S3)
        self.conf = conf
        self.bucket_name = conf.bucket_name
        self.__s3 = S3Client(self.conf)

    def normalize_base_path(self, base_path: str) -> str:
        return filename(base_path)

    def write(self, path: str, data: bytes | BufferedReader):
        return self.__s3.write(path, data)
