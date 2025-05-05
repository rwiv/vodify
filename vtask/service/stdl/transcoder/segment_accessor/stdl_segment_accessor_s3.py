from pyutils import path_join, filename

from .stdl_segment_accessor import StdlSegmentAccessor
from ...schema import StdlSegmentsInfo, STDL_INCOMPLETE_DIR_NAME
from .....common.fs import S3Config, FsType
from .....utils import S3Client


class StdlS3SegmentAccessor(StdlSegmentAccessor):
    def __init__(
        self,
        conf: S3Config,
        network_io_delay_ms: int,
        network_buf_size: int,
        retry_limit: int = 3,
    ):
        super().__init__(FsType.S3)
        self.__s3 = S3Client(conf, retry_limit)
        self.network_io_delay_ms = network_io_delay_ms
        self.network_buf_size = network_buf_size

    def get_paths(self, info: StdlSegmentsInfo) -> list[str]:
        return self.__get_keys(info)

    def get_size_sum(self, info: StdlSegmentsInfo) -> int:
        keys = self.__get_keys(info)
        size_sum = 0
        for key in keys:
            head = self.__s3.head(key)
            if head is not None:
                size_sum += head.content_length
        return size_sum

    def copy(self, paths: list[str], dest_dir_path: str):
        for key in paths:
            self.__s3.write_file(
                key=key,
                file_path=path_join(dest_dir_path, filename(key)),
                network_io_delay_ms=self.network_io_delay_ms,
                network_buf_size=self.network_buf_size,
            )

    def __get_keys(self, info: StdlSegmentsInfo):
        chunks_path = path_join(
            STDL_INCOMPLETE_DIR_NAME, info.platform_name, info.channel_id, info.video_name
        )
        keys = []
        for obj in self.__s3.list_all_objects(prefix=chunks_path):
            keys.append(obj.key)
        return keys

    def clear(self, info: StdlSegmentsInfo):
        keys = self.__get_keys(info)
        for key in keys:
            self.__s3.delete(key)
