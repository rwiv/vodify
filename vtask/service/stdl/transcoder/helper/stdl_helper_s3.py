import os

from pyutils import path_join, filename

from .stdl_helper import StdlHelper
from ...schema import StdlSegmentsInfo, STDL_INCOMPLETE_DIR_NAME
from .....common.fs import S3Config, FsType
from .....utils import S3Client


class StdlS3Helper(StdlHelper):
    def __init__(
        self,
        local_incomplete_dir_path: str,
        conf: S3Config,
        network_io_delay_ms: int,
        network_buf_size: int,
        retry_limit: int = 3,
    ):
        super().__init__(FsType.S3)
        self.local_incomplete_dir_path = local_incomplete_dir_path
        self.__s3 = S3Client(conf, retry_limit)
        self.network_io_delay_ms = network_io_delay_ms
        self.network_buf_size = network_buf_size

    def move(self, info: StdlSegmentsInfo):
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        keys = self.__get_keys(platform_name=platform_name, channel_id=channel_id, video_name=video_name)
        if len(keys) == 0:
            return

        dir_path = path_join(self.local_incomplete_dir_path, channel_id, video_name)
        if platform_name is not None:
            dir_path = path_join(self.local_incomplete_dir_path, platform_name, channel_id, video_name)
        os.makedirs(dir_path, exist_ok=True)

        for key in keys:
            self.__s3.write_file(
                key=key,
                file_path=path_join(dir_path, filename(key)),
                network_io_delay_ms=self.network_io_delay_ms,
                network_buf_size=self.network_buf_size,
            )
        for key in keys:
            self.__s3.delete(key)

    def __get_keys(self, channel_id: str, video_name: str, platform_name: str | None = None):
        chunks_path = path_join(STDL_INCOMPLETE_DIR_NAME, channel_id, video_name)
        if platform_name is not None:
            chunks_path = path_join(STDL_INCOMPLETE_DIR_NAME, platform_name, channel_id, video_name)
        keys = []
        for obj in self.__s3.list_all_objects(prefix=chunks_path):
            keys.append(obj.key)
        return keys

    def clear(self, info: StdlSegmentsInfo):
        keys = self.__get_keys(
            platform_name=info.platform_name, channel_id=info.channel_id, video_name=info.video_name
        )
        for key in keys:
            self.__s3.delete(key)
