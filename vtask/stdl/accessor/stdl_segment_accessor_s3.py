from pyutils import path_join, filename, log

from .stdl_segment_accessor import StdlSegmentAccessor
from ..schema.stdl_constrants import STDL_INCOMPLETE_DIR_NAME
from ..schema.stdl_types import StdlSegmentsInfo
from ...common.fs import FsType
from ...utils import S3AsyncClient, avg


class StdlS3SegmentAccessor(StdlSegmentAccessor):
    def __init__(self, s3_client: S3AsyncClient):
        super().__init__(FsType.S3)
        self.__s3 = s3_client

    async def get_paths(self, info: StdlSegmentsInfo) -> list[str]:
        return await self.__get_keys(info)

    async def get_size_sum(self, info: StdlSegmentsInfo) -> int:
        keys = await self.__get_keys(info)
        size_sum = 0
        for key in keys:
            head = await self.__s3.head(key)
            if head is not None:
                size_sum += head.content_length
        return size_sum

    async def copy(self, paths: list[str], dest_dir_path: str):
        retry_cnt = 0
        wasted_bytes = 0
        small_chunk_counts = []

        for key in paths:
            file_path = path_join(dest_dir_path, filename(key))
            ret = await self.__s3.write_file(key=key, file_path=file_path, sync_time=True)
            small_chunk_counts.append(ret.small_chunk_count)
            wasted_bytes += ret.wasted_bytes
            retry_cnt += ret.retry_count

        attr = {
            "retry_count": retry_cnt,
            "wasted_bytes_mb": wasted_bytes / 1024 / 1024,
            "small_chunk_count_avg": avg(small_chunk_counts),
            "small_chunk_count_max": max(small_chunk_counts),
        }
        log.debug("Download objects from S3", attr)

    async def __get_keys(self, info: StdlSegmentsInfo):
        chunks_path = path_join(STDL_INCOMPLETE_DIR_NAME, info.platform_name, info.channel_id, info.video_name)
        keys = []
        async for obj in self.__s3.list_all_objects(prefix=chunks_path):
            keys.append(obj.key)
        return keys

    async def clear_by_info(self, info: StdlSegmentsInfo):
        keys = await self.__get_keys(info)
        await self.clear_by_paths(keys)

    async def clear_by_paths(self, paths: list[str]):
        for key in paths:
            await self.__s3.delete(key)
