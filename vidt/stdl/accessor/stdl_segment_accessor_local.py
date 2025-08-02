from pathlib import Path

from aiofiles import os as aios
from pyutils import path_join

from .stdl_segment_accessor import StdlSegmentAccessor
from ..schema.stdl_types import StdlSegmentsInfo
from ...common.fs import FsType
from ...utils import copy_file, rmtree


class StdlLocalSegmentAccessor(StdlSegmentAccessor):
    def __init__(self, local_incomplete_dir_path: str):
        super().__init__(FsType.LOCAL)
        self.src_incomplete_dir_path = local_incomplete_dir_path

    async def get_paths(self, info: StdlSegmentsInfo) -> list[str]:
        dir_path = path_join(self.src_incomplete_dir_path, info.platform_name, info.channel_id, info.video_name)
        return [path_join(dir_path, file_name) for file_name in await aios.listdir(dir_path)]

    async def get_size_sum(self, info: StdlSegmentsInfo) -> int:
        dir_path = path_join(self.src_incomplete_dir_path, info.platform_name, info.channel_id, info.video_name)
        if not await aios.path.exists(dir_path):
            return 0
        size_sum = 0
        for file_name in await aios.listdir(dir_path):
            file_path = path_join(dir_path, file_name)
            if not await aios.path.isfile(file_path):
                raise ValueError(f"Source path {file_path} is not a file.")
            size_sum += await aios.path.getsize(file_path)
        return size_sum

    async def copy(self, paths: list[str], dest_dir_path: str):
        for src_file_path in paths:
            if not await aios.path.isfile(src_file_path):
                raise ValueError(f"Source path {src_file_path} is not a file.")
            out_file_path = path_join(dest_dir_path, Path(src_file_path).name)
            await copy_file(src=src_file_path, dst=out_file_path)

    async def clear_by_info(self, info: StdlSegmentsInfo):
        platform_dir_path = path_join(self.src_incomplete_dir_path, info.platform_name)
        channel_dir_path = path_join(platform_dir_path, info.channel_id)
        video_dir_path = path_join(channel_dir_path, info.video_name)
        if not await aios.path.exists(video_dir_path):
            return
        await rmtree(video_dir_path)
        if len(await aios.listdir(channel_dir_path)) == 0:
            await aios.rmdir(channel_dir_path)
        if len(await aios.listdir(platform_dir_path)) == 0:
            await aios.rmdir(platform_dir_path)

    async def clear_by_paths(self, paths: list[str]):
        for src_file_path in paths:
            if not await aios.path.isfile(src_file_path):
                raise ValueError(f"Source path {src_file_path} is not a file.")
            await aios.remove(src_file_path)
