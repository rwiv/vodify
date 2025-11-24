from pathlib import Path

from aiofiles import os as aios
from pyutils import path_join

from ..schema.recnode_types import RecnodeSegmentsInfo


async def clear_dir(
    base_dir_path: str,
    info: RecnodeSegmentsInfo,
    delete_platform: bool = False,
    delete_self: bool = False,
):
    platform_dir_path = path_join(base_dir_path, info.platform_name)
    channel_dir_path = path_join(platform_dir_path, info.channel_id)
    video_dir_path = path_join(channel_dir_path, info.video_name)
    if Path(video_dir_path).exists() and len(await aios.listdir(video_dir_path)) == 0:
        await aios.rmdir(video_dir_path)
    if Path(channel_dir_path).exists() and len(await aios.listdir(channel_dir_path)) == 0:
        await aios.rmdir(channel_dir_path)
    if delete_platform:
        if Path(platform_dir_path).exists() and len(await aios.listdir(platform_dir_path)) == 0:
            await aios.rmdir(platform_dir_path)
        if delete_self:
            if Path(base_dir_path).exists() and len(await aios.listdir(base_dir_path)) == 0:
                await aios.rmdir(base_dir_path)
