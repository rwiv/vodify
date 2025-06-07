from pathlib import Path

from aiofiles import os as aios

from ..ffmpeg import remux_video
from ..utils import rmtree, move_file
from ..utils.hls import merge_ts


async def convert_to_mp4(file_path: str, segments_path: str):
    # merge and remux
    merged_ts_path = await merge_ts(segments_path)
    await rmtree(segments_path)
    tmp_mp4_path = f"{segments_path}.mp4"
    await remux_video(merged_ts_path, tmp_mp4_path)
    await aios.remove(merged_ts_path)

    # move to out dir
    await aios.makedirs(Path(file_path).parent, exist_ok=True)
    await move_file(tmp_mp4_path, file_path)
