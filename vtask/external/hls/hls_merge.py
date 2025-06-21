import aiofiles
from aiofiles import os as aios
from pyutils import path_join

from .hls_utils import stem


async def merge_ts(chunks_path: str) -> str:
    merged_ts_path = f"{chunks_path}.ts"
    async with aiofiles.open(merged_ts_path, "wb") as outfile:
        ts_filenames = [f for f in await aios.listdir(chunks_path) if f.endswith(".ts")]
        for ts_filename in sorted(ts_filenames, key=lambda x: int(stem(x))):
            async with aiofiles.open(path_join(chunks_path, ts_filename), "rb") as infile:
                await outfile.write(await infile.read())
    return merged_ts_path
