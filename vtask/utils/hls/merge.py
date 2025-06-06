import os
import shutil
import subprocess

import aiofiles
from aiofiles import os as aios
from pyutils import path_join

from .utils import stem


def merge_ts(chunks_path: str) -> str:
    merged_ts_path = f"{chunks_path}.ts"
    with open(merged_ts_path, "wb") as outfile:
        ts_filenames = [f for f in os.listdir(chunks_path) if f.endswith(".ts")]
        for ts_filename in sorted(ts_filenames, key=lambda x: int(stem(x))):
            with open(path_join(chunks_path, ts_filename), "rb") as infile:
                outfile.write(infile.read())
    return merged_ts_path


async def merge_ts_async(chunks_path: str) -> str:
    merged_ts_path = f"{chunks_path}.ts"
    async with aiofiles.open(merged_ts_path, "wb") as outfile:
        ts_filenames = [f for f in await aios.listdir(chunks_path) if f.endswith(".ts")]
        for ts_filename in sorted(ts_filenames, key=lambda x: int(stem(x))):
            async with aiofiles.open(path_join(chunks_path, ts_filename), "rb") as infile:
                await outfile.write(await infile.read())
    return merged_ts_path


def convert_vid(src_path: str, out_path: str):
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def merge_and_remux_to_mp4(chunks_path: str):
    # merge ts files
    merged_ts_path = merge_ts(chunks_path)
    shutil.rmtree(chunks_path)

    # convert ts to mp4
    mp4_path = f"{chunks_path}.mp4"
    convert_vid(merged_ts_path, mp4_path)
    os.remove(merged_ts_path)
    return mp4_path
