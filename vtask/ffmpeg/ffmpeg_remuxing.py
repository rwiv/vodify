import subprocess

from ..utils import run_process


async def remux_video(src_path: str, out_path: str):
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    await run_process(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


async def concat_streams(video_path: str, audio_path: str, out_path: str):
    command = ["ffmpeg", "-i", video_path, "-i", audio_path, "-c", "copy", out_path]
    await run_process(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


async def concat_by_list(list_file_path: str, out_path: str):
    command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file_path, "-c", "copy", out_path]
    await run_process(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
