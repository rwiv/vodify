import json
import subprocess

from pydantic import BaseModel

from ..utils import run_process


class FfprobeStream(BaseModel):
    index: int
    codec_name: str
    codec_long_name: str
    codec_type: str
    duration: float
    tags: dict[str, str]


class FfprobeFormat(BaseModel):
    filename: str
    nb_streams: int
    nb_programs: int
    nb_stream_groups: int
    format_name: str
    format_long_name: str | None = None
    size: int
    probe_score: int
    tags: dict[str, str]


class FfprobeInfo(BaseModel):
    streams: list[FfprobeStream]
    format: FfprobeFormat


async def get_info(file_path: str) -> FfprobeInfo:
    command = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", file_path]
    stdout, stderr = await run_process(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if stderr:
        raise Exception(f"ffprobe returned stderr: {stderr}")
    return FfprobeInfo(**json.loads(stdout))
