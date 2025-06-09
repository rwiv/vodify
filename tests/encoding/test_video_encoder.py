import logging
from pathlib import Path

from aiofiles import os as aios
import pytest
from pyutils import find_project_root, path_join, log

from vtask.encoding import VideoEncoder, EncodingRequest

base_dir_path = path_join(find_project_root(), "dev", "test")


@pytest.mark.asyncio
async def test_video_encoder():
    log.set_level(logging.DEBUG)
    encoder = VideoEncoder()
    src_file_path = path_join(base_dir_path, "assets", "enc", "test.mp4")
    out_file_path = path_join(base_dir_path, "out", "test.mp4")
    await aios.makedirs(Path(out_file_path).parent, exist_ok=True)
    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoCodec="h265",  # type: ignore
        videoQuality=32,
        videoPreset="p4",
        videoScale={"width": 1280, "height": 720},  # type: ignore
        videoFrame=30,
        audioCodec="opus",  # type: ignore
        audioBitrateKb=128,
        enableGpu=True,
    )
    await encoder.encode(req)
