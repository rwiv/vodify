import asyncio
import subprocess
import sys

from aiofiles import os as aios
from pyutils import log

from .encoding_request import EncodingRequest
from .encoding_resolver import resolve_command
from .progress_parser import parse_encoding_progress
from ..utils import exec_process, cur_duration


class VideoEncoder:
    async def encode(self, req: EncodingRequest, logging: bool = False) -> None:
        if await aios.path.exists(req.out_file_path):
            raise FileExistsError(f"Output file {req.out_file_path} already exists.")

        start_time = asyncio.get_event_loop().time()
        command = resolve_command(req)
        process = await exec_process(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert process.stdout is not None

        stream_q_sum = 0.0
        stream_q_cnt = 0
        bitrate_sum = 0.0
        bitrate_cnt = 0
        speed_sum = 0.0
        speed_cnt = 0

        while True:
            chunk = await process.stdout.read(sys.maxsize)
            if not chunk:
                break
            info = parse_encoding_progress(chunk)
            if info.stream_q is not None:
                stream_q_sum += info.stream_q
                stream_q_cnt += 1
            if info.bitrate_kbits is not None:
                bitrate_sum += info.bitrate_kbits
                bitrate_cnt += 1
            if info.speed is not None:
                speed_sum += info.speed
                speed_cnt += 1
            if logging:
                log.debug("Encoding Progress", info.model_dump(mode="json"))

        await process.wait()
        attr = {
            "output_file": req.out_file_path,
            "quantizer_avg": stream_q_sum / stream_q_cnt if stream_q_cnt > 0 else None,
            "bitrate_avg": bitrate_sum / bitrate_cnt if bitrate_cnt > 0 else None,
            "speed_avg": speed_sum / speed_cnt if speed_cnt > 0 else None,
            "duration": cur_duration(start_time),
        }
        log.info("Encoding completed", attr)
