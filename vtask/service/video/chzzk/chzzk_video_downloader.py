import asyncio
import json
from typing import Any

import requests

from .chzzk_playback_types import ChzzkPlayback
from ..schema.video_schema import VideoDownloadContext
from ....utils import get_headers
from ....utils.hls.downloader import HlsDownloader


class ChzzkVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.hls = HlsDownloader(
            out_dir_path=tmp_dir,
            headers=get_headers(ctx.cookie_str),
            parallel_num=ctx.parallel_num,
            non_parallel_delay_ms=ctx.non_parallel_delay_ms,
        )

    def download_one(self, video_no: int) -> str:
        m3u8_url, title, channelId = self._get_info(video_no)
        file_title = str(video_no)
        if self.ctx.is_parallel:
            return asyncio.run(self.hls.download_parallel(m3u8_url, channelId, file_title))
        else:
            return asyncio.run(self.hls.download_non_parallel(m3u8_url, channelId, file_title))

    def _get_info(self, video_no: int) -> tuple[str, str, str]:
        res = self._request_video_info(video_no)
        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        pb = ChzzkPlayback(**json.loads(res["content"]["liveRewindPlaybackJson"]))
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        m3u8_url = pb.media[0].path
        return m3u8_url, title, channelId

    def _request_video_info(self, video_no: int) -> dict[str, Any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.ctx.cookie_str, "application/json"))
        return res.json()
