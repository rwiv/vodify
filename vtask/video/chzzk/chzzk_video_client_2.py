import json
from typing import Any

from pydantic import BaseModel

from vtask.utils import get_headers, fetch_json
from .chzzk_video_client import ChzzkVideoInfo, ChzzkVideoClient


class Media(BaseModel):
    path: str


class ChzzkPlayback(BaseModel):
    media: list[Media]


class ChzzkVideoClient2(ChzzkVideoClient):
    def __init__(self, cookie_str: str | None):
        self.cookie_str = cookie_str

    async def get_video_info(self, video_no: int) -> ChzzkVideoInfo:
        res = await self._request_video_info(video_no)
        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        pb = ChzzkPlayback(**json.loads(res["content"]["liveRewindPlaybackJson"]))

        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        return ChzzkVideoInfo(
            m3u8_url=pb.media[0].path,
            title=title,
            channel_id=channelId,
        )

    async def _request_video_info(self, video_no: int) -> dict[str, Any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        return await fetch_json(url=url, headers=get_headers(self.cookie_str, "application/json"))
