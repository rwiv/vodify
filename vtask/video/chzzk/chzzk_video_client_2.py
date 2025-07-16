import json
from typing import Any

from pydantic import BaseModel
from pyutils import parse_query_params

from .chzzk_video_client import ChzzkVideoInfo, ChzzkVideoClient, VideoResponse
from ...utils import get_headers, fetch_json


class Media(BaseModel):
    path: str


class Playback(BaseModel):
    media: list[Media]


class ChzzkVideoClient2(ChzzkVideoClient):
    def __init__(self, cookie_str: str | None):
        self.cookie_str = cookie_str

    async def get_video_info(self, video_no: int) -> ChzzkVideoInfo:
        res = await self._request_video_info(video_no)
        content = VideoResponse(**res).content
        if content.live_rewind_playback_json is None:
            raise ValueError("liveRewindPlaybackJson is null")
        pb = Playback(**json.loads(content.live_rewind_playback_json))
        query_params = None

        m3u8_url = pb.media[0].path
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        if content.paid_product_id is not None:
            query_params = get_prime_params(m3u8_url)

        return ChzzkVideoInfo(
            m3u8_url=m3u8_url,
            query_params=query_params,
            title=content.video_title.strip(),
            channel_id=content.channel.channel_id,
        )

    async def _request_video_info(self, video_no: int) -> dict[str, Any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        return await fetch_json(url=url, headers=get_headers(self.cookie_str, "application/json"))


def get_prime_params(url: str) -> dict[str, list[str]]:
    params = parse_query_params(url)
    params["__bgda__"] = params["hdnts"]
    del params["hdnts"]
    return params
