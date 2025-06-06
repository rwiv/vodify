from typing import Any
from urllib.parse import urlparse, parse_qs
from xml.etree.ElementTree import fromstring, Element

import requests
from pyutils import get_base_url

from .chzzk_video_client import ChzzkVideoInfo, ChzzkVideoClient
from ...utils import get_headers


class ChzzkVideoClient1(ChzzkVideoClient):
    def __init__(self, cookie_str: str | None):
        self.cookie_str = cookie_str

    def get_video_info(self, video_no: int) -> ChzzkVideoInfo:
        res = self.__request_video_info(video_no)

        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        videoId = res["content"]["videoId"]
        key = res["content"]["inKey"]

        m3u_url, lsu_sa, base_url = self.__request_play_info(videoId, key)

        return ChzzkVideoInfo(
            m3u8_url=m3u_url,
            qs=f"_lsu_sa_={lsu_sa}",
            title=title,
            channel_id=channelId,
        )

    def __request_video_info(self, video_no: int) -> dict[str, Any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.cookie_str, "application/json"))
        return res.json()

    def __request_play_info(self, video_id: str, key: str):
        url = f"https://apis.naver.com/neonplayer/vodplay/v2/playback/{video_id}?key={key}"
        res = requests.get(url, headers=get_headers(self.cookie_str, "application/xml")).text
        root: Element = fromstring(res)
        if len(root) != 1:
            raise ValueError("root element should be 1")
        period = root[0]
        m3u_url = ""
        for child in period:
            attr = child.attrib
            if attr["mimeType"] == "video/mp2t":
                target_key = ""
                for key in attr.keys():
                    if key.endswith("m3u"):
                        target_key = key
                        break
                if target_key == "":
                    raise ValueError("target key not found")
                m3u_url = attr[target_key]
                break
        if m3u_url == "":
            raise ValueError("m3u_url not found")

        lsu_sa = find_query_value_one(m3u_url, "_lsu_sa_")
        base_url = get_base_url(m3u_url)
        return m3u_url, lsu_sa, base_url


def find_query_value_one(url: str, key: str) -> str:
    parsed_rul = urlparse(url)
    params = parse_qs(parsed_rul.query)
    values = params[key]
    if len(values) != 1:
        raise ValueError("query values should be 1")
    return values[0]
