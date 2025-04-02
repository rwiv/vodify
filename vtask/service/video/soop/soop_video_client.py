import requests
from pydantic import BaseModel

from ....utils import get_headers


class SoopVideoInfo(BaseModel):
    m3u8_urls: list[str]
    title: str
    bj_id: str


class SoopVideoClient:
    def __init__(self, cookie_str: str | None):
        self.cookie_str = cookie_str

    def get_video_info(self, title_no: int) -> SoopVideoInfo:
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(
            url,
            headers=get_headers(self.cookie_str, "application/json"),
            data={
                "nTitleNo": title_no,
                "nApiLevel": 10,
                "nPlaylistIdx": 0,
            },
        ).json()
        data = res["data"]
        return SoopVideoInfo(
            m3u8_urls=[f["file"] for f in data["files"]],
            title=data["full_title"].strip(),
            bj_id=data["bj_id"],
        )
