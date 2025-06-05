import requests
from pydantic import BaseModel

from ....utils import get_headers


class SoopM3u8Info(BaseModel):
    video_master_url: str
    audio_media_url: str


class SoopVideoInfo(BaseModel):
    m3u8_infos: list[SoopM3u8Info]
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
        m3u8_infos = [SoopM3u8Info(video_master_url=f["file"], audio_media_url=f["radio_url"]) for f in data["files"]]
        return SoopVideoInfo(
            m3u8_infos=m3u8_infos,
            title=data["full_title"].strip(),
            bj_id=data["bj_id"],
        )
