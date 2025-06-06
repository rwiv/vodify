from abc import ABC, abstractmethod

from pydantic import BaseModel


class ChzzkVideoInfo(BaseModel):
    m3u8_url: str
    qs: str | None = None
    title: str
    channel_id: str


class ChzzkVideoClient(ABC):
    @abstractmethod
    def get_video_info(self, video_no: int) -> ChzzkVideoInfo:
        pass
