from abc import ABC, abstractmethod

from pydantic import BaseModel, Field


class Channel(BaseModel):
    channel_id: str = Field(alias="channelId")


class VideoContent(BaseModel):
    channel: Channel
    video_id: str = Field(alias="videoId")
    video_title: str = Field(alias="videoTitle")
    paid_product_id: str | None = Field(alias="paidProductId")
    live_rewind_playback_json: str | None = Field(alias="liveRewindPlaybackJson")
    in_key: str | None = Field(alias="inKey")


class VideoResponse(BaseModel):
    content: VideoContent


class ChzzkVideoInfo(BaseModel):
    m3u8_url: str
    query_params: dict[str, list[str]] | None
    title: str
    channel_id: str


class ChzzkVideoClient(ABC):
    @abstractmethod
    async def get_video_info(self, video_no: int) -> ChzzkVideoInfo:
        pass
