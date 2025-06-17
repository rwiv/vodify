from enum import Enum

from pydantic import BaseModel, Field


class VideoCodec(Enum):
    H265 = "h265"
    AV1 = "av1"
    COPY = "copy"


class AudioCodec(Enum):
    AAC = "aac"
    OPUS = "opus"
    COPY = "copy"


class VideoScale(BaseModel):
    width: int
    height: int


class EncodingRequest(BaseModel):
    src_file_path: str = Field(alias="srcFilePath")
    out_file_path: str = Field(alias="outFilePath")
    video_codec: VideoCodec = Field(alias="videoCodec", default=VideoCodec.COPY)
    video_quality: int | None = Field(alias="videoQuality", default=None)
    video_preset: str | None = Field(alias="videoPreset", default=None)
    video_scale: VideoScale | None = Field(alias="videoScale", default=None)
    video_frame: int | None = Field(alias="videoFrame", default=None)
    video_max_bitrate: int | None = Field(alias="videoMaxBitrate", default=None)
    audio_codec: AudioCodec = Field(alias="audioCodec", default=AudioCodec.COPY)
    audio_bitrate_kb: int | None = Field(alias="audioBitrateKb", default=None)
    enable_gpu: bool = Field(alias="enableGpu", default=False)
