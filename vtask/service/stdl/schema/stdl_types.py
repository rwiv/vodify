from enum import Enum

from pydantic import BaseModel, Field, constr


class StdlPlatformType(Enum):
    CHZZK = "chzzk"
    SOOP = "soop"
    TWITCH = "twitch"


class StdlDoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class StdlSegmentsInfo(BaseModel):
    platform_name: str
    channel_id: str
    video_name: str
    video_size_gb: float | None = None
    conditionally_archive: bool = False
    should_archive: bool = False

    def to_dict(self, extra: dict | None = None) -> dict:
        result = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        if extra:
            for k, v in extra.items():
                result[k] = v
        return result


class StdlDoneMsg(BaseModel):
    status: StdlDoneStatus
    platform: StdlPlatformType
    uid: constr(min_length=1)
    video_name: constr(min_length=1) = Field(alias="videoName")
    fs_name: constr(min_length=1) = Field(alias="fsName")
    conditionally_archive: bool = Field(alias="conditionallyArchive", default=False)
    should_archive: bool = Field(alias="conditionallyArchive", default=False)

    def to_json_dict(self) -> dict:
        return self.model_dump(mode="json", by_alias=True)

    @staticmethod
    def from_dict(dct: dict):
        return StdlDoneMsg(**dct)

    def to_segments_info(self) -> StdlSegmentsInfo:
        return StdlSegmentsInfo(
            platform_name=self.platform.value,
            channel_id=self.uid,
            video_name=self.video_name,
            conditionally_archive=self.conditionally_archive,
            should_archive=self.should_archive,
        )
