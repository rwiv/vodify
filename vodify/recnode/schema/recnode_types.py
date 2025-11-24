from enum import Enum

from pydantic import BaseModel, Field, constr


class RecnodePlatformType(Enum):
    CHZZK = "chzzk"
    SOOP = "soop"
    TWITCH = "twitch"


class RecnodeDoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class RecnodeSegmentsInfo(BaseModel):
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


class RecnodeMsg(BaseModel):
    status: RecnodeDoneStatus
    platform: RecnodePlatformType
    uid: constr(min_length=1)
    video_name: constr(min_length=1) = Field(alias="videoName")
    fs_name: constr(min_length=1) = Field(alias="fsName")
    cond_archive: bool = Field(alias="condArchive", default=False)
    should_archive: bool = Field(alias="shouldArchive", default=False)

    def to_json_dict(self) -> dict:
        return self.model_dump(mode="json", by_alias=True)

    @staticmethod
    def from_dict(dct: dict):
        return RecnodeMsg(**dct)

    def to_segments_info(self) -> RecnodeSegmentsInfo:
        return RecnodeSegmentsInfo(
            platform_name=self.platform.value,
            channel_id=self.uid,
            video_name=self.video_name,
            conditionally_archive=self.cond_archive,
            should_archive=self.should_archive,
        )
