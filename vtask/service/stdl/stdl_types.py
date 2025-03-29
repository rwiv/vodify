from enum import Enum

from pydantic import BaseModel, Field, constr


class StdlPlatformType(Enum):
    CHZZK = "chzzk"
    SOOP = "soop"


class StdlDoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class StdlDoneMsg(BaseModel):
    status: StdlDoneStatus
    platform: StdlPlatformType | None = None  # TODO: remove
    uid: constr(min_length=1)
    video_name: constr(min_length=1) = Field(alias="videoName")
    fs_name: constr(min_length=1) = Field(alias="fsName")

    def to_json_dict(self) -> dict:
        return self.model_dump(mode="json", by_alias=True)

    @staticmethod
    def from_dict(dct: dict):
        return StdlDoneMsg(**dct)
