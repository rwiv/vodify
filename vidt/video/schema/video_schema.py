from enum import Enum

from pydantic import BaseModel, Field


class VideoPlatform(Enum):
    CHZZK = "chzzk"
    SOOP = "soop"
    MISC = "misc"


class VideoDownloadContext(BaseModel):
    cookie_str: str | None = Field(alias="cookieStr", default=None)
    is_parallel: bool = Field(alias="isParallel")
    parallel_num: int = Field(alias="parallelNum")
    network_mbit: float = Field(alias="networkMbit")
    concat: bool
    use_qs: bool = Field(alias="useQs", default=False)
