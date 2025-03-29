from abc import ABC, abstractmethod

from pydantic import BaseModel, Field


class InspectResult(BaseModel):
    loss_ranges: list[str] = Field(serialization_alias="lossRanges")
    total_loss_time: str = Field(serialization_alias="totalLossTime")


class Packet(BaseModel):
    codec_type: str
    stream_index: int
    pts: int
    pts_time: float
    dts: int
    dts_time: float
    duration: int
    duration_time: float
    pos: int
    size: int

    @staticmethod
    def from_row(row: list[str]):
        return Packet(codec_type=row[1], stream_index=row[2], pts=row[3], pts_time=row[4], dts=row[5], dts_time=row[6], duration=row[7], duration_time=row[8], size=row[9], pos=row[10])  # type: ignore


class LossInspector(ABC):
    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    @abstractmethod
    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        pass

    @abstractmethod
    def analyze(self, csv_path: str) -> InspectResult:
        pass
