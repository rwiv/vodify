from abc import ABC, abstractmethod

from pydantic import BaseModel, Field


class InspectResult(BaseModel):
    frame_period: float | None = Field(serialization_alias="framePeriod", default=None)
    loss_ranges: list[str] = Field(serialization_alias="lossRanges")
    total_loss_time: str = Field(serialization_alias="totalLossTime")
    elapsed_time: float | None = Field(serialization_alias="elapsedTime", default=None)

    def to_out_dict(self):
        copied: InspectResult = self.copy()
        copied.elapsed_time = None
        return copied.model_dump(by_alias=True, exclude_none=True)


class Frame(BaseModel):
    pkt_pts_time: float
    pkt_size: int

    @staticmethod
    def from_row(row: list[str]):
        return Frame(pkt_pts_time=row[5], pkt_size=row[13])  # type: ignore


class LossInspector(ABC):
    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    @abstractmethod
    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        pass

    @abstractmethod
    def analyze(self, csv_path: str) -> InspectResult:
        pass
