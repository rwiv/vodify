import csv

from .loss_config import TimeFrameLossConfig
from .loss_inspector import InspectResult, Packet, LossInspector
from .loss_utils import format_time, extract_packets


class TimeLossInspector(LossInspector):
    def __init__(self, conf: TimeFrameLossConfig):
        super().__init__()
        self.threshold_sec = conf.threshold_sec

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        extract_packets(vid_path, csv_path)
        return self.analyze(csv_path)

    def analyze(self, csv_path: str) -> InspectResult:
        file = open(csv_path, mode="r", encoding=self.encoding)

        loss_ranges = []
        sum = 0
        prev = None
        for row in csv.reader(file):
            cur = Packet.from_row(row)
            if prev is None:
                prev = cur
                continue
            if cur.dts_time - prev.dts_time > self.threshold_sec:
                loss_ranges.append(f"{format_time(prev.dts_time)}-{format_time(cur.dts_time)}")
                sum += cur.dts_time - prev.dts_time
            prev = cur

        file.close()
        return InspectResult(loss_ranges=loss_ranges, total_loss_time=format_time(sum))
