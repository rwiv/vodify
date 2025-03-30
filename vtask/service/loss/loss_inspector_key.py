import csv

from .loss_config import KeyFrameLossConfig
from .loss_inspector import InspectResult, Frame, LossInspector
from .loss_utils import format_time, extract_frames


class KeyFrameLossInspector(LossInspector):
    def __init__(self, conf: KeyFrameLossConfig):
        super().__init__()
        self.threshold_sec = conf.threshold_sec

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        extract_frames(vid_path, csv_path, only_key_frames=True)
        return self.analyze(csv_path)

    def analyze(self, csv_path: str) -> InspectResult:
        file = open(csv_path, mode="r", encoding=self.encoding)

        loss_ranges = []
        sum = 0
        prev = None
        for row in csv.reader(file):
            cur = Frame.from_row(row)
            if prev is None:
                prev = cur
                continue
            if cur.pkt_pts_time - prev.pkt_pts_time > self.threshold_sec:
                loss_ranges.append(f"{format_time(prev.pkt_pts_time)}-{format_time(cur.pkt_pts_time)}")
                sum += cur.pkt_pts_time - prev.pkt_pts_time
            prev = cur

        file.close()
        return InspectResult(loss_ranges=loss_ranges, total_loss_time=format_time(sum))
