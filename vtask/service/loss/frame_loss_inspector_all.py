import csv
import statistics

from .frame_loss_config import AllFrameLossConfig
from .frame_loss_inspector import InspectResult, Frame, FrameLossInspector
from .frame_loss_utils import group_consecutive, format_time, extract_frames


class AllFrameLossInspector(FrameLossInspector):
    def __init__(self, conf: AllFrameLossConfig):
        super().__init__()
        self.threshold_byte = conf.threshold_byte
        self.list_capacity = conf.list_capacity
        self.weight_sec = conf.weight_sec

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        extract_frames(vid_path, csv_path, False)
        return self.analyze(csv_path)

    def analyze(self, csv_path: str) -> InspectResult:
        prev_size_list: list[float] = []
        result_times: list[int] = []

        file = open(csv_path, mode="r", encoding=self.encoding)

        prev_sec = 0
        for row in csv.reader(file):
            cur = Frame.from_row(row)
            if len(prev_size_list) > self.list_capacity:
                prev_size_list.pop(0)
            prev_size_list.append(cur.pkt_size)
            avg = statistics.mean(prev_size_list)
            if avg < self.threshold_byte:
                cur_sec = int(cur.pkt_pts_time)
                if prev_sec == cur_sec:
                    continue
                result_times.append(cur_sec)
                prev_sec = cur_sec

        file.close()

        loss_ranges = []
        sum = 0
        for group in group_consecutive(result_times):
            from_sec = group[0] - self.weight_sec
            to_sec = group[1] + self.weight_sec
            loss_ranges.append(f"{format_time(from_sec)}-{format_time(to_sec)}")
            sum += to_sec - from_sec

        return InspectResult(frame_loss_ranges=loss_ranges, total_loss_time=format_time(sum))
