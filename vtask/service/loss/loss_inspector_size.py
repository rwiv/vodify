import csv
import statistics

from .loss_config import SizeFrameLossConfig
from .loss_inspector import InspectResult, Packet, LossInspector
from .loss_utils import group_consecutive, format_time, extract_packets


class SizeLossInspector(LossInspector):
    def __init__(self, conf: SizeFrameLossConfig):
        super().__init__()
        self.threshold_byte = conf.threshold_byte
        self.list_capacity = conf.list_capacity
        self.weight_sec = conf.weight_sec

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        extract_packets(vid_path, csv_path)
        return self.analyze(csv_path)

    def analyze(self, csv_path: str) -> InspectResult:
        prev_size_list: list[float] = []
        result_times: list[int] = []

        file = open(csv_path, mode="r", encoding=self.encoding)

        prev_sec = 0
        for row in csv.reader(file):
            cur = Packet.from_row(row)
            if len(prev_size_list) > self.list_capacity:
                prev_size_list.pop(0)
            prev_size_list.append(cur.size)
            avg = statistics.mean(prev_size_list)
            if avg < self.threshold_byte:
                cur_sec = int(cur.pts_time)
                if prev_sec == cur_sec:
                    continue
                result_times.append(cur_sec)
                prev_sec = cur_sec

        file.close()

        loss_ranges = []
        sum_secs = 0
        for group in group_consecutive(result_times):
            from_sec = group[0] - self.weight_sec
            to_sec = group[1] + self.weight_sec
            loss_ranges.append(f"{format_time(from_sec)}-{format_time(to_sec)}")
            sum_secs += to_sec - from_sec

        return InspectResult(loss_ranges=loss_ranges, total_loss_time=format_time(sum_secs))
