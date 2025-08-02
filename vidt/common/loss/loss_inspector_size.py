import csv
import statistics
import time

from .loss_config import LossCheckSizeConfig
from .loss_inspector import InspectResult, Frame, LossInspector
from .loss_utils import group_consecutive, format_time, extract_frames


class SizeLossInspector(LossInspector):
    def __init__(self, conf: LossCheckSizeConfig = LossCheckSizeConfig()):
        super().__init__()
        self.threshold_byte = conf.threshold_byte
        self.list_capacity = conf.list_capacity
        self.weight_sec = conf.weight_sec

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        start_time = time.time()
        extract_frames(vid_path, csv_path, keyframe_only=False)
        result = self.analyze(csv_path)
        result.elapsed_time = time.time() - start_time
        return result

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
        sum_times = 0
        for group in group_consecutive(result_times):
            from_sec = group[0] - self.weight_sec
            to_sec = group[1] + self.weight_sec
            loss_ranges.append(f"{format_time(from_sec)}-{format_time(to_sec)}")
            sum_times += to_sec - from_sec

        return InspectResult(
            loss_ranges=loss_ranges,
            total_loss_time=format_time(sum_times),
        )
