import csv
import time

from .loss_inspector import InspectResult, Frame, LossInspector
from .loss_utils import format_time, extract_frames


class TimeLossInspector(LossInspector):
    def __init__(self, keyframe_only: bool = True):
        super().__init__()
        self.threshold_sec = 0.1
        self.round_digits = 3
        self.only_key_frames = keyframe_only

    def inspect(self, vid_path: str, csv_path: str) -> InspectResult:
        start_time = time.time()
        extract_frames(vid_path, csv_path, keyframe_only=self.only_key_frames)
        result = self.analyze(csv_path)
        result.elapsed_time = time.time() - start_time
        return result

    def analyze(self, csv_path: str) -> InspectResult:
        frame_period = self.__check_keyframe_period(csv_path)

        file = open(csv_path, mode="r", encoding=self.encoding)
        loss_ranges = []
        sum_times = 0
        prev = None
        for row in csv.reader(file):
            cur = Frame.from_row(row)
            if prev is None:
                prev = cur
                continue
            if cur.pkt_pts_time - prev.pkt_pts_time > frame_period + self.threshold_sec:
                loss_ranges.append(f"{format_time(prev.pkt_pts_time)}-{format_time(cur.pkt_pts_time)}")
                sum_times += cur.pkt_pts_time - prev.pkt_pts_time
            prev = cur

        file.close()
        return InspectResult(
            frame_period=frame_period,
            loss_ranges=loss_ranges,
            total_loss_time=format_time(sum_times),
        )

    def __check_keyframe_period(self, csv_path: str) -> float:
        file = open(csv_path, mode="r", encoding=self.encoding)
        keyframe_period_counts = {}
        prev = None
        for row in csv.reader(file):
            cur = Frame.from_row(row)
            if prev is None:
                prev = cur
                continue
            keyframe_period = round(number=cur.pkt_pts_time - prev.pkt_pts_time, ndigits=self.round_digits)
            if keyframe_period not in keyframe_period_counts:
                keyframe_period_counts[keyframe_period] = 0
            keyframe_period_counts[keyframe_period] += 1
            prev = cur
        file.close()

        selected = None
        for keyframe_period, count in keyframe_period_counts.items():
            if selected is None:
                selected = (keyframe_period, count)
                continue
            if count > selected[1]:
                selected = (keyframe_period, count)
        if selected is None:
            raise ValueError("No keyframe period found")

        return selected[0]
