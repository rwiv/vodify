import shutil
import subprocess
from itertools import groupby

from ...utils import check_dir


def format_time(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_seconds = seconds % 60
    return f"{hours}:{minutes}:{remaining_seconds:.0f}"


def group_consecutive(nums: list[int]):
    result = []
    for _, group in groupby(enumerate(nums), lambda x: x[0] - x[1]):
        group = list(group)
        result.append((group[0][1], group[-1][1]))
    return result


def extract_packets(vid_path: str, csv_path: str, is_print: bool = False):
    if shutil.which("ffprobe") is None:
        raise FileNotFoundError("ffprobe not found")

    command = ["ffprobe", "-select_streams", "v", "-show_packets", "-print_format", "csv", vid_path]

    check_dir(csv_path)
    with open(csv_path, "w") as output_file:
        if is_print:
            subprocess.run(command, stdout=output_file, stderr=None, text=True)
        else:
            subprocess.run(command, stdout=output_file, stderr=subprocess.PIPE, text=True)
