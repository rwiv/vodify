import os
import shutil
import subprocess
import tarfile
from enum import Enum
from pathlib import Path
from typing import TypedDict

import yaml
from pydantic import BaseModel, Field
from pyutils import log, path_join

from .accessor.stdl_accessor import StdlAccessor
from ..schema import StdlSegmentsInfo
from ...loss import TimeLossInspector
from ....common.notifier import Notifier


class StdlDoneTaskStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class StdlDoneTaskResult(TypedDict):
    status: str
    message: str


class InspectResult(BaseModel):
    # segment_period: float | None = Field(serialization_alias="segmentPeriod", default=None)
    # loss_ranges: list[str] = Field(serialization_alias="lossRanges")
    # total_loss_time: str = Field(serialization_alias="totalLossTime")
    missing_segments: list[int] = Field(serialization_alias="missingSegments")

    def to_out_dict(self):
        return self.model_dump(by_alias=True, exclude_none=True)


class StdlTranscoder:
    def __init__(
        self,
        accessor: StdlAccessor,
        notifier: Notifier,
        out_dir_path: str,
        tmp_path: str,
        is_archive: bool,
        video_size_limit_gb: int,
    ):
        self.accessor = accessor
        self.notifier = notifier
        self.tmp_path = tmp_path
        # self.incomplete_dir_path = path_join(base_path, STDL_INCOMPLETE_DIR_NAME)
        self.out_tmp_dir_path = path_join(out_dir_path, "_tmp")
        self.out_dir_path = out_dir_path
        self.is_archive = is_archive
        self.video_size_limit_gb = video_size_limit_gb
        self.loss_inspector = TimeLossInspector(keyframe_only=False)

    def clear(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        self.accessor.clear(info)
        return _get_success_result("Clear success")

    def transcode(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        tars_size_sum = self.accessor.get_size_sum(info)
        tars_size_sum_gb = round(tars_size_sum / 1024 / 1024 / 1024, 4)
        log.info(f"Video size: {tars_size_sum_gb}GB")
        if tars_size_sum > (self.video_size_limit_gb * 1024 * 1024 * 1024):
            message = f"Video size is too large: platform={platform_name}, channel_id={channel_id}, video_name={video_name}, size={tars_size_sum_gb}GB"
            self.notifier.notify(message)
            raise ValueError(message)

        # Copy segments from remote storage
        base_dir_path = path_join(self.tmp_path, platform_name, channel_id, video_name)
        tars_dir_path = path_join(base_dir_path, "tars")
        os.makedirs(tars_dir_path, exist_ok=True)
        self.accessor.copy(info, tars_dir_path)

        # Preprocess tar files
        if not Path(tars_dir_path).exists():
            raise ValueError(f"Source path {tars_dir_path} does not exist.")
        tar_names = os.listdir(tars_dir_path)
        if len(tar_names) == 0:
            raise ValueError(f"Source path {tars_dir_path} is empty.")
        for tar_name in tar_names:
            if not tar_name.endswith(".tar"):
                raise ValueError(f"Invalid file ext: {path_join(tars_dir_path, tar_name)}")

        # Extract tar files
        extract_dir_path = path_join(base_dir_path, "extract")
        os.makedirs(extract_dir_path, exist_ok=True)

        for tar_filename in os.listdir(tars_dir_path):
            src_tar_path = path_join(tars_dir_path, tar_filename)
            extracted_dir_path = path_join(extract_dir_path, Path(tar_filename).stem)
            os.makedirs(extracted_dir_path, exist_ok=True)
            with tarfile.open(src_tar_path, "r:*") as tar:
                tar.extractall(path=extracted_dir_path)
            os.remove(src_tar_path)

        os.rmdir(tars_dir_path)

        extract_seg_paths = []
        for root, _, files in os.walk(extract_dir_path):
            for file in files:
                if not file.endswith(".ts"):
                    raise ValueError(f"Invalid file ext: {path_join(root, file)}")
                extract_seg_paths.append(path_join(root, file))

        # Check for deplicate segments
        seg_map: dict[int, str] = {}
        for seg_path in extract_seg_paths:
            seg_num = int(Path(seg_path).stem)
            if seg_num in seg_map:
                prev_size = Path(seg_map[seg_num]).stat().st_size
                cur_size = Path(seg_path).stat().st_size
                if prev_size != cur_size:
                    dct = {
                        "path1": seg_map[seg_num],
                        "path2": seg_path,
                        "size1": prev_size,
                        "size2": cur_size,
                    }
                    log.error(f"File size mismatch", dct)
            else:
                seg_map[seg_num] = seg_path

        for _, seg_path in seg_map.items():
            shutil.move(seg_path, path_join(base_dir_path, Path(seg_path).name))

        shutil.rmtree(extract_dir_path)

        # Check for missing segments
        segment_paths = _get_sorted_segment_paths(segments_path=base_dir_path)
        if len(segment_paths) == 0:
            raise ValueError(f"Source path {base_dir_path} is empty.")

        seg_nums = [int(Path(path).stem) for path in segment_paths]
        if -1 in seg_nums and seg_nums[0] != -1:  # check if segment sorted order is valid
            raise ValueError(f"Invalid segment sorted order: {segment_paths}")

        seg_nums = set([int(Path(path).stem) for path in segment_paths])
        missing_seg_nums: list[int] = []
        start_num = int(Path(segment_paths[0]).stem)
        if start_num in (0, -1) and len(segment_paths) > 0:
            start_num = int(Path(segment_paths[1]).stem)
        end_num = int(Path(segment_paths[-1]).stem)
        for cur_num in range(start_num, end_num + 1):
            if cur_num not in seg_nums:
                missing_seg_nums.append(cur_num)

        inspect_result = InspectResult(
            missing_segments=missing_seg_nums,
        )

        # Merge segments
        merged_tmp_ts_path = path_join(base_dir_path, f"{video_name}.ts")
        with open(merged_tmp_ts_path, "wb") as outfile:
            for file_path in segment_paths:
                with open(file_path, "rb") as infile:
                    outfile.write(infile.read())

        # Remux video
        tmp_mp4_path = path_join(base_dir_path, f"{video_name}.mp4")
        _remux_video(merged_tmp_ts_path, tmp_mp4_path)
        os.remove(merged_tmp_ts_path)

        # Move mp4 file
        self.move_mp4(tmp_mp4_path=tmp_mp4_path, info=info)
        complete_loss_path = path_join(self.out_dir_path, platform_name, channel_id, f"{video_name}.yaml")
        with open(complete_loss_path, "w") as file:
            file.write(yaml.dump(inspect_result.to_out_dict(), allow_unicode=True))

        shutil.rmtree(base_dir_path)
        clear_dir(self.tmp_path, info, delete_platform=True)
        clear_dir(self.out_tmp_dir_path, info, delete_platform=True, delete_self=True)

        if not self.is_archive:
            self.accessor.clear(info)

        log.info(f"Convert file: {channel_id}/{video_name}")
        return _get_success_result(f"Convert success: {channel_id}/{video_name}")

    def move_mp4(self, tmp_mp4_path: str, info: StdlSegmentsInfo):
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        # Move mp4 file to complete directory
        out_tmp_channel_dir_path = path_join(self.out_tmp_dir_path, platform_name, channel_id)
        os.makedirs(out_tmp_channel_dir_path, exist_ok=True)
        out_tmp_mp4_path = path_join(out_tmp_channel_dir_path, f"{video_name}.mp4")
        complete_mp4_path = path_join(self.out_dir_path, platform_name, channel_id, f"{video_name}.mp4")

        # write 도중인 파일이 complete directory에 들어가면 안되기 때문에 먼저 incomplete directory로 이동
        shutil.move(tmp_mp4_path, out_tmp_mp4_path)

        # incomplete directory에 있는 파일을 complete directory로 이동
        os.makedirs(path_join(self.out_dir_path, platform_name, channel_id), exist_ok=True)
        shutil.move(out_tmp_mp4_path, complete_mp4_path)


def clear_dir(
    base_dir_path: str, info: StdlSegmentsInfo, delete_platform: bool = False, delete_self: bool = False
):
    platform_dir_path = path_join(base_dir_path, info.platform_name)
    channel_dir_path = path_join(platform_dir_path, info.channel_id)
    if len(os.listdir(channel_dir_path)) == 0:
        os.rmdir(channel_dir_path)
    if delete_platform:
        if len(os.listdir(platform_dir_path)) == 0:
            os.rmdir(platform_dir_path)
        if delete_self:
            if len(os.listdir(base_dir_path)) == 0:
                os.rmdir(base_dir_path)


def _get_sorted_segment_paths(segments_path: str) -> list[str]:
    paths = []
    for filename in os.listdir(segments_path):
        if filename.endswith(".ts"):
            paths.append(path_join(segments_path, filename))
    return sorted(paths, key=lambda x: int(Path(x).stem))


def _remux_video(src_path: str, out_path: str):
    if shutil.which("ffmpeg") is None:
        raise FileNotFoundError("ffmpeg not found")
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _get_success_result(message: str) -> StdlDoneTaskResult:
    return {
        "status": StdlDoneTaskStatus.SUCCESS.value,
        "message": message,
    }
