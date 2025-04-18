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

from .helper.stdl_helper import StdlHelper
from ..schema import (
    StdlSegmentsInfo,
    STDL_INCOMPLETE_DIR_NAME,
    STDL_COMPLETE_DIR_NAME,
    STDL_ARCHIVE_DIR_NAME,
)
from ...loss import TimeLossInspector
from ....common.fs import FsType


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
        helper: StdlHelper,
        base_path: str,
        tmp_path: str,
        is_archive: bool,
    ):
        self.helper = helper
        self.tmp_path = tmp_path
        self.incomplete_dir_path = path_join(base_path, STDL_INCOMPLETE_DIR_NAME)
        self.complete_dir_path = path_join(base_path, STDL_COMPLETE_DIR_NAME)
        self.archive_dir_path = path_join(base_path, STDL_ARCHIVE_DIR_NAME)
        self.is_archive = is_archive
        self.loss_inspector = TimeLossInspector(keyframe_only=False)

    def clear(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        self.helper.clear(info)
        return _get_success_result("Clear success")

    def transcode(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        # Download segments from remote storage
        self.helper.move(info)

        # Preprocess tar files
        base_dir_path = path_join(self.incomplete_dir_path, platform_name, channel_id, video_name)
        if not Path(base_dir_path).exists():
            return _get_failure_result("No video segments")

        tar_names = os.listdir(base_dir_path)
        if len(tar_names) == 0:
            return _get_failure_result("No video segments")

        tars_dir_path = path_join(self.incomplete_dir_path, platform_name, channel_id, video_name, "tars")
        os.makedirs(tars_dir_path, exist_ok=True)

        for file in tar_names:
            if file.endswith(".tar"):
                shutil.move(path_join(base_dir_path, file), path_join(tars_dir_path, file))
            else:
                raise ValueError(f"Invalid file type: {file}")

        # Extract tar files
        extract_dir_path = path_join(
            self.incomplete_dir_path, platform_name, channel_id, video_name, "extract"
        )
        os.makedirs(extract_dir_path, exist_ok=True)

        for tar_filename in os.listdir(tars_dir_path):
            src_path = path_join(tars_dir_path, tar_filename)
            out_path = path_join(extract_dir_path, Path(tar_filename).stem)
            os.makedirs(out_path, exist_ok=True)
            with tarfile.open(src_path, "r:*") as tar:
                tar.extractall(path=out_path)

        extract_seg_paths = []
        for root, _, files in os.walk(extract_dir_path):
            for file in files:
                if not file.endswith(".ts"):
                    raise ValueError(f"Invalid file type: {file}")
                extract_seg_paths.append(path_join(root, file))

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
            return _get_failure_result("No video segments")

        seg_nums = set([int(Path(path).stem) for path in segment_paths])
        missing_seg_nums: list[int] = []
        start_num = int(Path(segment_paths[0]).stem)
        if start_num == 0:
            start_num = int(Path(segment_paths[1]).stem)
        end_num = int(Path(segment_paths[-1]).stem)
        for cur_num in range(start_num, end_num + 1):
            if cur_num not in seg_nums:
                missing_seg_nums.append(cur_num)

        inspect_result = InspectResult(
            missing_segments=missing_seg_nums,
        )

        # Merge segments
        os.makedirs(path_join(self.tmp_path, platform_name, channel_id), exist_ok=True)
        merged_tmp_ts_path = path_join(self.tmp_path, platform_name, channel_id, f"{video_name}.ts")
        with open(merged_tmp_ts_path, "wb") as outfile:
            for file_path in segment_paths:
                with open(file_path, "rb") as infile:
                    outfile.write(infile.read())

        # Remux video
        tmp_mp4_path = path_join(self.tmp_path, platform_name, channel_id, f"{video_name}.mp4")
        _remux_video(merged_tmp_ts_path, tmp_mp4_path)
        os.remove(merged_tmp_ts_path)

        # Move mp4 file
        self.move_mp4(tmp_mp4_path=tmp_mp4_path, info=info)
        complete_loss_path = path_join(
            self.complete_dir_path, platform_name, channel_id, f"{video_name}.yaml"
        )
        with open(complete_loss_path, "w") as file:
            file.write(yaml.dump(inspect_result.to_out_dict(), allow_unicode=True))

        # Organize files
        if self.is_archive and self.helper.fs_type is not FsType.LOCAL:
            archive_dir_path = path_join(self.archive_dir_path, platform_name, channel_id)
            os.makedirs(archive_dir_path, exist_ok=True)
            shutil.move(tars_dir_path, path_join(archive_dir_path, video_name))
        else:
            shutil.rmtree(tars_dir_path)

        shutil.rmtree(base_dir_path)
        _clear_dir(self.incomplete_dir_path, platform_name, channel_id)
        _clear_dir(self.tmp_path, platform_name, channel_id)

        log.info(f"Convert file: {channel_id}/{video_name}")
        return _get_success_result(f"Convert success: {channel_id}/{video_name}")

    def move_mp4(self, tmp_mp4_path: str, info: StdlSegmentsInfo):
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        incomplete_mp4_path = path_join(
            self.incomplete_dir_path, platform_name, channel_id, f"{video_name}.mp4"
        )
        complete_mp4_path = path_join(self.complete_dir_path, platform_name, channel_id, f"{video_name}.mp4")

        # write 도중인 파일이 complete directory에 들어가면 안되기 때문에 먼저 incomplete directory로 이동
        os.makedirs(path_join(self.incomplete_dir_path, platform_name, channel_id), exist_ok=True)
        shutil.move(tmp_mp4_path, incomplete_mp4_path)

        # incomplete directory에 있는 파일을 complete directory로 이동
        os.makedirs(path_join(self.complete_dir_path, platform_name, channel_id), exist_ok=True)
        shutil.move(incomplete_mp4_path, complete_mp4_path)


def _get_sorted_segment_paths(segments_path: str) -> list[str]:
    paths = []
    for filename in os.listdir(segments_path):
        if filename.endswith(".ts"):
            paths.append(path_join(segments_path, filename))
    return sorted(paths, key=lambda x: int(Path(x).stem))


def _clear_dir(tmp_dir_path: str, platform_name: str, channel_id: str):
    platform_dir_path = path_join(tmp_dir_path, platform_name)
    channel_dir_path = path_join(platform_dir_path, channel_id)
    if len(os.listdir(channel_dir_path)) == 0:
        os.rmdir(channel_dir_path)
    if len(os.listdir(platform_dir_path)) == 0:
        os.rmdir(platform_dir_path)


def _remux_video(src_path: str, out_path: str):
    if shutil.which("ffmpeg") is None:
        raise FileNotFoundError("ffmpeg not found")
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _get_failure_result(message: str) -> StdlDoneTaskResult:
    return {
        "status": StdlDoneTaskStatus.FAILURE.value,
        "message": message,
    }


def _get_success_result(message: str) -> StdlDoneTaskResult:
    return {
        "status": StdlDoneTaskStatus.SUCCESS.value,
        "message": message,
    }
