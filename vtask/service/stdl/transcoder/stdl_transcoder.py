import os
import shutil
import subprocess
import tarfile
import time
from enum import Enum
from pathlib import Path
from typing import TypedDict

import yaml
from pydantic import BaseModel, Field
from pyutils import log, path_join

from .segment_accessor.stdl_segment_accessor import StdlSegmentAccessor
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


TAR_SIZE_MB = 18
SEG_SIZE_MB = 2


class StdlTranscoder:
    def __init__(
        self,
        accessor: StdlSegmentAccessor,
        notifier: Notifier,
        out_dir_path: str,
        tmp_path: str,
        is_archive: bool,
        video_size_limit_gb: int,
    ):
        self.__accessor = accessor
        self.__notifier = notifier
        self.__tmp_path = tmp_path
        self.__out_tmp_dir_path = path_join(out_dir_path, "_tmp")
        self.__out_dir_path = out_dir_path
        self.__is_archive = is_archive
        self.__video_size_limit_gb = video_size_limit_gb
        self.__loss_inspector = TimeLossInspector(keyframe_only=False)

    def clear(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        self.__accessor.clear_by_info(info)
        return _get_success_result("Clear success")

    def transcode(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        transcode_start = time.time()
        platform = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name
        attr_str = f"platform={platform}, channel_id={channel_id}, video_name={video_name}"

        src_paths = self.__accessor.get_paths(info)

        too_large, video_size_gb = self.__check_video_size_by_cnt2(src_paths)
        info.video_size_gb = video_size_gb
        if too_large:
            message = f"Video size is too large: {attr_str}, size={video_size_gb}GB"
            self.__notifier.notify(message)
            log.error(message)
            raise ValueError(message)

        log.info("Start Transcoding", info.to_dict())

        # Copy segments from remote storage
        base_dir_path, tars_dir_path = self.__copy_direct(info, src_paths)

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
        is_mismatched = False
        seg_map: dict[int, str] = {}
        for seg_path in extract_seg_paths:
            seg_num = int(Path(seg_path).stem)
            if seg_num in seg_map:
                prev_size = Path(seg_map[seg_num]).stat().st_size
                cur_size = Path(seg_path).stat().st_size
                if prev_size != cur_size:
                    attr = info.to_dict(
                        {"path1": seg_map[seg_num], "path2": seg_path, "size1": prev_size, "size2": cur_size}
                    )
                    log.error(f"File size mismatch", attr)
                    is_mismatched = True
            else:
                seg_map[seg_num] = seg_path

        if is_mismatched:
            self.__notifier.notify(f"File size mismatch: {attr_str}")

        seg_dir_path = path_join(base_dir_path, "segments")
        os.makedirs(seg_dir_path, exist_ok=True)
        for _, seg_path in seg_map.items():
            shutil.move(seg_path, path_join(seg_dir_path, Path(seg_path).name))

        shutil.rmtree(extract_dir_path)

        # Check for missing segments
        segment_paths = _get_sorted_segment_paths(segments_path=seg_dir_path)
        if len(segment_paths) == 0:
            raise ValueError(f"Source path {seg_dir_path} is empty.")

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
        with open(merged_tmp_ts_path, "wb") as merged_file:
            for seg_file_path in segment_paths:
                with open(seg_file_path, "rb") as seg_file:
                    merged_file.write(seg_file.read())
                os.remove(seg_file_path)
        os.rmdir(seg_dir_path)

        # Remux video
        tmp_mp4_path = path_join(base_dir_path, f"{video_name}.mp4")
        _remux_video(merged_tmp_ts_path, tmp_mp4_path, info)
        os.remove(merged_tmp_ts_path)

        # Move mp4 file
        self.move_mp4(tmp_mp4_path=tmp_mp4_path, info=info)
        complete_loss_path = path_join(self.__out_dir_path, platform, channel_id, f"{video_name}.yaml")
        with open(complete_loss_path, "w") as file:
            file.write(yaml.dump(inspect_result.to_out_dict(), allow_unicode=True))

        shutil.rmtree(base_dir_path)
        clear_dir(self.__tmp_path, info, delete_platform=True, delete_self=False)
        clear_dir(self.__out_tmp_dir_path, info, delete_platform=True, delete_self=False)

        if not self.__is_archive and not is_mismatched:
            self.__accessor.clear_by_paths(src_paths)

        result_msg = "Complete Transcoding"
        log.info(result_msg, info.to_dict({"elapsed_time_sec": round(time.time() - transcode_start, 2)}))
        return _get_success_result(f"{result_msg}: {platform}/{channel_id}/{video_name}")

    def __copy_direct(self, info: StdlSegmentsInfo, src_paths: list[str]) -> tuple[str, str]:
        start = time.time()
        base_dir_path = path_join(self.__tmp_path, info.platform_name, info.channel_id, info.video_name)
        tars_dir_path = path_join(base_dir_path, "tars")
        os.makedirs(tars_dir_path, exist_ok=True)
        self.__accessor.copy(src_paths, tars_dir_path)
        log.debug("Download segments", info.to_dict({"elapsed_time_sec": round(time.time() - start, 2)}))
        return base_dir_path, tars_dir_path

    def __copy_pass_by_out_dir(self, info: StdlSegmentsInfo, src_paths: list[str]) -> tuple[str, str]:
        dl_start = time.time()
        out_tars_dir_path = path_join(
            self.__out_tmp_dir_path, info.platform_name, info.channel_id, info.video_name
        )
        os.makedirs(out_tars_dir_path, exist_ok=True)
        self.__accessor.copy(src_paths, out_tars_dir_path)
        log.debug(
            "Download segments to out directory",
            info.to_dict({"elapsed_time_sec": round(time.time() - dl_start, 2)}),
        )

        move_start = time.time()
        base_dir_path = path_join(self.__tmp_path, info.platform_name, info.channel_id, info.video_name)
        tars_dir_path = path_join(base_dir_path, "tars")
        os.makedirs(tars_dir_path, exist_ok=True)
        for out_tar_path in os.listdir(out_tars_dir_path):
            out_tar_full_path = path_join(out_tars_dir_path, out_tar_path)
            if os.path.isfile(out_tar_full_path):
                shutil.move(out_tar_full_path, path_join(tars_dir_path, out_tar_path))
        log.debug(
            "Move segments to tmp directory",
            info.to_dict({"elapsed_time_sec": round(time.time() - move_start, 2)}),
        )
        clear_dir(self.__out_tmp_dir_path, info, delete_platform=True, delete_self=False)
        return base_dir_path, tars_dir_path

    def move_mp4(self, tmp_mp4_path: str, info: StdlSegmentsInfo):
        platform_name = info.platform_name
        channel_id = info.channel_id
        video_name = info.video_name

        # Move mp4 file to complete directory
        out_tmp_channel_dir_path = path_join(self.__out_tmp_dir_path, platform_name, channel_id)
        os.makedirs(out_tmp_channel_dir_path, exist_ok=True)
        out_tmp_mp4_path = path_join(out_tmp_channel_dir_path, f"{video_name}.mp4")
        complete_mp4_path = path_join(self.__out_dir_path, platform_name, channel_id, f"{video_name}.mp4")

        # write 도중인 파일이 complete directory에 들어가면 안되기 때문에 먼저 incomplete directory로 이동
        start = time.time()
        shutil.move(tmp_mp4_path, out_tmp_mp4_path)

        # incomplete directory에 있는 파일을 complete directory로 이동
        os.makedirs(path_join(self.__out_dir_path, platform_name, channel_id), exist_ok=True)
        shutil.move(out_tmp_mp4_path, complete_mp4_path)
        log.debug("Move mp4", info.to_dict({"elapsed_time_sec": round(time.time() - start, 2)}))

    def __check_video_size_by_cnt1(self, paths: list[str]) -> tuple[bool, float]:
        tars_size_sum_mb = len(paths) * TAR_SIZE_MB
        tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
        tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
        is_too_large = tars_size_sum_b > (self.__video_size_limit_gb * 1024 * 1024 * 1024)
        return is_too_large, tars_size_sum_gb

    def __check_video_size_by_cnt2(self, paths: list[str]) -> tuple[bool, float]:
        stems = [Path(path).stem for path in paths]
        tars_size_sum_mb = 0
        for stem in stems:
            if len(stem.split("_")) == 3:
                tars_size_sum_mb += TAR_SIZE_MB
            elif len(stem.split("_")) == 2:
                tars_size_sum_mb += SEG_SIZE_MB
            else:
                raise ValueError(f"Invalid tar name: {stem}")
        tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
        tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
        is_too_large = tars_size_sum_b > (self.__video_size_limit_gb * 1024 * 1024 * 1024)
        return is_too_large, tars_size_sum_gb


def clear_dir(
    base_dir_path: str, info: StdlSegmentsInfo, delete_platform: bool = False, delete_self: bool = False
):
    platform_dir_path = path_join(base_dir_path, info.platform_name)
    channel_dir_path = path_join(platform_dir_path, info.channel_id)
    video_dir_path = path_join(channel_dir_path, info.video_name)
    if Path(video_dir_path).exists() and len(os.listdir(video_dir_path)) == 0:
        os.rmdir(video_dir_path)
    if Path(channel_dir_path).exists() and len(os.listdir(channel_dir_path)) == 0:
        os.rmdir(channel_dir_path)
    if delete_platform:
        if Path(platform_dir_path).exists() and len(os.listdir(platform_dir_path)) == 0:
            os.rmdir(platform_dir_path)
        if delete_self:
            if Path(base_dir_path).exists() and len(os.listdir(base_dir_path)) == 0:
                os.rmdir(base_dir_path)


def _get_sorted_segment_paths(segments_path: str) -> list[str]:
    paths = []
    for filename in os.listdir(segments_path):
        if filename.endswith(".ts"):
            paths.append(path_join(segments_path, filename))
    return sorted(paths, key=lambda x: int(Path(x).stem))


def _remux_video(src_path: str, out_path: str, info: StdlSegmentsInfo):
    start = time.time()
    if shutil.which("ffmpeg") is None:
        raise FileNotFoundError("ffmpeg not found")
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.debug("Remux from ts to mp4", info.to_dict({"elapsed_time_sec": round(time.time() - start, 2)}))


def _get_success_result(message: str) -> StdlDoneTaskResult:
    return {
        "status": StdlDoneTaskStatus.SUCCESS.value,
        "message": message,
    }
