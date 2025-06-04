import asyncio
import shutil
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import TypedDict

import aiofiles
from aiofiles import os as aios
from pydantic import BaseModel, Field
from pyutils import log, path_join

from .segment_accessor.stdl_segment_accessor import StdlSegmentAccessor
from ..schema import StdlSegmentsInfo
from ...loss import TimeLossInspector
from ....common.notifier import Notifier
from ....utils import (
    cur_duration,
    write_yaml_file,
    ensure_dir,
    move_directory_not_recur,
    listdir_recur,
    move_file,
    rmtree,
    open_tar,
    stem,
)


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

TAR_DIR_NAME = "tars"
EXTRACTED_DIR_NAME = "extracted"
SEGMENTS_DIR_NAME = "segments"


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

    async def clear(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        await self.__accessor.clear_by_info(info)
        return _get_success_result("Clear success")

    async def transcode(self, info: StdlSegmentsInfo) -> StdlDoneTaskResult:
        # Initialize local variables
        transcoding_start = asyncio.get_event_loop().time()

        pf = info.platform_name
        ch_id = info.channel_id
        vid = info.video_name

        archive_source = False
        archive_tars = self.__is_archive
        if info.should_archive:
            archive_tars = True

        # Get source paths
        source_paths = await self.__accessor.get_paths(info)

        # Check video size
        await self.__check_video_size(info=info, source_paths=source_paths)

        # Start transcoding
        log.info("Start Transcoding", info.to_dict())
        base_dir_path = path_join(self.__tmp_path, pf, ch_id, vid)

        # Copy segments from remote storage
        tars_dir_path = await self.__copy_direct(
            info=info, base_dir_path=base_dir_path, source_paths=source_paths
        )
        await _validate_tar_files(tars_dir_path)

        # Extract tar files
        extracted_dir_path = await ensure_dir(path_join(base_dir_path, EXTRACTED_DIR_NAME))
        extracted_seg_paths = await _extract_tar_files(
            src_dir_path=tars_dir_path, out_dir_path=extracted_dir_path
        )

        # Get deduplicated segment paths, and Check mismatched segments
        dd_seg_paths, mismatch_seg_infos = await _get_deduplicated_seg_paths(extracted_seg_paths)
        if len(mismatch_seg_infos) > 0:
            head = "Segment size mismatch"
            log.error(head, info.to_dict())
            msg = f"{head}: platform={pf}, channel_id={ch_id}, video_name={vid}"
            await self.__notifier.notify_async(msg)
            archive_source = True
            archive_tars = True

        # Move segment files to `segments` directory
        seg_dir_path = await ensure_dir(path_join(base_dir_path, SEGMENTS_DIR_NAME))
        for seg_path in dd_seg_paths:
            await move_file(src=seg_path, dst=path_join(seg_dir_path, Path(seg_path).name))

        # Remove duplicated segment files
        await rmtree(extracted_dir_path)

        # Get sorted segment paths
        sorted_segment_paths = await _get_sorted_segment_paths(segments_path=seg_dir_path)
        if len(sorted_segment_paths) == 0:
            raise ValueError(f"Source path {seg_dir_path} is empty.")

        # Check for missing segments
        inspect_result = _check_missing_segments(segment_paths=sorted_segment_paths)
        if len(inspect_result.missing_segments) > 0 and info.conditionally_archive:
            archive_tars = True

        # Remove tar files
        out_tmp_archive_dir_path = path_join(self.__out_tmp_dir_path, pf, ch_id, vid, TAR_DIR_NAME)
        if archive_tars:
            duration = await move_directory_not_recur(tars_dir_path, out_tmp_archive_dir_path)
            attr = info.to_dict({"duration": round(duration, 2)})
            log.debug("Move archive tar files to out tmp directory", attr)
        else:
            await rmtree(tars_dir_path)

        # Merge segments
        merged_tmp_ts_path = path_join(base_dir_path, f"{vid}.ts")
        async with aiofiles.open(merged_tmp_ts_path, "wb") as merged_file:
            for seg_file_path in sorted_segment_paths:
                async with aiofiles.open(seg_file_path, "rb") as seg_file:
                    await merged_file.write(await seg_file.read())
                await aios.remove(seg_file_path)
        await aios.rmdir(seg_dir_path)

        # Remux video from ts to mp4
        tmp_mp4_path = path_join(base_dir_path, f"{vid}.mp4")
        await asyncio.to_thread(_remux_video, merged_tmp_ts_path, tmp_mp4_path, info)
        await aios.remove(merged_tmp_ts_path)

        # Move result files
        await self.move_mp4(info=info, tmp_mp4_path=tmp_mp4_path)
        comp_chan_dir_path = path_join(self.__out_dir_path, pf, ch_id)
        await write_yaml_file(inspect_result.to_out_dict(), path_join(comp_chan_dir_path, f"{vid}.yaml"))
        if len(mismatch_seg_infos) > 0:
            await write_yaml_file(mismatch_seg_infos, path_join(comp_chan_dir_path, f"{vid}_missmatch.yaml"))
        if Path(out_tmp_archive_dir_path).exists():
            comp_vid_dir_path = await ensure_dir(path_join(self.__out_dir_path, pf, ch_id, vid))
            await move_directory_not_recur(out_tmp_archive_dir_path, comp_vid_dir_path)

        # Clear temporary directories
        await rmtree(base_dir_path)
        await clear_dir(self.__tmp_path, info, delete_platform=True, delete_self=False)
        await clear_dir(out_tmp_archive_dir_path, info, delete_platform=True, delete_self=True)
        await clear_dir(self.__out_tmp_dir_path, info, delete_platform=True, delete_self=False)

        # Delete source tar files if not archive
        if not archive_source:
            await self.__accessor.clear_by_paths(source_paths)

        # Close
        result_msg = "Complete Transcoding"
        log.info(result_msg, info.to_dict({"duration": round(cur_duration(transcoding_start), 2)}))
        return _get_success_result(f"{result_msg}: platform={pf}, channel_id={ch_id}, video_name={vid}")

    async def __copy_direct(self, info: StdlSegmentsInfo, base_dir_path: str, source_paths: list[str]) -> str:
        start = asyncio.get_event_loop().time()
        tars_dir_path = await ensure_dir(path_join(base_dir_path, TAR_DIR_NAME))
        await self.__accessor.copy(source_paths, tars_dir_path)
        log.debug("Download segments", info.to_dict({"duration": round(cur_duration(start), 2)}))
        return tars_dir_path

    async def __copy_pass_by_out_dir(
        self, info: StdlSegmentsInfo, base_dir_path: str, src_paths: list[str]
    ) -> str:
        dl_start = asyncio.get_event_loop().time()
        out_tmp_tars_dir_path = await ensure_dir(
            path_join(self.__out_tmp_dir_path, info.platform_name, info.channel_id, info.video_name)
        )
        await self.__accessor.copy(src_paths, out_tmp_tars_dir_path)
        attr = info.to_dict({"duration": round(cur_duration(dl_start), 2)})
        log.debug("Download segments to out directory", attr)

        move_start = asyncio.get_event_loop().time()
        tars_dir_path = await ensure_dir(path_join(base_dir_path, TAR_DIR_NAME))
        for out_tar_path in await aios.listdir(out_tmp_tars_dir_path):
            out_tar_full_path = path_join(out_tmp_tars_dir_path, out_tar_path)
            if await aios.path.isfile(out_tar_full_path):
                dst = path_join(tars_dir_path, out_tar_path)
                await move_file(src=out_tar_full_path, dst=dst)
        attr = info.to_dict({"duration": round(cur_duration(move_start), 2)})
        log.debug("Move segments to tmp directory", attr)
        return tars_dir_path

    async def move_mp4(self, info: StdlSegmentsInfo, tmp_mp4_path: str):
        pf = info.platform_name
        ch_id = info.channel_id
        vid = info.video_name

        # Move mp4 file to complete directory

        start = asyncio.get_event_loop().time()
        # write 도중인 파일이 complete directory에 들어가면 안되기 때문에 먼저 incomplete directory로 이동
        out_tmp_vid_dir_path = await ensure_dir(path_join(self.__out_tmp_dir_path, pf, ch_id, vid))
        out_tmp_mp4_path = path_join(out_tmp_vid_dir_path, f"{vid}.mp4")
        await move_file(src=tmp_mp4_path, dst=out_tmp_mp4_path)

        # incomplete directory에 있는 파일을 complete directory로 이동
        complete_chan_dir_path = await ensure_dir(path_join(self.__out_dir_path, pf, ch_id))
        complete_mp4_path = path_join(complete_chan_dir_path, f"{vid}.mp4")
        await move_file(src=out_tmp_mp4_path, dst=complete_mp4_path)
        log.debug("Move mp4", info.to_dict({"duration": round(cur_duration(start), 2)}))

    async def __check_video_size(self, info: StdlSegmentsInfo, source_paths: list[str]):
        too_large, video_size_gb = _get_video_size_by_cnt(self.__video_size_limit_gb, source_paths)
        info.video_size_gb = video_size_gb
        if too_large:
            head = "Video size is too large"
            log.error(head, info.to_dict())

            message = f"{head}: platform={info.platform_name}, channel_id={info.channel_id}, video_name={info.video_name}, size={video_size_gb}GB"
            await self.__notifier.notify_async(message)
            raise ValueError(message)


def _get_video_size_by_cnt(size_limit_gb: int, paths: list[str]) -> tuple[bool, float]:
    tars_size_sum_mb = len(paths) * TAR_SIZE_MB
    tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
    tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
    is_too_large = tars_size_sum_b > (size_limit_gb * 1024 * 1024 * 1024)
    return is_too_large, tars_size_sum_gb


def _get_video_size_by_name(size_limit_gb: int, paths: list[str]) -> tuple[bool, float]:
    tars_size_sum_mb = 0
    for path in paths:
        stem_name = stem(path)
        if len(stem_name.split("_")) == 3:
            tars_size_sum_mb += TAR_SIZE_MB
        elif len(stem_name.split("_")) == 2:
            tars_size_sum_mb += SEG_SIZE_MB
        else:
            raise ValueError(f"Invalid tar name: {stem_name}")
    tars_size_sum_b = tars_size_sum_mb * 1024 * 1024
    tars_size_sum_gb = round(tars_size_sum_b / 1024 / 1024 / 1024, 2)
    is_too_large = tars_size_sum_b > (size_limit_gb * 1024 * 1024 * 1024)
    return is_too_large, tars_size_sum_gb


async def _validate_tar_files(tars_dir_path: str):
    if not Path(tars_dir_path).exists():
        raise ValueError(f"Source path {tars_dir_path} does not exist.")
    tar_names = await aios.listdir(tars_dir_path)
    if len(tar_names) == 0:
        raise ValueError(f"Source path {tars_dir_path} is empty.")
    for tar_name in tar_names:
        if not tar_name.endswith(".tar"):
            raise ValueError(f"Invalid file ext: {path_join(tars_dir_path, tar_name)}")


async def _extract_tar_files(src_dir_path: str, out_dir_path: str) -> list[str]:
    for tar_filename in await aios.listdir(src_dir_path):
        extracted_dir_path = await ensure_dir(path_join(out_dir_path, stem(tar_filename)))
        await open_tar(path_join(src_dir_path, tar_filename), extracted_dir_path)

    extracted_seg_paths = []
    for file_path in await listdir_recur(out_dir_path):
        if not file_path.endswith(".ts"):
            raise ValueError(f"Invalid file ext: {file_path}")
        extracted_seg_paths.append(file_path)

    return extracted_seg_paths


async def _get_deduplicated_seg_paths(extract_seg_paths: list[str]):
    seg_map: dict[int, str] = {}
    mismatch_seg_infos = []
    for seg_path in extract_seg_paths:
        seg_num = int(stem(seg_path))
        if seg_num not in seg_map:
            seg_map[seg_num] = seg_path
        else:
            prev_size = (await aios.stat(seg_map[seg_num])).st_size
            cur_size = (await aios.stat(seg_path)).st_size
            if prev_size != cur_size:
                mismatch_seg_infos.append({"path1": seg_map[seg_num], "path2": seg_path})
    return list(seg_map.values()), mismatch_seg_infos


def _check_missing_segments(segment_paths: list[str]) -> InspectResult:
    seg_nums = [int(stem(path)) for path in segment_paths]
    if -1 in seg_nums and seg_nums[0] != -1:  # check if segment sorted order is valid
        raise ValueError(f"Invalid segment sorted order: {segment_paths}")

    seg_nums = set([int(stem(path)) for path in segment_paths])
    missing_seg_nums: list[int] = []
    start_num = int(stem(segment_paths[0]))
    if start_num in (0, -1) and len(segment_paths) > 0:
        start_num = int(stem(segment_paths[1]))
    end_num = int(stem(segment_paths[-1]))
    for cur_num in range(start_num, end_num + 1):
        if cur_num not in seg_nums:
            missing_seg_nums.append(cur_num)

    return InspectResult(
        missing_segments=missing_seg_nums,
    )


async def clear_dir(
    base_dir_path: str,
    info: StdlSegmentsInfo,
    delete_platform: bool = False,
    delete_self: bool = False,
):
    platform_dir_path = path_join(base_dir_path, info.platform_name)
    channel_dir_path = path_join(platform_dir_path, info.channel_id)
    video_dir_path = path_join(channel_dir_path, info.video_name)
    if Path(video_dir_path).exists() and len(await aios.listdir(video_dir_path)) == 0:
        await aios.rmdir(video_dir_path)
    if Path(channel_dir_path).exists() and len(await aios.listdir(channel_dir_path)) == 0:
        await aios.rmdir(channel_dir_path)
    if delete_platform:
        if Path(platform_dir_path).exists() and len(await aios.listdir(platform_dir_path)) == 0:
            await aios.rmdir(platform_dir_path)
        if delete_self:
            if Path(base_dir_path).exists() and len(await aios.listdir(base_dir_path)) == 0:
                await aios.rmdir(base_dir_path)


async def _get_sorted_segment_paths(segments_path: str) -> list[str]:
    paths = []
    for filename in await aios.listdir(segments_path):
        if filename.endswith(".ts"):
            paths.append(path_join(segments_path, filename))
    return sorted(paths, key=lambda x: int(stem(x)))


def _remux_video(src_path: str, out_path: str, info: StdlSegmentsInfo):
    start = time.time()
    if shutil.which("ffmpeg") is None:
        raise FileNotFoundError("ffmpeg not found")
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.debug("Remux from ts to mp4", info.to_dict({"duration": round(time.time() - start, 2)}))


def _get_success_result(message: str) -> StdlDoneTaskResult:
    return {
        "status": StdlDoneTaskStatus.SUCCESS.value,
        "message": message,
    }
