import asyncio
import subprocess
from pathlib import Path

import aiofiles
from aiofiles import os as aios
from pyutils import log, path_join, error_dict, cur_duration, run_process

from .recnode_data import RecnodeDoneTaskResult, RecnodeDoneTaskStatus
from .utils_preprocess_segments import _get_deduplicated_seg_paths, _get_sorted_segment_paths, _check_missing_segments
from .utils_preprocess_tars import _validate_tar_files, _extract_tar_files
from .utils_estimate_size import _get_video_size_by_cnt
from .utils_postprocess import clear_dir
from ..accessor.segment_accessor import SegmentAccessor
from ..schema.recnode_types import RecnodeSegmentsInfo
from ...external.notifier import Notifier
from ...utils import write_yaml_file, ensure_dir, move_directory_not_recur, move_file, rmtree

TAR_DIR_NAME = "tars"
EXTRACTED_DIR_NAME = "extracted"
SEGMENTS_DIR_NAME = "segments"


class RecnodeTranscoder:
    def __init__(
        self,
        accessor: SegmentAccessor,
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

    async def clear(self, info: RecnodeSegmentsInfo) -> RecnodeDoneTaskResult:
        await self.__accessor.clear_by_info(info)
        return _get_success_result("Clear success")

    async def transcode(self, info: RecnodeSegmentsInfo) -> RecnodeDoneTaskResult:
        try:
            return await self.__transcode(info)
        except Exception as e:
            attr = info.to_dict()
            for k, v in error_dict(e).items():
                attr[k] = v
            log.error("Failed to Transcode", attr)
            await rmtree(path_join(self.__tmp_path, info.platform_name, info.channel_id, info.video_name))
            raise e

    async def __transcode(self, info: RecnodeSegmentsInfo) -> RecnodeDoneTaskResult:
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
        tars_dir_path = await self.__copy_direct(info=info, base_dir_path=base_dir_path, source_paths=source_paths)
        await _validate_tar_files(tars_dir_path)

        # Extract tar files
        extracted_dir_path = await ensure_dir(path_join(base_dir_path, EXTRACTED_DIR_NAME))
        extracted_seg_paths = await _extract_tar_files(src_dir_path=tars_dir_path, out_dir_path=extracted_dir_path)

        # Get deduplicated segment paths, and Check mismatched segments
        dd_seg_paths, mismatch_seg_infos = await _get_deduplicated_seg_paths(extracted_seg_paths)
        if len(mismatch_seg_infos) > 0:
            head = "Segment size mismatch"
            log.error(head, info.to_dict())
            msg = f"{head}: platform={pf}, channel_id={ch_id}, video_name={vid}"
            await self.__notifier.notify(msg)
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
        await _remux_video(merged_tmp_ts_path, tmp_mp4_path, info)
        await aios.remove(merged_tmp_ts_path)

        # Move result files
        await self.__move_mp4(info=info, tmp_mp4_path=tmp_mp4_path)
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

    async def __copy_direct(self, info: RecnodeSegmentsInfo, base_dir_path: str, source_paths: list[str]) -> str:
        start = asyncio.get_event_loop().time()
        tars_dir_path = await ensure_dir(path_join(base_dir_path, TAR_DIR_NAME))
        await self.__accessor.copy(source_paths, tars_dir_path)
        log.debug("Download segments", info.to_dict({"duration": round(cur_duration(start), 2)}))
        return tars_dir_path

    async def __copy_pass_by_out_dir(self, info: RecnodeSegmentsInfo, base_dir_path: str, src_paths: list[str]) -> str:
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

    async def __move_mp4(self, info: RecnodeSegmentsInfo, tmp_mp4_path: str):
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

    async def __check_video_size(self, info: RecnodeSegmentsInfo, source_paths: list[str]):
        too_large, video_size_gb = _get_video_size_by_cnt(self.__video_size_limit_gb, source_paths)
        info.video_size_gb = video_size_gb
        if too_large:
            head = "Video size is too large"
            log.error(head, info.to_dict())

            message = f"{head}: platform={info.platform_name}, channel_id={info.channel_id}, video_name={info.video_name}, size={video_size_gb}GB"
            await self.__notifier.notify(message)
            raise ValueError(message)


async def _remux_video(src_path: str, out_path: str, info: RecnodeSegmentsInfo):
    start = asyncio.get_event_loop().time()
    command = ["ffmpeg", "-i", src_path, "-c", "copy", out_path]
    await run_process(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.debug("Remux from ts to mp4", info.to_dict({"duration": round(cur_duration(start), 2)}))


def _get_success_result(message: str) -> RecnodeDoneTaskResult:
    return {
        "status": RecnodeDoneTaskStatus.SUCCESS.value,
        "message": message,
    }
