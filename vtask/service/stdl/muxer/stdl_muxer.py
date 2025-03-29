import os
import shutil
import subprocess
from enum import Enum
from pathlib import Path
from typing import TypedDict

import yaml
from pyutils import log, path_join

from .stdl_helper import StdlHelper
from ..schema.stdl_constrants import (
    STDL_INCOMPLETE_DIR_NAME,
    STDL_COMPLETE_DIR_NAME,
    STDL_ARCHIVE_DIR_NAME,
)
from ...loss import TimeLossInspector, TimeFrameLossConfig
from ....common.fs import FsType


class StdlDoneTaskStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class StdlDoneTaskResult(TypedDict):
    status: str
    message: str


class StdlMuxer:
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
        self.loss_inspector = TimeLossInspector(TimeFrameLossConfig())

    def clear(self, uid: str, video_name: str) -> StdlDoneTaskResult:
        self.helper.clear(uid=uid, video_name=video_name)
        return _get_success_result("Clear success")

    def mux(self, uid: str, video_name: str) -> StdlDoneTaskResult:
        self.helper.move(uid=uid, video_name=video_name)

        # Convert video
        chunks_path = path_join(self.incomplete_dir_path, uid, video_name)
        if not Path(chunks_path).exists():
            return _get_failure_result("No video segments")

        chunk_paths = _get_sorted_chunk_paths(chunks_path=chunks_path)
        if len(chunk_paths) == 0:
            return _get_failure_result("No video segments")

        # Merge chunks
        os.makedirs(path_join(self.tmp_path, uid), exist_ok=True)
        merged_tmp_ts_path = path_join(self.tmp_path, uid, f"{video_name}.ts")
        with open(merged_tmp_ts_path, "wb") as outfile:
            for file_path in chunk_paths:
                with open(file_path, "rb") as infile:
                    outfile.write(infile.read())

        # Mux video
        tmp_mp4_path = path_join(self.tmp_path, uid, f"{video_name}.mp4")
        _mux_video(merged_tmp_ts_path, tmp_mp4_path)
        os.remove(merged_tmp_ts_path)

        # Inspect video
        tmp_csv_path = path_join(self.tmp_path, uid, f"{video_name}.csv")
        loss_result = self.loss_inspector.inspect(tmp_mp4_path, tmp_csv_path)
        os.remove(tmp_csv_path)

        # Move mp4 file
        self.move_mp4(tmp_mp4_path=tmp_mp4_path, uid=uid, video_name=video_name)

        # Write loss result file
        with open(path_join(self.complete_dir_path, uid, f"{video_name}.yaml"), "w") as file:
            file.write(yaml.dump(loss_result.model_dump(by_alias=True), allow_unicode=True))

        # Organize files
        if self.is_archive and self.helper.fs_type is not FsType.LOCAL:
            os.makedirs(path_join(self.archive_dir_path, uid), exist_ok=True)
            shutil.move(chunks_path, path_join(self.archive_dir_path, uid, video_name))
        else:
            shutil.rmtree(chunks_path)

        self.__clear_incomplete_dir(uid)
        _clear_tmp_dir(self.tmp_path, uid)

        log.info(f"Convert file: {uid}/{video_name}")
        return _get_success_result(f"Convert success: {uid}/{video_name}")

    def move_mp4(self, tmp_mp4_path: str, uid: str, video_name: str):
        incomplete_mp4_path = path_join(self.incomplete_dir_path, uid, f"{video_name}.mp4")
        complete_mp4_path = path_join(self.complete_dir_path, uid, f"{video_name}.mp4")

        # write 도중인 파일이 complete directory에 들어가면 안되기 때문에 먼저 incomplete directory로 이동
        os.makedirs(path_join(self.incomplete_dir_path, uid), exist_ok=True)
        shutil.move(tmp_mp4_path, incomplete_mp4_path)

        # incomplete directory에 있는 파일을 complete directory로 이동
        os.makedirs(path_join(self.complete_dir_path, uid), exist_ok=True)
        shutil.move(incomplete_mp4_path, complete_mp4_path)

    def __clear_incomplete_dir(self, uid: str):
        incomplete_uid_dir_path = path_join(self.incomplete_dir_path, uid)
        if len(os.listdir(incomplete_uid_dir_path)) == 0:
            os.rmdir(incomplete_uid_dir_path)


def _get_sorted_chunk_paths(chunks_path: str) -> list[str]:
    paths = []
    for filename in os.listdir(chunks_path):
        if filename.endswith(".ts"):
            paths.append(path_join(chunks_path, filename))
    return sorted(paths, key=lambda x: int(x.split("/")[-1].split(".")[0]))


def _clear_tmp_dir(tmp_dir_path: str, uid: str):
    tmp_uid_dir_path = path_join(tmp_dir_path, uid)
    if len(os.listdir(tmp_uid_dir_path)) == 0:
        os.rmdir(tmp_uid_dir_path)


def _mux_video(src_path: str, out_path: str):
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
