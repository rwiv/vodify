import os
import time
from pathlib import Path

from pydantic import BaseModel
from pyutils import path_join, filename, log

from ..schema import StdlSegmentsInfo, STDL_INCOMPLETE_DIR_NAME
from ..transcoder import StdlTranscoder, StdlLocalHelper
from ....utils import S3Client


class ArchiveTarget(BaseModel):
    platform: str
    uid: str
    video_name: str


class StdlArchiver:
    def __init__(self, s3_client: S3Client, out_dir_path: str, tmp_dir_path: str):
        self.s3_client = s3_client
        self.out_dir_path = out_dir_path
        self.incomplete_dir_path = path_join(out_dir_path, STDL_INCOMPLETE_DIR_NAME)
        self.trans = StdlTranscoder(
            helper=StdlLocalHelper(incomplete_dir_path=self.incomplete_dir_path),
            base_path=out_dir_path,
            tmp_path=tmp_dir_path,
            is_archive=False,
        )

    def transcode(self):
        for platform_name in os.listdir(self.incomplete_dir_path):
            platform_dir_path = checked_dir_path(self.incomplete_dir_path, platform_name)
            for channel_id in os.listdir(platform_dir_path):
                channel_dir_path = checked_dir_path(platform_dir_path, channel_id)
                for video_name in os.listdir(channel_dir_path):
                    checked_dir_path(channel_dir_path, video_name)
                    result = self.trans.transcode(
                        StdlSegmentsInfo(
                            platform_name=platform_name,
                            channel_id=channel_id,
                            video_name=video_name,
                        )
                    )
                    log.info(
                        "End transcode",
                        {
                            "status": result["status"],
                            "message": result["message"],
                        },
                    )

    def archive(self, targets: list[ArchiveTarget]):
        start_time = time.time()

        for target in targets:
            cnt = 0
            keys: list[str] = []
            prefix = path_join("incomplete", target.platform, target.uid, target.video_name)
            for obj in self.s3_client.list_all_objects(prefix=prefix):
                keys.append(obj.key)

            out_dir_path = path_join(self.out_dir_path, target.platform, target.uid, target.video_name)
            os.makedirs(out_dir_path, exist_ok=True)
            for key in keys:
                self.s3_client.write_file(
                    key=key,
                    file_path=path_join(out_dir_path, filename(key)),
                    network_io_delay_ms=0,
                    network_buf_size=65536,
                )
                if cnt % 10 == 0:
                    log.info(f"Archived {cnt} files")
                cnt += 1
            log.info(f"Archived {len(keys)} files")

        log.info(f"Elapsed time: {time.time() - start_time:.3f} sec")


def checked_dir_path(base_dir_path: str, new_path: str) -> str:
    return_dir_path = path_join(base_dir_path, new_path)
    if not Path(return_dir_path).is_dir():
        raise ValueError(f"Invalid directory: {return_dir_path}")
    return return_dir_path
