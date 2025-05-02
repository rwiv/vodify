import os
import time

from pydantic import BaseModel
from pyutils import path_join, filename, log

from ....utils import S3Client


class ArchiveTarget(BaseModel):
    platform: str
    uid: str
    video_name: str


class StdlArchiver:
    def __init__(self, s3_client: S3Client, out_dir_path: str):
        self.s3_client = s3_client
        self.out_dir_path = out_dir_path

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
