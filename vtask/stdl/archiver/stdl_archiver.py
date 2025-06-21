import asyncio

from aiofiles import os as aios
from pydantic import BaseModel
from pyutils import path_join, filename, log

from ..accessor.stdl_segment_accessor_local import StdlLocalSegmentAccessor
from ..accessor.stdl_segment_accessor_s3 import StdlS3SegmentAccessor
from ..schema.stdl_constrants import STDL_INCOMPLETE_DIR_NAME
from ..schema.stdl_types import StdlSegmentsInfo
from ..transcoder.stdl_transcoder import StdlTranscoder
from ...external.notifier import Notifier
from ...external.s3 import S3AsyncClient
from ...utils import cur_duration


class ArchiveTarget(BaseModel):
    platform: str
    uid: str
    video_name: str


class StdlArchiver:
    def __init__(
        self,
        s3_client: S3AsyncClient,
        tmp_dir_path: str,
        out_dir_path: str,
        is_archive: bool,
        video_size_limit_gb: int,
        notifier: Notifier,
    ):
        self.s3_client = s3_client
        self.notifier = notifier
        self.tmp_dir_path = tmp_dir_path
        self.out_dir_path = out_dir_path
        self.incomplete_dir_path = path_join(out_dir_path, STDL_INCOMPLETE_DIR_NAME)
        self.is_archive = is_archive
        self.video_size_limit_gb = video_size_limit_gb

    async def transcode_by_s3(self, targets: list[ArchiveTarget]):
        trans = StdlTranscoder(
            accessor=StdlS3SegmentAccessor(s3_client=self.s3_client),
            notifier=self.notifier,
            out_dir_path=path_join(self.out_dir_path, "complete"),
            tmp_path=self.tmp_dir_path,
            is_archive=self.is_archive,
            video_size_limit_gb=self.video_size_limit_gb,
        )

        for target in targets:
            start_time = asyncio.get_event_loop().time()
            info = StdlSegmentsInfo(
                platform_name=target.platform,
                channel_id=target.uid,
                video_name=target.video_name,
            )
            await trans.transcode(info)
            log.info(f"End transcode video", {"elapsed_time": round(cur_duration(start_time), 3)})
        log.info("All transcoding is done")

    async def transcode_by_local(self):
        trans = StdlTranscoder(
            accessor=StdlLocalSegmentAccessor(local_incomplete_dir_path=self.incomplete_dir_path),
            notifier=self.notifier,
            out_dir_path=path_join(self.out_dir_path, "complete"),
            tmp_path=self.tmp_dir_path,
            is_archive=self.is_archive,
            video_size_limit_gb=self.video_size_limit_gb,
        )
        for platform_name in await aios.listdir(self.incomplete_dir_path):
            platform_dir_path = await checked_dir_path(self.incomplete_dir_path, platform_name)
            for channel_id in await aios.listdir(platform_dir_path):
                channel_dir_path = await checked_dir_path(platform_dir_path, channel_id)
                for video_name in await aios.listdir(channel_dir_path):
                    await checked_dir_path(channel_dir_path, video_name)
                    info = StdlSegmentsInfo(
                        platform_name=platform_name,
                        channel_id=channel_id,
                        video_name=video_name,
                    )
                    await trans.transcode(info)
                    log.info(f"End transcode video", {video_name: video_name})

        message = "All transcoding is done"
        log.info(message)
        await self.notifier.notify(message)

    async def download(self, targets: list[ArchiveTarget]):
        start_time = asyncio.get_event_loop().time()

        for target in targets:
            cnt = 0
            keys: list[str] = []
            prefix = path_join("incomplete", target.platform, target.uid, target.video_name)
            async for obj in self.s3_client.list_all_objects(prefix=prefix):
                keys.append(obj.key)

            out_dir_path = path_join(self.out_dir_path, target.platform, target.uid, target.video_name)
            await aios.makedirs(out_dir_path, exist_ok=True)
            for key in keys:
                file_path = path_join(out_dir_path, filename(key))
                await self.s3_client.write_file(key=key, file_path=file_path, sync_time=True)
                if cnt % 100 == 0:
                    log.info(f"Archived {cnt} files")
                cnt += 1
            log.info(f"Archived {len(keys)} files")

        log.info(f"Elapsed time: {cur_duration(start_time):.3f} sec")


async def checked_dir_path(base_dir_path: str, new_path: str) -> str:
    return_dir_path = path_join(base_dir_path, new_path)
    if not await aios.path.isdir(return_dir_path):
        raise ValueError(f"Invalid directory: {return_dir_path}")
    return return_dir_path
