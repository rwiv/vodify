import asyncio
import os
import shutil

from pyutils import path_join

from .chzzk_video_client import ChzzkVideoClient
from ..schema.video_schema import VideoDownloadContext
from ....utils import get_headers
from ....utils.hls.downloader import HlsDownloader
from ....utils.hls.merge import remux_to_mp4


class ChzzkVideoDownloader:
    def __init__(
        self,
        tmp_dir_path: str,
        out_dir_path: str,
        ctx: VideoDownloadContext,
        client: ChzzkVideoClient,
    ):
        self.tmp_dir_path = tmp_dir_path
        self.out_dir_path = out_dir_path
        self.ctx = ctx
        self.client = client
        self.hls = HlsDownloader(
            out_dir_path=tmp_dir_path,
            headers=get_headers(ctx.cookie_str),
            parallel_num=ctx.parallel_num,
            network_mbit=ctx.network_mbit,
        )

    def download_one(self, video_no: int) -> str:
        info = self.client.get_video_info(video_no)
        channel_id = info.channel_id
        file_name = str(video_no)
        if self.ctx.is_parallel:
            chunks_path = asyncio.run(self.hls.download_parallel(info.m3u8_url, channel_id, file_name, info.qs))
        else:
            chunks_path = asyncio.run(self.hls.download_non_parallel(info.m3u8_url, channel_id, file_name, info.qs))

        tmp_mp4_path = remux_to_mp4(chunks_path)
        out_mp4_path = path_join(self.out_dir_path, channel_id, f"{file_name}.mp4")
        os.makedirs(path_join(self.out_dir_path, channel_id), exist_ok=True)
        shutil.move(tmp_mp4_path, out_mp4_path)

        if len(os.listdir(path_join(self.tmp_dir_path, channel_id))) == 0:
            os.rmdir(path_join(self.tmp_dir_path, channel_id))
        return out_mp4_path
