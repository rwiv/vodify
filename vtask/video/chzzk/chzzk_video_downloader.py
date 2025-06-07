from aiofiles import os as aios
from pyutils import path_join

from .chzzk_video_client import ChzzkVideoClient
from ..schema.video_schema import VideoDownloadContext
from ..video_utils import convert_to_mp4
from ...utils import get_headers
from ...utils.hls import HlsDownloader


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

    async def download_one(self, video_no: int) -> str:
        info = await self.client.get_video_info(video_no)
        channel_id = info.channel_id
        file_name = str(video_no)

        urls = await self.hls.get_seg_urls_by_master(info.m3u8_url, info.qs)
        segments_path = path_join(self.tmp_dir_path, channel_id, file_name)

        if self.ctx.is_parallel:
            await self.hls.download_parallel(urls=urls, segments_path=segments_path)
        else:
            await self.hls.download(urls=urls, segments_path=segments_path)

        # move to out dir
        out_mp4_path = path_join(self.out_dir_path, channel_id, f"{file_name}.mp4")
        await convert_to_mp4(file_path=out_mp4_path, segments_path=segments_path)

        # clean up empty directories
        if len(await aios.listdir(path_join(self.tmp_dir_path, channel_id))) == 0:
            await aios.rmdir(path_join(self.tmp_dir_path, channel_id))
        return out_mp4_path
