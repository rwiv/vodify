from aiofiles import os as aios
from pyutils import path_join

from .chzzk_video_client import ChzzkVideoClient
from ..schema.video_schema import VideoDownloadContext
from ...ffmpeg import remux_video
from ...utils import get_headers, move_file, rmtree
from ...utils.hls import HlsDownloader, merge_ts


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
            segments_path = await self.hls.download_parallel(urls=urls, segments_path=segments_path)
        else:
            segments_path = await self.hls.download(urls=urls, segments_path=segments_path)

        # merge and remux
        merged_ts_path = await merge_ts(segments_path)
        await rmtree(segments_path)
        tmp_mp4_path = f"{segments_path}.mp4"
        await remux_video(merged_ts_path, tmp_mp4_path)
        await aios.remove(merged_ts_path)

        # move to out dir
        out_mp4_path = path_join(self.out_dir_path, channel_id, f"{file_name}.mp4")
        await aios.makedirs(path_join(self.out_dir_path, channel_id), exist_ok=True)
        await move_file(tmp_mp4_path, out_mp4_path)

        # clean up empty directories
        if len(await aios.listdir(path_join(self.tmp_dir_path, channel_id))) == 0:
            await aios.rmdir(path_join(self.tmp_dir_path, channel_id))
        return out_mp4_path
