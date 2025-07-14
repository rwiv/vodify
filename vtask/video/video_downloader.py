import asyncio
from datetime import datetime
from urllib.parse import urlparse

from aiofiles import os as aios
from pyutils import get_query_string, path_join

from .chzzk.chzzk_video_client_1 import ChzzkVideoClient1
from .chzzk.chzzk_video_client_2 import ChzzkVideoClient2
from .chzzk.chzzk_video_downloader import ChzzkVideoDownloader
from .schema.video_schema import VideoPlatform, VideoDownloadContext
from .soop.soop_video_downloader import SoopVideoDownloader
from .video_utils import convert_to_mp4
from .ytdl.ytdl_downloader import YtdlDownloader
from ..external.hls import HlsDownloader
from ..utils import get_headers


class VideoDownloader:
    def __init__(self, out_dir_path: str, tmp_dir_path: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.out_dir_path = out_dir_path
        self.tmp_dir_path = tmp_dir_path

    async def download(self, url: str, is_m3u8_url: bool = False):
        if is_m3u8_url:
            return await self.__download_hls_video_using_m3u8(url)

        platform = find_platform_by_url(url)
        if platform == VideoPlatform.CHZZK:
            return await self.__download_chzzk_video(url)
        elif platform == VideoPlatform.SOOP:
            return await self.__download_soop_video(url)
        elif platform == VideoPlatform.MISC:
            return await self.__download_video_using_ytdl(url)
        else:
            raise ValueError(f"Unsupported platform: {platform}")

    async def __download_hls_video_using_m3u8(self, m3u8_url: str):
        hls = HlsDownloader(
            out_dir_path=self.tmp_dir_path,
            headers=get_headers(self.ctx.cookie_str),
            parallel_num=self.ctx.parallel_num,
            network_mbit=self.ctx.network_mbit,
        )
        qs = None
        if self.ctx.use_qs:
            qs = get_query_string(m3u8_url) or None
        file_stem = datetime.now().strftime("%Y%m%d_%H%M%S")
        urls = await hls.get_seg_urls_by_media(m3u8_url, qs)
        dir_name = "hls"
        segments_path = path_join(self.tmp_dir_path, dir_name, file_stem)
        if self.ctx.is_parallel:
            await hls.download_parallel(urls=urls, segments_path=segments_path)
        else:
            await hls.download(urls=urls, segments_path=segments_path)
        out_mp4_path = path_join(self.out_dir_path, dir_name, f"{file_stem}.mp4")
        await convert_to_mp4(file_path=out_mp4_path, segments_path=segments_path)
        if len(await aios.listdir(path_join(self.tmp_dir_path, dir_name))) == 0:
            await aios.rmdir(path_join(self.tmp_dir_path, dir_name))

    async def __download_video_using_ytdl(self, url):
        downloader = YtdlDownloader(self.out_dir_path)
        await asyncio.to_thread(downloader.download, [url])

    async def __download_chzzk_video(self, url: str):
        video_no = int(url.split("/")[-1])
        try:
            c = ChzzkVideoClient1(self.ctx.cookie_str)
            dl = ChzzkVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx, c)
            await dl.download_one(video_no)
        except Exception:
            c = ChzzkVideoClient2(self.ctx.cookie_str)
            dl = ChzzkVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx, c)
            await dl.download_one(video_no)

    async def __download_soop_video(self, url: str):
        dl = SoopVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx)
        video_no = int(url.replace("/catch", "").split("/")[-1])
        await dl.download_one(video_no)


def find_platform_by_url(url: str) -> VideoPlatform:
    origin = urlparse(url).netloc
    if "chzzk" in origin:
        return VideoPlatform.CHZZK
    elif "soop" in origin:
        return VideoPlatform.SOOP
    elif "afreeca" in origin:
        return VideoPlatform.SOOP
    else:
        return VideoPlatform.MISC
