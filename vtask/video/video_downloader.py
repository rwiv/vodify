import asyncio
from datetime import datetime
from urllib.parse import urlparse

from pyutils import get_query_string, path_join

from .chzzk.chzzk_video_client_1 import ChzzkVideoClient1
from .chzzk.chzzk_video_client_2 import ChzzkVideoClient2
from .chzzk.chzzk_video_downloader import ChzzkVideoDownloader
from .schema.video_schema import VideoPlatform, VideoDownloadContext
from .soop.soop_video_downloader import SoopVideoDownloader
from .ytdl.ytdl_downloader import YtdlDownloader
from ..utils import get_headers
from ..utils.hls.downloader import HlsDownloader


class VideoDownloader:
    def __init__(self, out_dir_path: str, tmp_dir_path: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.out_dir_path = out_dir_path
        self.tmp_dir_path = tmp_dir_path

    def download(self, url: str, is_m3u8_url: bool = False):
        if is_m3u8_url:
            return self.__download_hls_video_using_m3u8(url)

        platform = find_platform_by_url(url)
        if platform == VideoPlatform.CHZZK:
            return self.__download_chzzk_video(url)
        elif platform == VideoPlatform.SOOP:
            return self.__download_soop_video(url)
        elif platform == VideoPlatform.MISC:
            return self.__download_video_using_ytdl(url)
        else:
            raise ValueError(f"Unsupported platform: {platform}")

    def __download_hls_video_using_m3u8(self, m3u8_url: str):
        hls = HlsDownloader(
            out_dir_path=self.tmp_dir_path,
            headers=get_headers(self.ctx.cookie_str),
            parallel_num=self.ctx.parallel_num,
            network_mbit=self.ctx.network_mbit,
        )
        qs = get_query_string(m3u8_url) or None
        title = datetime.now().strftime("%Y%m%d_%H%M%S")
        urls = hls.get_seg_urls_by_master(m3u8_url, qs)
        chunks_path = path_join(self.tmp_dir_path, "hls", title)
        if self.ctx.is_parallel:
            asyncio.run(hls.download_parallel(urls=urls, segments_path=chunks_path))
        else:
            asyncio.run(hls.download(urls=urls, segments_path=chunks_path))

    def __download_video_using_ytdl(self, url):
        YtdlDownloader(self.out_dir_path).download([url])

    def __download_chzzk_video(self, url: str):
        video_no = int(url.split("/")[-1])
        try:
            c = ChzzkVideoClient1(self.ctx.cookie_str)
            dl = ChzzkVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx, c)
            dl.download_one(video_no)
        except Exception:
            c = ChzzkVideoClient2(self.ctx.cookie_str)
            dl = ChzzkVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx, c)
            dl.download_one(video_no)

    def __download_soop_video(self, url: str):
        dl = SoopVideoDownloader(self.tmp_dir_path, self.out_dir_path, self.ctx)
        video_no = int(url.split("/")[-1])
        dl.download_one(video_no)


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
