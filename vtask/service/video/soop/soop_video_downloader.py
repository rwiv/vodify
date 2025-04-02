import asyncio
import os
import shutil
import subprocess

from pyutils import path_join, write_file

from .soop_hls_url_extractor import SoopHlsUrlExtractor
from .soop_video_client import SoopVideoClient, SoopVideoInfo
from ..schema.video_schema import VideoDownloadContext
from ....utils import get_headers
from ....utils.hls.downloader import HlsDownloader
from ....utils.hls.merge import merge_to_mp4


class SoopVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.out_dir_path = out_dir
        self.tmp_dir_path = tmp_dir
        self.hls = HlsDownloader(
            out_dir_path=tmp_dir,
            headers=get_headers(ctx.cookie_str),
            parallel_num=ctx.parallel_num,
            non_parallel_delay_ms=ctx.non_parallel_delay_ms,
            url_extractor=SoopHlsUrlExtractor(),
        )
        self.client = SoopVideoClient(cookie_str=self.ctx.cookie_str)

    def download_one(self, title_no: int) -> str:
        info = self.client.get_video_info(title_no)
        bj_id = info.bj_id
        for i, m3u8_url in enumerate(info.m3u8_urls):
            if self.ctx.is_parallel:
                asyncio.run(self.hls.download_parallel(m3u8_url, bj_id, str(i)))
            else:
                asyncio.run(self.hls.download_non_parallel(m3u8_url, bj_id, str(i)))

        if len(info.m3u8_urls) == 1:
            chunks_path = path_join(self.tmp_dir_path, bj_id, "0")
            tmp_mp4_path = merge_to_mp4(chunks_path)
            out_mp4_path = path_join(self.out_dir_path, bj_id, f"{title_no}.mp4")

            os.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
            shutil.move(tmp_mp4_path, out_mp4_path)

            if len(os.listdir(path_join(self.tmp_dir_path, bj_id))) == 0:
                os.rmdir(path_join(self.tmp_dir_path, bj_id))
            return out_mp4_path
        else:
            out_mp4_path = self.merge_video_parts(title_no, info)
            return out_mp4_path

    def merge_video_parts(self, title_no: int, info: SoopVideoInfo):
        bj_id = info.bj_id
        os.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
        os.makedirs(path_join(self.tmp_dir_path, bj_id), exist_ok=True)

        out_mp4_part_paths = [
            path_join(self.out_dir_path, bj_id, f"{i}.mp4") for i in range(len(info.m3u8_urls))
        ]
        tmp_mp4_part_paths = [
            path_join(self.tmp_dir_path, bj_id, f"{i}.mp4") for i in range(len(info.m3u8_urls))
        ]
        for i, out_mp4_part_path in enumerate(out_mp4_part_paths):
            shutil.move(out_mp4_part_path, tmp_mp4_part_paths[i])

        list_file_path = path_join(self.tmp_dir_path, bj_id, "list.txt")
        write_file(list_file_path, "\n".join([f"file '{f}'" for f in tmp_mp4_part_paths]))

        tmp_mp4_path = path_join(self.tmp_dir_path, bj_id, f"{title_no}.mp4")
        out_mp4_path = path_join(self.out_dir_path, bj_id, f"{title_no}.mp4")

        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file_path, "-c", "copy", tmp_mp4_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        shutil.move(tmp_mp4_path, out_mp4_path)

        os.remove(list_file_path)
        for tmp_mp4_part_path in tmp_mp4_part_paths:
            os.remove(tmp_mp4_part_path)

        return out_mp4_path
