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
from ....utils.hls.merge import remux_to_mp4


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

    def download_one(self, title_no: int) -> str | list[str]:
        info = self.client.get_video_info(title_no)
        bj_id = info.bj_id
        chunks_paths = []
        for i, m3u8_url in enumerate(info.m3u8_urls):
            if self.ctx.is_parallel:
                chunks_path = asyncio.run(self.hls.download_parallel(m3u8_url, bj_id, str(i)))
            else:
                chunks_path = asyncio.run(self.hls.download_non_parallel(m3u8_url, bj_id, str(i)))
            chunks_paths.append(chunks_path)

        if len(chunks_paths) == 0:
            raise Exception("No chunks downloaded")

        if len(chunks_paths) == 1:
            return self.__remux_video(chunks_paths[0], bj_id, f"{title_no}.mp4")

        if self.ctx.concat:
            out_mp4_path = self.__concat_video_parts(title_no, info, chunks_paths)
            return out_mp4_path

        result = []
        for i, chunks_path in enumerate(chunks_paths):
            file_name = f"{title_no}_{i}.mp4"
            out_mp4_path = self.__remux_video(chunks_path, bj_id, file_name)
            result.append(out_mp4_path)
        return result

    def __remux_video(self, chunks_path: str, bj_id: str, file_name: str):
        tmp_mp4_path = remux_to_mp4(chunks_path)
        out_mp4_path = path_join(self.out_dir_path, bj_id, file_name)

        os.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
        shutil.move(tmp_mp4_path, out_mp4_path)

        if len(os.listdir(path_join(self.tmp_dir_path, bj_id))) == 0:
            os.rmdir(path_join(self.tmp_dir_path, bj_id))
        return out_mp4_path

    def __concat_video_parts(self, title_no: int, info: SoopVideoInfo, chunks_paths: list[str]) -> str:
        bj_id = info.bj_id
        os.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
        os.makedirs(path_join(self.tmp_dir_path, bj_id), exist_ok=True)

        tmp_mp4_part_paths = []
        for chunks_path in chunks_paths:
            tmp_mp4_path = remux_to_mp4(chunks_path)
            tmp_mp4_part_paths.append(tmp_mp4_path)

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
        if len(os.listdir(path_join(self.tmp_dir_path, bj_id))) == 0:
            os.rmdir(path_join(self.tmp_dir_path, bj_id))

        return out_mp4_path
