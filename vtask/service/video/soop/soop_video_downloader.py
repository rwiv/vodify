import asyncio
import os
import random
import shutil
import subprocess

import requests
from pyutils import path_join, write_file, sanitize_filename

from .soop_hls_url_extractor import SoopHlsUrlExtractor
from ..schema.video_schema import VideoDownloadContext
from ....utils import get_headers
from ....utils.hls.downloader import HlsDownloader


class SoopVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.out_dir = out_dir
        self.tmp_dir = tmp_dir
        self.hls = HlsDownloader(
            tmp_dir,
            get_headers(ctx.cookie_str),
            ctx.parallel_num,
            ctx.non_parallel_delay_ms,
            SoopHlsUrlExtractor(),
        )

    def download_one(self, title_no: int):
        m3u8_urls, title, bjId = self.__get_url(title_no)
        file_title = str(title_no)
        for i, m3u8_url in enumerate(m3u8_urls):
            if self.ctx.is_parallel:
                asyncio.run(self.hls.download_parallel(m3u8_url, bjId, file_title))
            else:
                asyncio.run(self.hls.download_non_parallel(m3u8_url, bjId, file_title))

        # merge
        os.makedirs(path_join(self.out_dir, bjId), exist_ok=True)
        os.makedirs(path_join(self.tmp_dir, bjId), exist_ok=True)
        file_title = sanitize_filename(title)
        out_paths = [path_join(self.out_dir, bjId, f"{file_title}_{i}.mp4") for i in range(len(m3u8_urls))]
        tmp_paths = [path_join(self.tmp_dir, bjId, f"{i}.mp4") for i in range(len(m3u8_urls))]
        for i, out_path in enumerate(out_paths):
            shutil.move(out_path, tmp_paths[i])

        list_path = path_join(self.tmp_dir, bjId, "list.txt")
        write_file(list_path, "\n".join([f"file '{f}'" for f in tmp_paths]))
        rand_num = random.randint(100000, 999999)

        tmp_mp4_path = path_join(self.tmp_dir, bjId, f"{rand_num}_{file_title}.mp4")
        out_mp4_path = path_join(self.out_dir, bjId, f"{rand_num}_{file_title}.mp4")

        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", tmp_mp4_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        shutil.move(tmp_mp4_path, out_mp4_path)
        os.remove(list_path)
        for input_file in tmp_paths:
            os.remove(input_file)

        return out_mp4_path

    def __get_url(self, title_no: int):
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(
            url,
            headers=get_headers(self.ctx.cookie_str, "application/json"),
            data={
                "nTitleNo": title_no,
                "nApiLevel": 10,
                "nPlaylistIdx": 0,
            },
        ).json()
        data = res["data"]
        return [f["file"] for f in data["files"]], data["full_title"].strip(), data["bj_id"]
