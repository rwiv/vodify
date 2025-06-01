import asyncio
import os
import time

import aiohttp
from pyutils import path_join, sanitize_filename, log, error_dict

from .hls_url_extractor import HlsUrlExtractor
from .utils import sub_lists_with_idx

buf_size = 8192
retry_count = 5


class HttpError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP Error: {status_code}")


class HlsDownloader:
    def __init__(
        self,
        out_dir_path: str,
        headers: dict | None = None,
        parallel_num: int = 3,
        non_parallel_delay_ms: int = 0,
        url_extractor=HlsUrlExtractor(),
    ):
        self.headers = headers
        self.tmp_dir_path = out_dir_path
        self.parallel_num = parallel_num
        self.non_parallel_delay_ms = non_parallel_delay_ms
        self.url_extractor = url_extractor

    async def download_parallel(
        self,
        m3u8_url: str,
        name: str,
        title: str,
        qs: str | None = None,
    ) -> str:
        title_name = sanitize_filename(title)
        chunks_path = path_join(self.tmp_dir_path, name, title_name)
        urls = self.url_extractor.get_urls(m3u8_url, qs)
        subs = sub_lists_with_idx(urls, self.parallel_num)
        for sub in subs:
            log.info(f"{sub[0].idx}-{sub[0].idx + self.parallel_num}")
            os.makedirs(chunks_path, exist_ok=True)

            tasks = [_download_file_wrapper(elem.value, self.headers, elem.idx, chunks_path) for elem in sub]
            await asyncio.gather(*tasks)
        return chunks_path

    async def download_non_parallel(
        self,
        m3u8_url: str,
        name: str,
        title: str,
        qs: str | None = None,
    ) -> str:
        title_name = sanitize_filename(title)
        chunks_path = path_join(self.tmp_dir_path, name, title_name)
        os.makedirs(chunks_path, exist_ok=True)
        urls = self.url_extractor.get_urls(m3u8_url, qs)
        cnt = 0
        for i, url in enumerate(urls):
            if cnt % 100 == 0:
                log.info(f"{i}")
                cnt = 0
            await _download_file_wrapper(url, self.headers, i, chunks_path)
            if self.non_parallel_delay_ms > 0:
                time.sleep(self.non_parallel_delay_ms / 1000)
            cnt += 1
        return chunks_path


async def _download_file_wrapper(url: str, headers: dict[str, str] | None, num: int, out_dir_path: str):
    for retry_cnt in range(retry_count + 1):
        try:
            await _download_file(url, headers, num, out_dir_path)
            break
        except Exception as e:
            err_msg = "Download Error"
            attr = error_dict(e)
            attr["retry_cnt"] = retry_cnt
            attr["num"] = num

            if retry_cnt == retry_count:
                log.warn("Download Error", attr)
                raise Exception(err_msg) from e

            log.warn(err_msg, attr)
            time.sleep(1)


async def _download_file(url: str, headers: dict[str, str] | None, num: int, out_dir_path: str):
    file_path = path_join(out_dir_path, f"{num}.ts")
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as res:
            if res.status >= 400:
                raise HttpError(res.status)
            with open(file_path, "wb") as file:
                while True:
                    chunk = await res.content.read(buf_size)
                    if not chunk:
                        break
                    file.write(chunk)
