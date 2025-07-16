import asyncio

import aiofiles
import aiohttp
from aiofiles import os as aios
from pyutils import path_join, log, error_dict, get_base_url

from .hls_url_extractor import HlsUrlExtractor
from .hls_parser import parse_media_playlist
from .hls_utils import sub_lists_with_idx
from ...utils import nio_limiter, fetch_text


class HttpError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP Error: {status_code}")


class HlsDownloader:
    def __init__(
        self,
        out_dir_path: str,
        headers: dict | None,
        parallel_num: int,
        network_mbit: float,
        url_extractor: HlsUrlExtractor | None = None,
    ):
        self.__headers = headers
        self.__tmp_dir_path = out_dir_path
        self.__parallel_num = parallel_num
        self.__network_mbit = network_mbit
        self.__url_extractor = url_extractor if url_extractor is not None else HlsUrlExtractor(headers=headers)
        self.__buf_size = 8192
        self.__retry_limit = 8
        self.__retry_base_delay_sec = 0.5

    async def get_seg_urls_by_master(self, m3u8_url: str, query_params: dict[str, list[str]] | None) -> list[str]:
        return await self.__url_extractor.get_urls(m3u8_url, query_params)

    async def get_seg_urls_by_media(self, m3u8_url: str, query_params: dict[str, list[str]] | None) -> list[str]:
        text = await fetch_text(url=m3u8_url, headers=self.__headers)
        return parse_media_playlist(text, get_base_url(m3u8_url), query_params).segment_paths

    async def download(self, urls: list[str], segments_path: str) -> str:
        await aios.makedirs(segments_path, exist_ok=True)
        retry_cnt_sum = 0
        req_cnt = 0
        for i, url in enumerate(urls):
            if req_cnt % 100 == 0:
                log.info(f"{i}")
                req_cnt = 0
            retry_cnt = await self.__download_file_wrapper(url, i, segments_path)
            req_cnt += 1
            retry_cnt_sum += retry_cnt
        if retry_cnt_sum > 0:
            log.warn(f"Retry count: {retry_cnt_sum}, segments_path={segments_path}")
        return segments_path

    async def download_parallel(self, urls: list[str], segments_path: str) -> str:
        await aios.makedirs(segments_path, exist_ok=True)
        retry_cnt_sum = 0
        for sub in sub_lists_with_idx(urls, self.__parallel_num):
            log.info(f"{sub[0].idx}-{sub[0].idx + self.__parallel_num}")
            coroutines = [self.__download_file_wrapper(elem.value, elem.idx, segments_path) for elem in sub]
            retry_counts = await asyncio.gather(*coroutines)
            retry_cnt_sum += sum(retry_counts)
        if retry_cnt_sum > 0:
            log.warn(f"Retry count: {retry_cnt_sum}, segments_path={segments_path}")
        return segments_path

    async def __download_file_wrapper(self, url: str, num: int, out_dir_path: str) -> int:
        file_path = path_join(out_dir_path, f"{num}.ts")
        retry_cnt_total = 0
        for retry_cnt in range(self.__retry_limit + 1):
            try:
                await self.__download_file(url=url, file_path=file_path)
                break
            except Exception as e:
                if await aios.path.exists(file_path):
                    await aios.remove(file_path)

                if retry_cnt == self.__retry_limit:
                    attr = error_dict(e)
                    attr["retry_cnt"] = retry_cnt
                    attr["num"] = num
                    log.error("Download Error", attr)
                    raise Exception("Download Error") from e

                retry_cnt_total += 1
                await asyncio.sleep(self.__retry_base_delay_sec * (2**retry_cnt))
        return retry_cnt_total

    async def __download_file(self, url: str, file_path: str):
        limiter = nio_limiter(self.__network_mbit, self.__buf_size)
        async with aiohttp.ClientSession(headers=self.__headers) as session:
            async with session.get(url) as res:
                if res.status >= 400:
                    raise HttpError(res.status)
                async with aiofiles.open(file_path, "wb") as file:
                    while True:
                        async with limiter:
                            chunk = await res.content.read(self.__buf_size)
                            if not chunk:
                                break
                            await file.write(chunk)
