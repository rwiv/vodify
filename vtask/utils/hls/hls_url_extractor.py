import aiohttp
from pyutils import get_base_url

from .parser import parse_master_playlist, parse_media_playlist, Resolution


class HlsUrlExtractor:
    def __init__(self, headers: dict | None = None):
        self.__headers = headers

    async def get_urls(self, m3u8_url: str, qs: str | None = None) -> list[str]:
        m3u8 = await self.__fetch_m3u8_content(m3u8_url)
        pl = parse_master_playlist(m3u8)

        if len(pl.resolutions) == 0:
            raise ValueError("No resolutions found")

        r = pl.resolutions[0]
        for cur in pl.resolutions:
            if cur.resolution > r.resolution:
                r = cur

        base_url = self._get_base_url(m3u8_url, r)
        if qs is not None and qs != "":
            base_url += f"?{qs}"
        m3u8 = await self.__fetch_m3u8_content(base_url)
        pl = parse_media_playlist(m3u8, get_base_url(base_url), qs)
        return pl.segment_paths

    async def __fetch_m3u8_content(self, m3u8_url: str) -> str:
        async with aiohttp.ClientSession(headers=self.__headers) as session:
            async with session.get(m3u8_url) as res:
                if res.status >= 400:
                    raise ValueError(f"Failed to fetch m3u8 content: {res.status}")
                return await res.text()

    def _get_base_url(self, m3u8_url: str, r: Resolution) -> str:
        return f"{get_base_url(m3u8_url)}/{r.name}"
