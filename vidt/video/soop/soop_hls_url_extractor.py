from pyutils import get_origin_url, get_base_url

from ...external.hls import HlsUrlExtractor, Resolution


class SoopHlsUrlExtractor(HlsUrlExtractor):
    def _get_base_url(self, m3u8_url: str, r: Resolution) -> str:
        if "/" not in r.name:
            return f"{get_base_url(m3u8_url)}/{r.name}"
        if r.name.startswith("../"):
            return f"{get_base_url(get_base_url(m3u8_url))}{r.name[2:]}"
        else:
            return f"{get_origin_url(m3u8_url)}{r.name}"
