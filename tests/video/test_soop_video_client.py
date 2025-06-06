import pytest

from vtask.video import SoopVideoClient, SoopHlsUrlExtractor


@pytest.mark.asyncio
async def test_get_info():
    print()
    title_no = 0
    client = SoopVideoClient(cookie_str=None)
    info = await client.get_video_info(title_no)
    print(len(info.m3u8_infos))
    urls = await SoopHlsUrlExtractor().get_urls(info.m3u8_infos[0].video_master_url, None)
    print(len(urls))
