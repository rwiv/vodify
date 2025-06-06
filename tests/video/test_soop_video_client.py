from vtask.video import SoopVideoClient, SoopHlsUrlExtractor


def test_get_info():
    print()
    title_no = 0
    client = SoopVideoClient(cookie_str=None)
    info = client.get_video_info(title_no)
    print(len(info.m3u8_infos))
    urls = SoopHlsUrlExtractor().get_urls(info.m3u8_infos[0].video_master_url, None)
    print(len(urls))
