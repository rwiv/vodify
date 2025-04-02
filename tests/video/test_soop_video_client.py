from vtask.service.video import SoopVideoClient


def test_get_info():
    print()
    title_no = 0
    client = SoopVideoClient(cookie_str=None)
    info = client.get_video_info(title_no)
    print(len(info.m3u8_urls))
