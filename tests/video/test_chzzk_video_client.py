from vtask.video import ChzzkVideoClient1


def test_get_info():
    print()
    title_no = 0
    client = ChzzkVideoClient1(cookie_str=None)
    info = client.get_video_info(title_no)
    print(info)
