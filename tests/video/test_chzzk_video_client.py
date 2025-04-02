from vtask.service.video import ChzzkVideoClient1


def test_get_info():
    print()
    title_no = 6588404
    client = ChzzkVideoClient1(cookie_str=None)
    info = client.get_video_info(title_no)
    print(info)
