from pyutils import path_join, find_project_root

from vidt.external.hls import parse_master_playlist, parse_media_playlist


def test_master_playlist():
    print()
    with open(path_join(find_project_root(), "dev", "test", "assets", "hls", "test_master.m3u8"), "r") as f:
        m3u8 = f.read()
    p = parse_master_playlist(m3u8)
    for r in p.resolutions:
        print(r)


def test_media_playlist():
    print()
    with open(path_join(find_project_root(), "dev", "test", "assets", "hls", "test_media.m3u8"), "r") as f:
        m3u8 = f.read()
    p = parse_media_playlist(m3u8, "https://hello/")
    print(p.ext)
    print(p.segment_paths)
