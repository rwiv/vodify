from vtask.encoding import parse_encoding_progress, ProgressInfo


def test_progress_parser():
    raw_data = b"""
    frame=123
    fps=24.0
    stream_0_0_q=22.3
    bitrate=284.3kbits/s
    total_size=2048000
    out_time_us=1000000
    out_time_ms=1000
    out_time=00:00:01.00
    dup_frames=0
    drop_frames=0
    speed=1.03x
    progress=continue
    """
    expected = ProgressInfo(
        frame=123,
        fps=24.0,
        stream_q=22.3,
        bitrate_kbits=284.3,
        total_size=2048000,
        out_time_us=1000000,
        out_time_ms=1000,
        out_time="00:00:01.00",
        dup_frames=0,
        drop_frames=0,
        speed=1.03,
        progress="continue",
    )
    assert parse_encoding_progress(raw_data) == expected
