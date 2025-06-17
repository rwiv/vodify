from vtask.encoding import resolve_command, EncodingRequest


def test_resolver():
    src_file_path = "input.mp4"
    out_file_path = "output.mp4"

    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoCodec="h265",  # type: ignore
        videoQuality=23,
        videoPreset="p4",
        videoScale={"width": 1280, "height": 720},  # type: ignore
        videoFrame=30,
        videoMaxBitrate=3000,
        audioCodec="opus",  # type: ignore
        audioBitrateKb=128,
        enableGpu=True,
    )
    expected = ["ffmpeg", "-hwaccel", "nvdec", "-hwaccel_output_format", "cuda", "-i", src_file_path]
    expected.extend(["-c:v", "hevc_nvenc"])
    expected.extend(["-cq", "23", "-preset", "p4"])
    expected.extend(["-vf", "scale=1280:720,fps=30"])
    expected.extend(["-c:a", "libopus", "-b:a", "128k"])
    expected.extend(["-v", "warning", "-progress", "-", out_file_path])
    assert resolve_command(req) == expected

    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoCodec="av1",  # type: ignore
        videoQuality=23,
        videoPreset="4",
        videoScale=None,
        videoFrame=None,
        audioCodec="opus",  # type: ignore
        audioBitrateKb=128,
        enableGpu=False,
    )
    expected = ["ffmpeg", "-i", src_file_path]
    expected.extend(["-c:v", "libsvtav1"])
    expected.extend(["-crf", "23", "-preset", "4"])
    expected.extend(["-c:a", "libopus", "-b:a", "128k"])
    expected.extend(["-v", "warning", "-progress", "-", out_file_path])
    assert resolve_command(req) == expected

    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoScale=None,
        videoFrame=None,
        audioBitrateKb=None,
        enableGpu=False,
    )
    expected = ["ffmpeg", "-i", src_file_path, "-c:v", "copy", "-c:a", "copy"]
    expected.extend(["-v", "warning", "-progress", "-", out_file_path])
    assert resolve_command(req) == expected

    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoScale=None,
        videoFrame=30,
        audioBitrateKb=None,
        enableGpu=False,
    )
    expected = ["ffmpeg", "-i", src_file_path, "-c:v", "copy", "-vf", "fps=30", "-c:a", "copy"]
    expected.extend(["-v", "warning", "-progress", "-", out_file_path])
    assert resolve_command(req) == expected

    req = EncodingRequest(
        srcFilePath=src_file_path,
        outFilePath=out_file_path,
        videoCodec="av1",  # type: ignore
        videoQuality=23,
        videoPreset="4",
        videoScale=None,
        videoFrame=None,
        videoMaxBitrate=3000,
        audioBitrateKb=None,
        enableGpu=False,
    )
    expected = ["ffmpeg", "-i", src_file_path]
    expected.extend(["-c:v", "libsvtav1"])
    expected.extend(["-crf", "23", "-preset", "4"])
    expected.extend(["-svtav1-params", f"mbr=3000"])
    expected.extend(["-c:a", "copy"])
    expected.extend(["-v", "warning", "-progress", "-", out_file_path])
    assert resolve_command(req) == expected
