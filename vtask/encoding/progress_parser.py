import re

from pydantic import BaseModel


class ProgressInfo(BaseModel):
    frame: int | None = None
    fps: float | None = None
    stream_q: float | None = None
    bitrate_kbits: float | None = None
    total_size: int | None = None
    out_time_us: int | None = None
    out_time_ms: int | None = None
    out_time: str | None = None
    dup_frames: int | None = None
    drop_frames: int | None = None
    speed: float | None = None
    progress: str | None = None


def parse_encoding_progress(data: bytes) -> ProgressInfo:
    lines = data.decode().splitlines()
    fields = {}

    for line in lines:
        if not line.strip():
            continue

        if "=" not in line:
            raise ValueError(f"invalid format: {line}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Map keys from ffmpeg format to ProgressInfo attributes
        if key == "frame":
            fields["frame"] = check_nan(value)
        elif key == "fps":
            fields["fps"] = check_nan(value)
        elif key == "stream_0_0_q":  # input=0, output=0 stream
            fields["stream_q"] = check_nan(value)
        elif key == "bitrate":
            fields["bitrate_kbits"] = parse_bitrate(value)
        elif key == "total_size":
            fields["total_size"] = check_nan(value)
        elif key == "out_time_us":
            fields["out_time_us"] = check_nan(value)
        elif key == "out_time_ms":
            fields["out_time_ms"] = check_nan(value)
        elif key == "out_time":
            fields["out_time"] = value
        elif key == "dup_frames":
            fields["dup_frames"] = check_nan(value)
        elif key == "drop_frames":
            fields["drop_frames"] = check_nan(value)
        elif key == "speed":
            fields["speed"] = parse_speed(value)
        elif key == "progress":
            fields["progress"] = value
        else:
            raise ValueError(f"unknown key: {key}")

    return ProgressInfo(**fields)


def check_nan(value: str) -> str | None:
    try:
        return value if value != "N/A" else None
    except ValueError:
        return None


def parse_speed(value: str) -> float | None:
    match = re.match(r"([\d.]+)x", value)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_bitrate(value: str) -> str:
    suffix = "kbits/s"
    if not value.endswith(suffix):
        raise ValueError(f"invalid bitrate format: {value}")
    return value[: -len(suffix)]
