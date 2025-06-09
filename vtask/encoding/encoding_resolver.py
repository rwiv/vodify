from .encoding_request import EncodingRequest, VideoCodec, AudioCodec


def resolve_video_codec(req: EncodingRequest) -> str:
    if req.enable_gpu:
        if req.video_codec == VideoCodec.H265:
            return "hevc_nvenc"
        elif req.video_codec == VideoCodec.AV1:
            return "av1_nvenc"
        else:
            raise ValueError(f"Unsupported GPU video codec: {req.video_codec}")
    else:
        if req.video_codec == VideoCodec.H265:
            return "libx265"
        elif req.video_codec == VideoCodec.AV1:
            return "libsvtav1"
        elif req.video_codec == VideoCodec.COPY:
            return "copy"
        else:
            raise ValueError(f"Unsupported video codec: {req.video_codec}")


def resolve_audio_codec(req: EncodingRequest) -> str:
    if req.audio_codec == AudioCodec.AAC:
        return "aac"
    elif req.audio_codec == AudioCodec.OPUS:
        return "libopus"
    elif req.audio_codec == AudioCodec.COPY:
        return "copy"
    else:
        raise ValueError(f"Unsupported audio codec: {req.audio_codec}")


def resolve_audio_bitrate(req: EncodingRequest) -> str | None:
    if req.audio_bitrate_kb is not None:
        return f"{req.audio_bitrate_kb}k"
    return None


def resolve_vf(req: EncodingRequest) -> str | None:
    vf = []
    if req.video_scale:
        vf.append(f"scale={req.video_scale.width}:{req.video_scale.height}")
    if req.video_frame:
        vf.append(f"fps={req.video_frame}")
    return ",".join(vf) if vf else None


def resolve_command(req: EncodingRequest) -> list[str]:
    video_codec = resolve_video_codec(req)
    audio_codec = resolve_audio_codec(req)
    audio_bitrate = resolve_audio_bitrate(req)
    vf = resolve_vf(req)

    command = ["ffmpeg", "-i", req.src_file_path, "-c:v", video_codec]

    if req.video_quality is not None:
        if req.enable_gpu:
            command.extend(["-cq", str(req.video_quality)])
        else:
            command.extend(["-crf", str(req.video_quality)])
    if req.video_preset:
        command.extend(["-preset", req.video_preset])
    if vf:
        command.extend(["-vf", vf])
    command.extend(["-c:a", audio_codec])
    if audio_bitrate:
        command.extend(["-b:a", audio_bitrate])

    command.extend(["-v", "warning", "-progress", "-", req.out_file_path])

    return command
