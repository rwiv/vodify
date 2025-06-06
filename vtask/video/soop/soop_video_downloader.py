from aiofiles import os as aios
from pydantic import BaseModel
from pyutils import path_join

from .soop_hls_url_extractor import SoopHlsUrlExtractor
from .soop_video_client import SoopVideoClient, SoopVideoInfo
from ..schema.video_schema import VideoDownloadContext
from ...ffmpeg import concat_streams, remux_video, get_info, concat_by_list
from ...utils import get_headers, stem, move_file, rmtree, write_file
from ...utils.hls import HlsDownloader, merge_ts_async


class SegmentsPathInfo(BaseModel):
    bj_id: str
    video_name: str
    out_dir_path: str
    video_segments_path: str
    audio_segments_path: str


class FfprobeStream(BaseModel):
    codec_type: str


class FfprobeInfo(BaseModel):
    streams: list[FfprobeStream]


class SoopVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, ctx: VideoDownloadContext):
        self.ctx = ctx
        self.out_dir_path = out_dir
        self.tmp_dir_path = tmp_dir
        self.hls = HlsDownloader(
            out_dir_path=tmp_dir,
            headers=get_headers(ctx.cookie_str),
            parallel_num=ctx.parallel_num,
            network_mbit=ctx.network_mbit,
            url_extractor=SoopHlsUrlExtractor(),
        )
        self.client = SoopVideoClient(cookie_str=self.ctx.cookie_str)

    async def download_one(self, title_no: int) -> str | list[str]:
        video_info = await self.client.get_video_info(title_no)
        bj_id = video_info.bj_id
        path_infos: list[SegmentsPathInfo] = []
        for i, m3u8_info in enumerate(video_info.m3u8_infos):
            video_path = await self.__download_segments(m3u8_info.video_master_url, bj_id, f"{i}_video", True)
            audio_path = await self.__download_segments(m3u8_info.audio_media_url, bj_id, f"{i}_audio", False)
            segments_path = SegmentsPathInfo(
                bj_id=bj_id,
                video_name=str(i),
                out_dir_path=self.tmp_dir_path,
                video_segments_path=video_path,
                audio_segments_path=audio_path,
            )
            path_infos.append(segments_path)

        if len(path_infos) == 0:
            raise Exception("No segments downloaded")

        if len(path_infos) == 1:
            return await self.__remux_video(path_infos[0], bj_id, f"{title_no}.mp4")

        if self.ctx.concat:
            out_mp4_path = await self.__concat_video_parts(title_no, video_info, path_infos)
            return out_mp4_path

        result = []
        for i, segments_path in enumerate(path_infos):
            file_name = f"{title_no}_{i}.mp4"
            out_mp4_path = await self.__remux_video(segments_path, bj_id, file_name)
            result.append(out_mp4_path)
        return result

    async def __download_segments(self, m3u8_url: str, bj_id: str, file_name: str, is_video: bool) -> str:
        if is_video:
            urls = await self.hls.get_seg_urls_by_master(m3u8_url, None)
        else:
            urls = await self.hls.get_seg_urls_by_media(m3u8_url, None)
        segments_path = path_join(self.tmp_dir_path, bj_id, file_name)
        if self.ctx.is_parallel:
            return await self.hls.download_parallel(urls=urls, segments_path=segments_path)
        else:
            return await self.hls.download(urls=urls, segments_path=segments_path)

    async def __remux_video(self, path_info: SegmentsPathInfo, bj_id: str, file_name: str):
        tmp_mp4_path = await mux_to_mp4(path_info)
        out_mp4_path = path_join(self.out_dir_path, bj_id, file_name)

        await aios.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
        await move_file(tmp_mp4_path, out_mp4_path)

        if len(await aios.listdir(path_join(self.tmp_dir_path, bj_id))) == 0:
            await aios.rmdir(path_join(self.tmp_dir_path, bj_id))
        return out_mp4_path

    async def __concat_video_parts(
        self, title_no: int, video_info: SoopVideoInfo, path_infos: list[SegmentsPathInfo]
    ) -> str:
        bj_id = video_info.bj_id
        await aios.makedirs(path_join(self.out_dir_path, bj_id), exist_ok=True)
        await aios.makedirs(path_join(self.tmp_dir_path, bj_id), exist_ok=True)

        tmp_mp4_part_paths = []
        for path_info in path_infos:
            tmp_mp4_path = await mux_to_mp4(path_info)
            tmp_mp4_part_paths.append(tmp_mp4_path)

        list_file_path = path_join(self.tmp_dir_path, bj_id, "list.txt")
        await write_file(list_file_path, "\n".join([f"file '{f}'" for f in tmp_mp4_part_paths]))

        tmp_mp4_path = path_join(self.tmp_dir_path, bj_id, f"{title_no}.mp4")
        out_mp4_path = path_join(self.out_dir_path, bj_id, f"{title_no}.mp4")

        await concat_by_list(list_file_path, tmp_mp4_path)
        await move_file(tmp_mp4_path, out_mp4_path)

        await aios.remove(list_file_path)
        for tmp_mp4_part_path in tmp_mp4_part_paths:
            await aios.remove(tmp_mp4_part_path)
        if len(await aios.listdir(path_join(self.tmp_dir_path, bj_id))) == 0:
            await aios.rmdir(path_join(self.tmp_dir_path, bj_id))

        return out_mp4_path


async def mux_to_mp4(info: SegmentsPathInfo) -> str:
    # validate segments
    video_seg_names = await aios.listdir(info.video_segments_path)
    video_seg_names.sort(key=lambda x: int(stem(x)))
    audio_seg_names = await aios.listdir(info.video_segments_path)
    audio_seg_names.sort(key=lambda x: int(stem(x)))
    if len(video_seg_names) != len(audio_seg_names):
        raise ValueError("Video and audio segments count mismatch")

    await check_stream(path_join(info.video_segments_path, video_seg_names[0]), True)
    await check_stream(path_join(info.audio_segments_path, audio_seg_names[0]), False)

    # merge ts files
    video_path = await merge_ts_async(info.video_segments_path)
    await rmtree(info.video_segments_path)
    audio_path = await merge_ts_async(info.audio_segments_path)
    await rmtree(info.audio_segments_path)

    merged_ts_path = path_join(info.out_dir_path, info.bj_id, f"{info.video_name}.ts")
    await concat_streams(video_path=video_path, audio_path=audio_path, out_path=merged_ts_path)
    await aios.remove(video_path)
    await aios.remove(audio_path)

    # convert ts to mp4
    mp4_path = path_join(info.out_dir_path, info.bj_id, f"{info.video_name}.mp4")
    await remux_video(merged_ts_path, mp4_path)
    await aios.remove(merged_ts_path)
    return mp4_path


async def check_stream(file_path: str, is_video: bool):
    info = await get_info(file_path)
    if len(info.streams) != 1:
        raise Exception(f"ffprobe returned {len(info.streams)} streams, expected 1")
    stream = info.streams[0]
    if is_video and stream.codec_type != "video":
        raise Exception(f"Expected video codec, got {stream.codec_type}")
    if not is_video and stream.codec_type != "audio":
        raise Exception(f"Expected audio codec, got {stream.codec_type}")
