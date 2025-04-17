import os

from pyutils import path_join

from tests.testutils.test_utils_conf import read_test_conf
from tests.testutils.test_utils_fs import read_test_fs_configs, find_test_fs_config
from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.utils import S3ObjectWriter

load_test_dotenv(".env-server-dev")

from vtask.service.stdl.transcoder import StdlSegmentedTranscoder, StdlS3Helper, StdlSegmentsInfo
from vtask.service.stdl.schema import StdlDoneMsg, StdlDoneStatus, StdlPlatformType

test_conf = read_test_conf()


# fs_name = "local"
fs_name = "minio"

is_archive = True
# is_archive = False

platform = StdlPlatformType.CHZZK
video_name = "test_video"
complete = StdlDoneStatus.COMPLETE
canceled = StdlDoneStatus.CANCELED

done_messages = [
    StdlDoneMsg(platform=platform, uid="test_uid1", videoName=video_name, fsName=fs_name, status=complete),
    StdlDoneMsg(platform=platform, uid="test_uid2", videoName=video_name, fsName=fs_name, status=complete),
    StdlDoneMsg(platform=platform, uid="test_uid3", videoName=video_name, fsName=fs_name, status=canceled),
    StdlDoneMsg(platform=platform, uid="test_uid4", videoName=video_name, fsName=fs_name, status=canceled),
    StdlDoneMsg(platform=platform, uid="test_uid5", videoName=video_name, fsName=fs_name, status=complete),
]

fs_configs = read_test_fs_configs(is_prod=False)
fs_conf = find_test_fs_config(fs_configs, fs_name)
# src_writer = LocalObjectWriter()
src_writer = S3ObjectWriter(fs_conf.s3)  # type: ignore

local_chunks_path = test_conf.chunks_path
base_dir_path = test_conf.local_base_dir_path
tmp_dir_path = test_conf.tmp_dir_path


def write_test_context_files(platform: str, uid: str, video_name: str):
    incomplete_path = src_writer.normalize_base_path(path_join(base_dir_path, "incomplete"))
    vid_dir_path = path_join(incomplete_path, platform, uid, video_name)

    for chunk_name in os.listdir(local_chunks_path):
        with open(path_join(local_chunks_path, chunk_name), mode="rb") as f:
            src_writer.write(path_join(vid_dir_path, chunk_name), f.read())


def test_transcode():
    print()
    target = done_messages[0]

    write_test_context_files(target.platform.value, target.uid, target.video_name)

    helper = StdlS3Helper(
        local_incomplete_dir_path=path_join(base_dir_path, "incomplete"),
        conf=fs_conf.s3,  # type: ignore
        network_io_delay_ms=1,
        network_buf_size=65536,
    )
    transcoder = StdlSegmentedTranscoder(
        helper=helper, base_path=base_dir_path, tmp_path=tmp_dir_path, is_archive=is_archive
    )
    info = StdlSegmentsInfo(
        platform_name=target.platform.value,
        channel_id=target.uid,
        video_name=target.video_name,
    )
    result = transcoder.transcode(info)
    print(result)
