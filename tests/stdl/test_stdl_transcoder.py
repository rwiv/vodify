import logging
import os

from pyutils import path_join, log

from tests.testutils.test_utils_conf import read_test_conf
from tests.testutils.test_utils_fs import read_test_fs_configs, find_test_fs_config
from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.common.notifier import MockNotifier
from vtask.service.stdl.archiver import StdlArchiver, ArchiveTarget
from vtask.utils import S3ObjectWriter

load_test_dotenv(".env-server-dev")

from vtask.service.stdl.transcoder import StdlTranscoder, StdlS3SegmentAccessor
from vtask.service.stdl.schema import StdlDoneMsg, StdlDoneStatus, StdlPlatformType, StdlSegmentsInfo

test_conf = read_test_conf()

# fs_name = "local"
fs = "minio"

pf = StdlPlatformType.CHZZK
vid = "test_video"
o = StdlDoneStatus.COMPLETE
x = StdlDoneStatus.CANCELED

done_messages = [
    StdlDoneMsg(platform=pf, uid="test_uid1", videoName=vid, fsName=fs, status=o),
    StdlDoneMsg(platform=pf, uid="test_uid2", videoName=vid, fsName=fs, status=o),
    StdlDoneMsg(platform=pf, uid="test_uid3", videoName=vid, fsName=fs, status=x),
    StdlDoneMsg(platform=pf, uid="test_uid4", videoName=vid, fsName=fs, status=x),
    StdlDoneMsg(platform=pf, uid="test_uid5", videoName=vid, fsName=fs, status=o),
]

fs_configs = read_test_fs_configs(is_prod=False)
fs_conf = find_test_fs_config(fs_configs, fs)
s3_conf = fs_conf.s3
assert s3_conf is not None
# src_writer = LocalObjectWriter()
src_writer = S3ObjectWriter(s3_conf)

local_chunks_path = test_conf.chunks_path
base_dir_path = test_conf.local_base_dir_path
tmp_dir_path = test_conf.tmp_dir_path


def write_test_context_files(platform: str, uid: str, video_name: str):
    incomplete_path = src_writer.normalize_base_path(path_join(base_dir_path, "incomplete"))
    vid_dir_path = path_join(incomplete_path, platform, uid, video_name)

    for chunk_name in os.listdir(local_chunks_path):
        with open(path_join(local_chunks_path, chunk_name), mode="rb") as f:
            src_writer.write(path_join(vid_dir_path, chunk_name), f.read())


def test_write_files():
    target = done_messages[0]
    write_test_context_files(target.platform.value, target.uid, target.video_name)


def test_transcode():
    print()
    assert s3_conf is not None
    log.set_level(logging.DEBUG)
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    write_test_context_files(target.platform.value, target.uid, target.video_name)

    transcoder = StdlTranscoder(
        accessor=StdlS3SegmentAccessor(
            conf=s3_conf,
            network_io_delay_ms=1,
            network_buf_size=65536,
        ),
        notifier=MockNotifier(),
        out_dir_path=base_dir_path,
        tmp_path=tmp_dir_path,
        is_archive=is_archive,
        video_size_limit_gb=1024,
    )
    result = transcoder.transcode(
        StdlSegmentsInfo(
            platform_name=target.platform.value,
            channel_id=target.uid,
            video_name=target.video_name,
        )
    )
    print(result)


def test_transcode_by_archiver():
    assert s3_conf is not None
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    write_test_context_files(target.platform.value, target.uid, target.video_name)

    archiver = StdlArchiver(
        s3_conf=s3_conf,
        notifier=MockNotifier(),
        out_dir_path=base_dir_path,
        tmp_dir_path=tmp_dir_path,
        is_archive=is_archive,
    )
    archive_target = ArchiveTarget(
        platform=target.platform.value,
        uid=target.uid,
        video_name=target.video_name,
    )
    archiver.transcode_by_s3([archive_target])


def test_download():
    assert s3_conf is not None
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    # write_test_context_files(target.platform.value, target.uid, target.video_name)

    archiver = StdlArchiver(
        s3_conf=s3_conf,
        notifier=MockNotifier(),
        out_dir_path=base_dir_path,
        tmp_dir_path=tmp_dir_path,
        is_archive=is_archive,
    )
    archive_target = ArchiveTarget(
        platform=target.platform.value,
        uid=target.uid,
        video_name=target.video_name,
    )
    archiver.download([archive_target])
