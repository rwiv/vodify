import logging

import aiofiles
import pytest
from aiofiles import os as aios
from pyutils import path_join, log, find_project_root

from tests.testutils.test_utils_fs import read_test_fs_configs, find_test_fs_config
from tests.testutils.test_utils_misc import load_test_dotenv
from vodify.common.fs import S3ObjectWriter
from vodify.external.notifier import MockNotifier
from vodify.external.s3 import S3AsyncClient
from vodify.recnode import (
    RecnodeMsg,
    RecnodeDoneStatus,
    RecnodePlatformType,
    RecnodeSegmentsInfo,
    RecnodeTranscoder,
    S3SegmentAccessor,
    RecnodeArchiver,
    ArchiveTarget,
)
from vodify.utils import rmtree

load_test_dotenv(".env-server-dev")


# fs_name = "local"
fs = "minio"

pf = RecnodePlatformType.CHZZK
vid = "test_video"
o = RecnodeDoneStatus.COMPLETE
x = RecnodeDoneStatus.CANCELED

done_messages = [
    RecnodeMsg(platform=pf, uid="test_uid1", videoName=vid, fsName=fs, status=o),
    RecnodeMsg(platform=pf, uid="test_uid2", videoName=vid, fsName=fs, status=o),
    RecnodeMsg(platform=pf, uid="test_uid3", videoName=vid, fsName=fs, status=x),
    RecnodeMsg(platform=pf, uid="test_uid4", videoName=vid, fsName=fs, status=x),
    RecnodeMsg(platform=pf, uid="test_uid5", videoName=vid, fsName=fs, status=o),
]

fs_configs = read_test_fs_configs(is_prod=False)
fs_conf = find_test_fs_config(fs_configs, fs)
s3_conf = fs_conf.s3
assert s3_conf is not None
# src_writer = LocalObjectWriter()
s3_client = S3AsyncClient(
    conf=s3_conf,
    network_mbit=64,
    network_buf_size=8192,
    retry_limit=1,
    min_read_timeout_sec=10,
    read_timeout_threshold=2.0,
)
src_writer = S3ObjectWriter(s3_client)

dev_test_dir_path = path_join(find_project_root(), "dev", "test")

local_chunks_path = path_join(dev_test_dir_path, "assets", "recnode", "dup_o_miss_o_loss_o")
base_dir_path = path_join(dev_test_dir_path, "out")
tmp_dir_path = path_join(dev_test_dir_path, "tmp")


async def write_test_context_files(platform: str, uid: str, video_name: str):
    incomplete_path = src_writer.normalize_base_path(path_join(base_dir_path, "incomplete"))
    vid_dir_path = path_join(incomplete_path, platform, uid, video_name)

    for chunk_name in await aios.listdir(local_chunks_path):
        async with aiofiles.open(path_join(local_chunks_path, chunk_name), mode="rb") as f:
            await src_writer.write(path_join(vid_dir_path, chunk_name), await f.read())


@pytest.mark.asyncio
async def test_write_files():
    target = done_messages[0]
    await write_test_context_files(target.platform.value, target.uid, target.video_name)


@pytest.mark.asyncio
async def test_transcode():
    print()
    assert s3_conf is not None
    log.set_level(logging.DEBUG)
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    await write_test_context_files(target.platform.value, target.uid, target.video_name)

    transcoder = RecnodeTranscoder(
        accessor=S3SegmentAccessor(s3_client=s3_client, delete_batch_size=100),
        notifier=MockNotifier(),
        out_dir_path=base_dir_path,
        tmp_path=tmp_dir_path,
        is_archive=is_archive,
        video_size_limit_gb=1024,
    )
    result = await transcoder.transcode(
        RecnodeSegmentsInfo(
            platform_name=target.platform.value,
            channel_id=target.uid,
            video_name=target.video_name,
        )
    )
    await rmtree(tmp_dir_path)
    print(result)


@pytest.mark.asyncio
async def test_transcode_by_archiver():
    assert s3_conf is not None
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    await write_test_context_files(target.platform.value, target.uid, target.video_name)

    archiver = RecnodeArchiver(
        s3_client=s3_client,
        out_dir_path=base_dir_path,
        tmp_dir_path=tmp_dir_path,
        is_archive=is_archive,
        video_size_limit_gb=1024,
        notifier=MockNotifier(),
    )
    archive_target = ArchiveTarget(
        platform=target.platform.value,
        uid=target.uid,
        video_name=target.video_name,
    )
    await archiver.transcode_by_s3([archive_target])
    await rmtree(tmp_dir_path)


@pytest.mark.asyncio
async def test_download():
    assert s3_conf is not None
    target = done_messages[0]

    # is_archive = True
    is_archive = False

    await write_test_context_files(target.platform.value, target.uid, target.video_name)

    archiver = RecnodeArchiver(
        s3_client=s3_client,
        out_dir_path=base_dir_path,
        tmp_dir_path=tmp_dir_path,
        is_archive=is_archive,
        video_size_limit_gb=1024,
        notifier=MockNotifier(),
    )
    archive_target = ArchiveTarget(
        platform=target.platform.value,
        uid=target.uid,
        video_name=target.video_name,
    )
    await archiver.download([archive_target])
    await rmtree(tmp_dir_path)
