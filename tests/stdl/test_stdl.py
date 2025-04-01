import os

from pyutils import path_join

from tests.testutils.test_utils_conf import read_test_conf
from tests.testutils.test_utils_fs import read_test_fs_configs, find_test_fs_config
from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.utils import S3ObjectWriter

load_test_dotenv(".env-server-dev")

from vtask.common.amqp import AmqpHelperBlocking
from vtask.common.env import get_server_env
from vtask.service.stdl.muxer import StdlMuxer, StdlS3Helper
from vtask.service.stdl.schema import STDL_DONE_QUEUE, StdlDoneMsg, StdlDoneStatus

test_conf = read_test_conf()


# fs_name = "local"
fs_name = "minio"

is_archive = True
# is_archive = False

done_messages = [
    StdlDoneMsg(uid="test_uid1", videoName="test_video", fsName=fs_name, status=StdlDoneStatus.COMPLETE),
    StdlDoneMsg(uid="test_uid2", videoName="test_video", fsName=fs_name, status=StdlDoneStatus.COMPLETE),
    StdlDoneMsg(uid="test_uid3", videoName="test_video", fsName=fs_name, status=StdlDoneStatus.CANCELED),
    StdlDoneMsg(uid="test_uid4", videoName="test_video", fsName=fs_name, status=StdlDoneStatus.COMPLETE),
    StdlDoneMsg(uid="test_uid5", videoName="test_video", fsName=fs_name, status=StdlDoneStatus.COMPLETE),
]

fs_configs = read_test_fs_configs(is_prod=False)
fs_conf = find_test_fs_config(fs_configs, fs_name)
# src_writer = LocalObjectWriter()
src_writer = S3ObjectWriter(fs_conf.s3)  # type: ignore

local_chunks_path = test_conf.chunks_path
base_dir_path = test_conf.local_base_dir_path
tmp_dir_path = test_conf.tmp_dir_path


def write_test_context_files(uid: str, video_name: str):
    incomplete_path = src_writer.normalize_base_path(path_join(base_dir_path, "incomplete"))
    vid_dir_path = path_join(incomplete_path, uid, video_name)

    for chunk_name in os.listdir(local_chunks_path):
        with open(path_join(local_chunks_path, chunk_name), mode="rb") as f:
            src_writer.write(path_join(vid_dir_path, chunk_name), f.read())


def test_write_context_files():
    for target in done_messages:
        write_test_context_files(target.uid, target.video_name)


def test_publish_amqp():
    print()
    env = get_server_env()
    amqp = AmqpHelperBlocking(env.amqp)
    for target in done_messages:
        amqp.instance_publish(STDL_DONE_QUEUE, target)


def test_mux():
    print()
    target = done_messages[0]
    uid = target.uid
    video_name = target.video_name

    write_test_context_files(uid, video_name)

    # helper = StdlLocalChunksMover()
    helper = StdlS3Helper(
        local_incomplete_dir_path=path_join(base_dir_path, "incomplete"),
        conf=fs_conf.s3,  # type: ignore
        network_io_delay_ms=1,
        network_buf_size=65536,
    )
    muxer = StdlMuxer(helper=helper, base_path=base_dir_path, tmp_path=tmp_dir_path, is_archive=is_archive)
    result = muxer.mux(uid, video_name)
    print(result)
