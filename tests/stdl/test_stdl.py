import json
import os

import yaml
from pyutils import load_dotenv, path_join, find_project_root

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))

from vtask.common.amqp import AmqpHelperBlocking
from vtask.common.env import get_server_env
from vtask.common.fs import FsType, read_fs_config
from vtask.celery import stdl_done_local, LOCAL_QUEUE_NAME
from vtask.service.stdl.muxer import StdlMuxer, StdlS3Helper
from vtask.service.stdl.schema import StdlDoneMsg, StdlDoneStatus, StdlPlatformType, STDL_DONE_QUEUE
from vtask.utils import LocalObjectWriter

with open(path_join(find_project_root(), "dev", "test_conf.yaml"), "r") as file:
    test_conf = yaml.load(file.read(), Loader=yaml.FullLoader)

fs_configs = read_fs_config(path_join(find_project_root(), "dev", "fs_conf_test.yaml"))

targets = [
    {
        "uid": "test_uid",
        "video_name": "test_video",
    }
]

fs_name = "local"
# fs_name = "minio"
is_archive = True
# is_archive = False

fs_conf = None
for conf in fs_configs:
    if conf.name == fs_name:
        fs_conf = conf
        break

src_writer = LocalObjectWriter()
# src_writer = S3ObjectWriter(fs_conf.s3)  # type: ignore

out_writer = LocalObjectWriter()
# out_writer = S3ObjectWriter(fs_conf.s3)  # type: ignore

base_dir_path = test_conf["local_base_dir_path"]
tmp_dir_path = test_conf["tmp_dir_path"]


def set_test_environment(uid: str, video_name: str):
    src_dir_path = base_dir_path if src_writer.fs_type == FsType.LOCAL else ""
    vid_dir_path = path_join(src_dir_path, "incomplete", uid, video_name)

    local_chunks_path = test_conf["chunks_path"]
    for chunk_name in os.listdir(local_chunks_path):
        with open(path_join(local_chunks_path, chunk_name), mode="rb") as f:
            src_writer.write(path_join(vid_dir_path, chunk_name), f.read())


def test_mux():
    print()
    target = targets[0]
    uid = target["uid"]
    video_name = target["video_name"]

    set_test_environment(uid, video_name)

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


def test_task():
    for target in targets:
        set_test_environment(target["uid"], target["video_name"])
        dct = create_msg(target).to_json_dict()
        result = stdl_done_local.apply_async(args=[dct], queue=LOCAL_QUEUE_NAME)  # type: ignore
        # result = stdl_done_remote.apply_async(args=[dct], queue=REMOTE_QUEUE_NAME)  # type: ignore
        print(result)


def test_amqp():
    print()
    env = get_server_env()
    amqp = AmqpHelperBlocking(env.amqp)
    conn, chan = amqp.connect()
    amqp.ensure_queue(chan, STDL_DONE_QUEUE)
    for target in targets:
        body = json.dumps(create_msg(target).to_json_dict()).encode("utf-8")
        amqp.publish(chan, STDL_DONE_QUEUE, body)
    amqp.close(conn)


def create_msg(target):
    return StdlDoneMsg(
        status=StdlDoneStatus.COMPLETE,
        # status=StdlDoneStatus.CANCELED,
        platform=StdlPlatformType.CHZZK,
        uid=target["uid"],
        videoName=target["video_name"],
        fsName="local",
    )
