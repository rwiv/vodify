import os
import time

import yaml
from pyutils import path_join, find_project_root, load_dotenv, filename

load_dotenv(path_join(find_project_root(), "dev", ".env-server-prod"))

from vtask.common.amqp import AmqpBlocking
from vtask.common.env import get_server_env
from vtask.common.fs import read_fs_config
from vtask.server.stdl import STDL_DONE_QUEUE
from vtask.service.stdl import StdlDoneMsg, StdlDoneStatus, StdlPlatformType
from vtask.utils import S3Client


with open(path_join(find_project_root(), "dev", "test_conf.yaml"), "r") as file:
    test_conf = yaml.load(file.read(), Loader=yaml.FullLoader)["stdl"]


out_dir_path = test_conf["out_dir_path"]

uid = test_conf["uid"]
vid_name = test_conf["vid_name"]
fs_name = test_conf["fs_name"]

fs_configs = read_fs_config(path_join(find_project_root(), "dev", "fs_conf.yaml"))
fs_conf = None
for cur_conf in fs_configs:
    if cur_conf.name == fs_name:
        fs_conf = cur_conf
if fs_conf is None or fs_conf.s3 is None:
    raise ValueError("S3 config not found")
s3 = S3Client(fs_conf.s3, retry_limit=3)


def test_backup():
    start_time = time.time()
    res = s3.list(path_join("incomplete", uid, vid_name))
    if res.contents is None:
        raise ValueError("Files not found")
    os.makedirs(out_dir_path, exist_ok=True)
    for content in res.contents:
        s3.write_file(
            key=content.key,
            file_path=path_join(out_dir_path, filename(content.key)),
            network_io_delay_ms=0,
            network_buf_size=65536,
        )
    print(f"Elapsed time: {time.time() - start_time:.6f} sec")


def test_publish_done_message():
    print()
    env = get_server_env()
    amqp = AmqpBlocking(env.amqp)

    conn, chan = amqp.connect()
    amqp.assert_queue(chan, STDL_DONE_QUEUE, auto_delete=False)
    msg = StdlDoneMsg(
        status=StdlDoneStatus.COMPLETE,
        platform=StdlPlatformType.CHZZK,
        uid=uid,
        videoName=vid_name,
        fsName=fs_name,
    ).model_dump_json(by_alias=True)
    amqp.publish(chan, STDL_DONE_QUEUE, msg.encode("utf-8"))
    amqp.close(conn)
