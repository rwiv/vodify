import os
import time

from pyutils import path_join, filename

from tests.testutils.test_utils_conf import read_test_conf
from tests.testutils.test_utils_fs import read_test_fs_configs, find_test_fs_config
from tests.testutils.test_utils_misc import load_test_dotenv
from vtask.common.amqp import AmqpHelperBlocking
from vtask.common.env import get_server_env
from vtask.service.stdl.schema import STDL_DONE_QUEUE
from vtask.utils import S3Client


load_test_dotenv(".env-server-dev")
# load_test_dotenv(".env-server-prod")

test_conf = read_test_conf()
fs_configs = read_test_fs_configs(is_prod=False)


def test_backup():
    start_time = time.time()
    out_dir_path = test_conf.stdl.out_dir_path

    for target in test_conf.stdl.done_messages:
        fs_conf = find_test_fs_config(fs_configs, target.fs_name)
        s3_conf = S3Client(fs_conf.s3)  # type: ignore

        res = s3_conf.list(path_join("incomplete", target.uid, target.video_name))
        if res.contents is None:
            raise ValueError("Files not found")
        os.makedirs(out_dir_path, exist_ok=True)
        for content in res.contents:
            s3_conf.write_file(
                key=content.key,
                file_path=path_join(out_dir_path, filename(content.key)),
                network_io_delay_ms=0,
                network_buf_size=65536,
            )

    print(f"Elapsed time: {time.time() - start_time:.6f} sec")


def test_publish_done_message():
    env = get_server_env()
    amqp = AmqpHelperBlocking(env.amqp)

    for target in test_conf.stdl.done_messages:
        amqp.instance_publish(STDL_DONE_QUEUE, target)
