import os
from pathlib import Path

from pyutils import path_join, find_project_root, load_dotenv, dirpath

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))
# load_dotenv(path_join(find_project_root(), "dev", ".env-server-prod"))

from vtask.common.amqp import AmqpHelperBlocking
from vtask.common.env import get_server_env, get_celery_env
from vtask.service.stdl.schema import StdlDoneStatus, StdlPlatformType, StdlDoneMsg, STDL_DONE_QUEUE
from vtask.service.stdl.task import StdlMessageManager, StdlMessageHelper

server_env = get_server_env()
celery_env = get_celery_env()
amqp = AmqpHelperBlocking(server_env.amqp)

mgr = StdlMessageManager(amqp, celery_env.redis)
helper = StdlMessageHelper(amqp, mgr)


def test_consume_all():
    print()
    for message in mgr.consume_all():
        print(message)
    mgr.clear_list()


def test_stdl_archive_test():
    # for i in range(3):
    #     publish_done_message(i)

    out_file_path = path_join(find_project_root(), "dev", "out", "stdl_done.json")
    if not Path(dirpath(out_file_path)).exists():
        os.makedirs(dirpath(out_file_path), exist_ok=True)

    helper.archive(out_file_path)


def test_stdl_publish_by_archive():
    src_file_path = path_join(find_project_root(), "dev", "out", "stdl_done.json")
    if not Path(dirpath(src_file_path)).exists():
        os.makedirs(dirpath(src_file_path), exist_ok=True)

    helper.publish_by_archive(src_file_path, 5)


def publish_done_message(n: int):
    conn, chan = amqp.connect()
    amqp.ensure_queue(chan, STDL_DONE_QUEUE, auto_delete=False)
    msg = StdlDoneMsg(
        status=StdlDoneStatus.COMPLETE,
        platform=StdlPlatformType.CHZZK,
        uid=str(n),
        videoName=str(n),
        fsName="local",
    ).model_dump_json(by_alias=True)
    amqp.publish(chan, STDL_DONE_QUEUE, msg.encode("utf-8"))
    amqp.close(conn)
