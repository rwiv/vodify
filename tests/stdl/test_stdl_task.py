from pyutils import path_join, find_project_root, load_dotenv

from vtask.service.stdl import StdlDoneStatus, StdlPlatformType, StdlDoneMsg

load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))
# load_dotenv(path_join(find_project_root(), "dev", ".env-server-prod"))

from vtask.common.amqp import AmqpHelperBlocking
from vtask.common.env import get_server_env, get_celery_env
from vtask.server.stdl import StdlDoneMessageManager, STDL_DONE_QUEUE


server_env = get_server_env()
celery_env = get_celery_env()
amqp = AmqpHelperBlocking(server_env.amqp)


def test_stdl_test():
    print()
    for i in range(3):
        publish_done_message(i)
    task = StdlDoneMessageManager(amqp, celery_env.redis)
    task.run_task()


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
