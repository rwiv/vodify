from pyutils import log

from ..schema import StdlDoneMsg
from ....celery import stdl_done_local, stdl_done_remote, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from ....common.fs import LOCAL_FILE_NAME


class StdlTaskRequester:
    def request_done(self, msg: StdlDoneMsg, queue_name: str):
        msg_dict = msg.model_dump(mode="json", by_alias=True)
        if queue_name == LOCAL_QUEUE_NAME:
            stdl_done_local.apply_async(args=[msg_dict], queue=queue_name)  # type: ignore
        elif queue_name == REMOTE_QUEUE_NAME:
            stdl_done_remote.apply_async(args=[msg_dict], queue=queue_name)  # type: ignore
        else:
            raise ValueError(f"Unknown queue name: {queue_name}")
        log.info("stdl.done task start", msg_dict)

    def resolve_queue(self, msg: StdlDoneMsg):
        # if msg.status == StdlDoneStatus.CANCELED:
        #     return LOCAL_QUEUE_NAME

        if msg.fs_name == LOCAL_FILE_NAME:
            return LOCAL_QUEUE_NAME
        else:
            return REMOTE_QUEUE_NAME


class MockStdlTaskRequester(StdlTaskRequester):
    def request_done(self, msg: StdlDoneMsg, queue_name: str):
        msg_dict = msg.model_dump(mode="json", by_alias=True)
        log.info("stdl.done task start", msg_dict)
