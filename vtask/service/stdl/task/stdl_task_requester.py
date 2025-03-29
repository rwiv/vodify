from pyutils import log

from ..schema import StdlDoneMsg
from ....common.fs import LOCAL_FILE_NAME
from ....celery import stdl_done_local, stdl_done_remote, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME


class StdlTaskRequester:
    def __init__(self):
        pass

    def request(self, msg: StdlDoneMsg):
        msg_dict = msg.model_dump(mode="json", by_alias=True)
        if msg.fs_name == LOCAL_FILE_NAME:
            stdl_done_local.apply_async(args=[msg_dict], queue=LOCAL_QUEUE_NAME)  # type: ignore
        else:
            stdl_done_remote.apply_async(args=[msg_dict], queue=REMOTE_QUEUE_NAME)  # type: ignore
        log.info("stdl.done task sent", msg_dict)
