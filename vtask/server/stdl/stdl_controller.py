from fastapi import APIRouter
from pyutils import log

from ...celery import stdl_done_local, stdl_done_remote, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from ...common.fs import LOCAL_FILE_NAME
from ...service.stdl import StdlDoneMsg


class StdlController:
    def __init__(self):
        self.router = APIRouter(prefix="/api/stdl")
        self.router.add_api_route("/done", self.done_task, methods=["POST"])

    def done_task(self, msg: StdlDoneMsg):
        dct = msg.to_json_dict()
        if msg.fs_name == LOCAL_FILE_NAME:
            stdl_done_local.apply_async(args=[dct], queue=LOCAL_QUEUE_NAME)  # type: ignore
        else:
            stdl_done_remote.apply_async(args=[dct], queue=REMOTE_QUEUE_NAME)  # type: ignore
        log.info("stdl.done task sent", dct)
        return "ok"
