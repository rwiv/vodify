from pyutils import log

from ...celery import recnode_transcode, DEFAULT_QUEUE_NAME, IO_NET_QUEUE_NAME, IO_LFS_QUEUE_NAME
from ...common.fs import LOCAL_FILE_NAME
from ...recnode import RecnodeMsg, RecnodeDoneStatus


class RecnodeTaskRegistrar:
    def register(self, msg: RecnodeMsg, queue_name: str):
        msg_dict = msg.model_dump(mode="json", by_alias=True)
        recnode_transcode.apply_async(args=[msg_dict], queue=queue_name)  # type: ignore
        log.info("recnode.transcode task started", msg_dict)

    def resolve_queue(self, msg: RecnodeMsg):
        if msg.status == RecnodeDoneStatus.CANCELED:
            return DEFAULT_QUEUE_NAME

        if msg.fs_name == LOCAL_FILE_NAME:
            return IO_LFS_QUEUE_NAME
        else:
            return IO_NET_QUEUE_NAME
