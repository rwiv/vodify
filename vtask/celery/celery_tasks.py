from .celery_app import app
from .celery_constants import LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from .celery_worker_deps import deps
from ..service.stdl import StdlDoneMsg, StdlDoneStatus


@app.task(name="vtask.stdl.done.local", queue=LOCAL_QUEUE_NAME)
def stdl_done_local(dct: dict):
    return stdl_done(dct)


@app.task(name="vtask.stdl.done.remote", queue=REMOTE_QUEUE_NAME)
def stdl_done_remote(dct: dict):
    return stdl_done(dct)


def stdl_done(dct: dict):
    msg = StdlDoneMsg(**dct)
    muxer = deps.create_stdl_muxer(msg.fs_name)
    if msg.status == StdlDoneStatus.COMPLETE:
        return muxer.mux(msg.uid, msg.video_name)
    elif msg.status == StdlDoneStatus.CANCELED:
        return muxer.clear(msg.uid, msg.video_name)
    else:
        raise ValueError(f"Unknown status: {msg.status}")
