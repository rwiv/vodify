from .celery_app import app
from .celery_worker_deps import deps
from ..service.stdl.schema import StdlDoneMsg, StdlDoneStatus


@app.task(name="vtask.stdl.done")
def stdl_done(dct: dict):
    msg = StdlDoneMsg(**dct)
    muxer = deps.create_stdl_muxer(msg.fs_name)
    if msg.status == StdlDoneStatus.COMPLETE:
        return muxer.mux(msg.uid, msg.video_name)
    elif msg.status == StdlDoneStatus.CANCELED:
        return muxer.clear(msg.uid, msg.video_name)
    else:
        raise ValueError(f"Unknown status: {msg.status}")
