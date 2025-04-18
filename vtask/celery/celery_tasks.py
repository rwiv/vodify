from .celery_app import app
from .celery_worker_deps import deps
from ..service.stdl.schema import StdlDoneMsg, StdlDoneStatus


@app.task(name="vtask.stdl.done")
def stdl_done(dct: dict):
    msg = StdlDoneMsg(**dct)
    transcoder = deps.create_stdl_transcoder(msg.fs_name)
    if msg.status == StdlDoneStatus.COMPLETE:
        return transcoder.transcode(msg.to_segments_info())
    elif msg.status == StdlDoneStatus.CANCELED:
        return transcoder.clear(msg.to_segments_info())
    else:
        raise ValueError(f"Unknown status: {msg.status}")
