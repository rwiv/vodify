from pyutils import log

from .celery_app import app
from .celery_worker_deps import WorkerDependencyManager
from ..common.status import TaskStatus
from ..service.stdl.schema import StdlDoneMsg, StdlDoneStatus


@app.task(name="vtask.stdl.done")
def stdl_done(dct: dict):
    deps = WorkerDependencyManager()
    msg = StdlDoneMsg(**dct)

    task_uname = f"{msg.platform}:{msg.uid}:{msg.video_name}"
    task_status = deps.task_status_repository.get(task_uname=task_uname)
    if task_status == TaskStatus.PENDING:
        log.debug(f"Task already pending", {"task_uname": task_uname})
        return
    elif task_status == TaskStatus.SUCCESS:
        log.debug(f"Task already completed", {"task_uname": task_uname})
        return

    transcoder = deps.create_stdl_transcoder(msg.fs_name)
    if msg.status == StdlDoneStatus.COMPLETE:
        return transcoder.transcode(msg.to_segments_info())
    elif msg.status == StdlDoneStatus.CANCELED:
        return transcoder.clear(msg.to_segments_info())
    else:
        raise ValueError(f"Unknown status: {msg.status}")
