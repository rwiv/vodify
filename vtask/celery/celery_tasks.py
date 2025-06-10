import asyncio

from pyutils import log, error_dict

from .celery_app import app
from .celery_worker_deps import WorkerDependencyManager
from ..stdl import StdlDoneMsg, StdlDoneStatus


@app.task(name="vtask.stdl.done")
def stdl_done(dct: dict):
    deps = WorkerDependencyManager()
    msg = StdlDoneMsg(**dct)

    task_uname = f"{msg.platform.value}:{msg.uid}:{msg.video_name}"
    exists_result = deps.task_status_repository.check(task_uname=task_uname)
    if exists_result is not None:
        return exists_result

    deps.task_status_repository.set_pending(task_uname=task_uname)

    try:
        transcoder = deps.create_stdl_transcoder(msg.fs_name)
        if msg.status == StdlDoneStatus.COMPLETE:
            result = asyncio.run(transcoder.transcode(msg.to_segments_info()))
        elif msg.status == StdlDoneStatus.CANCELED:
            result = asyncio.run(transcoder.clear(msg.to_segments_info()))
        else:
            raise ValueError(f"Unknown status: {msg.status}")
        deps.task_status_repository.set_success(task_uname=task_uname)
        return result
    except Exception as ex:
        err = error_dict(ex)
        err["task_uname"] = task_uname
        log.error(f"Failed to process task", err)
        deps.task_status_repository.set_failure(task_uname=task_uname)
        raise ex
