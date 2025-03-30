import threading
import time
from enum import Enum

from pyutils import log, error_dict

from .job_spec import Job


class CronJobStatus(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    STOPPING = "stopping"
    DONE = "done"


class CronJob:
    def __init__(
        self,
        job: Job,
        interval_sec: float = 1,
        retry_limit: int = 3,
    ):
        self.__job = job
        self.__interval_sec = interval_sec
        self.__retry_limit = retry_limit

        self.__abort_flag = False
        self.__thread: threading.Thread | None = None

        self.status = CronJobStatus.WAITING

    def is_running(self) -> bool:
        return self.status == CronJobStatus.RUNNING

    def start(self):
        if self.__thread is not None:
            raise ValueError("CronJob already started")
        self.__thread = threading.Thread(target=self.__run)
        self.__thread.start()

    def __run(self):
        log.info("Start CronJob", {"job_name": self.__job.name})
        self.status = CronJobStatus.RUNNING
        retry_cnt = 0
        while not self.__abort_flag:
            try:
                self.__job.run()
                retry_cnt = 0
            except Exception as e:
                err_info = error_dict(e)
                err_info["job_name"] = self.__job.name
                err_info["retry_cnt"] = retry_cnt

                if retry_cnt >= self.__retry_limit:
                    log.error("CronJob failed", err_info)
                    break

                log.error("Retry CronJob", err_info)
                retry_cnt += 1

            time.sleep(self.__interval_sec)

        log.info("Stop CronJob", {"job_name": self.__job.name})

    def stop(self):
        if self.__thread is None:
            raise ValueError("Thread not started")

        self.__abort_flag = True
        self.status = CronJobStatus.STOPPING

        self.__thread.join()

        self.__thread = None
        self.__abort_flag = False
        self.status = CronJobStatus.DONE
