import threading

from redis import Redis

from ...common.env import RedisConfig


class StdlPeriodicTask:
    def __init__(self, conf: RedisConfig):
        self.abort_flag = False
        self.thread: threading.Thread | None = None
        self.__redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)

    def start(self):
        if self.thread is not None:
            raise ValueError("Thread already started")
        self.thread = threading.Thread(target=self.__run)

    def __run(self):
        while not self.abort_flag:
            self.__redis.lpush(REDIS_STDL_DONE_KEY, "test")

    def stop(self):
        if self.thread is None:
            raise ValueError("Thread not started")
        self.thread.join()
        self.thread = None
