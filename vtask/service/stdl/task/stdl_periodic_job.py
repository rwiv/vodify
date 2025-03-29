import threading


class StdlPeriodicJob:
    def __init__(self):
        self.abort_flag = False
        self.thread: threading.Thread | None = None

    def start(self):
        if self.thread is not None:
            raise ValueError("Thread already started")
        self.thread = threading.Thread(target=self.__run)

    def __run(self):
        while not self.abort_flag:
            # TODO: implement
            pass

    def stop(self):
        if self.thread is None:
            raise ValueError("Thread not started")
        self.thread.join()
        self.thread = None
