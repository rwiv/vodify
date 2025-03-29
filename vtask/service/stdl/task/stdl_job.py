from .stdl_message_manager import StdlMessageManager


class StdlJob:
    def __init__(self, msg_manager: StdlMessageManager):
        self.msg_manager = msg_manager

    def run(self):
        self.msg_manager.push_all()
        while True:
            stdl_msg = self.msg_manager.pop()
            print(stdl_msg)
